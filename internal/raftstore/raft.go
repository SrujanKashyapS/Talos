package raftstore

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"os"
	"path/filepath"
	"time"
)

type Peer struct {
	ID      string
	Address string
}
type Options struct {
	ID        string
	Bind      string
	StateDir  string
	Bootstrap bool
	Peers     []Peer
}
type Node struct {
	Raft      *raft.Raft
	FSM       *FSM
	Transport *raft.NetworkTransport
	Store     *raftboltdb.BoltStore
	Snapshots *raft.FileSnapshotStore
}

func Open(o Options) (*Node, error) {
	if o.ID == "" || o.Bind == "" || o.StateDir == "" {
		return nil, fmt.Errorf("raft id, bind address, and state directory are required")
	}
	if e := os.MkdirAll(o.StateDir, 0o755); e != nil {
		return nil, fmt.Errorf("create raft dir: %w", e)
	}
	store, e := raftboltdb.NewBoltStore(filepath.Join(o.StateDir, "raft.db"))
	if e != nil {
		return nil, fmt.Errorf("open raft bolt store: %w", e)
	}
	snaps, e := raft.NewFileSnapshotStore(o.StateDir, 2, os.Stderr)
	if e != nil {
		_ = store.Close()
		return nil, fmt.Errorf("open snapshots: %w", e)
	}
	tr, e := raft.NewTCPTransport(o.Bind, nil, 5, 10*time.Second, os.Stderr)
	if e != nil {
		_ = store.Close()
		return nil, fmt.Errorf("open raft transport: %w", e)
	}
	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(o.ID)
	conf.HeartbeatTimeout = 500 * time.Millisecond
	conf.ElectionTimeout = 1500 * time.Millisecond
	conf.CommitTimeout = 50 * time.Millisecond
	conf.SnapshotInterval = 20 * time.Second
	conf.SnapshotThreshold = 64
	if o.Bootstrap {
		has, e := raft.HasExistingState(store, store, snaps)
		if e != nil {
			_ = tr.Close()
			_ = store.Close()
			return nil, fmt.Errorf("check raft state: %w", e)
		}
		if !has {
			cfg := raft.Configuration{}
			for _, p := range o.Peers {
				cfg.Servers = append(cfg.Servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.Address)})
			}
			if len(cfg.Servers) == 0 {
				cfg.Servers = []raft.Server{{ID: raft.ServerID(o.ID), Address: raft.ServerAddress(o.Bind)}}
			}
			if e := raft.BootstrapCluster(conf, store, store, snaps, tr, cfg); e != nil {
				_ = tr.Close()
				_ = store.Close()
				return nil, fmt.Errorf("bootstrap raft: %w", e)
			}
		}
	}
	fsm := NewFSM()
	r, e := raft.NewRaft(conf, fsm, store, store, snaps, tr)
	if e != nil {
		_ = tr.Close()
		_ = store.Close()
		return nil, fmt.Errorf("create raft: %w", e)
	}
	return &Node{Raft: r, FSM: fsm, Transport: tr, Store: store, Snapshots: snaps}, nil
}
func (n *Node) Apply(c Command, timeout time.Duration) (interface{}, error) {
	if n.Raft.State() != raft.Leader {
		return nil, fmt.Errorf("not leader: %s", n.Raft.Leader())
	}
	b, e := json.Marshal(c)
	if e != nil {
		return nil, fmt.Errorf("marshal raft command: %w", e)
	}
	f := n.Raft.Apply(b, timeout)
	if e := f.Error(); e != nil {
		return nil, fmt.Errorf("raft apply: %w", e)
	}
	return f.Response(), nil
}
func (n *Node) Join(id, addr string) error {
	if n.Raft.State() != raft.Leader {
		return fmt.Errorf("not leader: %s", n.Raft.Leader())
	}
	f := n.Raft.GetConfiguration()
	if e := f.Error(); e != nil {
		return fmt.Errorf("get configuration: %w", e)
	}
	c := f.Configuration()
	for _, s := range c.Servers {
		if string(s.ID) == id {
			return nil
		}
	}
	if e := n.Raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 10*time.Second).Error(); e != nil {
		return fmt.Errorf("add voter: %w", e)
	}
	return nil
}
func (n *Node) Leader() string { return string(n.Raft.Leader()) }
func (n *Node) IsLeader() bool { return n.Raft.State() == raft.Leader }
func (n *Node) Shutdown() error {
	var e1, e2 error
	if n.Raft != nil {
		e1 = n.Raft.Shutdown().Error()
	}
	if n.Transport != nil {
		e2 = n.Transport.Close()
	}
	if n.Store != nil {
		_ = n.Store.Close()
	}
	if e1 != nil {
		return e1
	}
	if e2 != nil {
		return e2
	}
	return nil
}
func (n *Node) ServerAddress() string { return string(n.Transport.LocalAddr()) }
