package raftstore

import (
	"encoding/json"
	"fmt"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"github.com/hashicorp/raft"
	"io"
	"strings"
	"sync"
)

const (
	OpNode             = "node"
	OpWorkload         = "workload"
	OpAssignment       = "assignment"
	OpDeleteAssignment = "delete_assignment"
	OpDeleteWorkload   = "delete_workload"
	OpCache            = "cache"
)

type Command struct {
	Op    string
	Key   string
	Value json.RawMessage
}
type State struct {
	Nodes       map[string]rpc.NodeInfo
	Workloads   map[string]json.RawMessage
	Assignments map[string]string
	Cache       map[string]string
}
type FSM struct {
	mu    sync.RWMutex
	state State
}

func NewFSM() *FSM {
	return &FSM{state: State{Nodes: map[string]rpc.NodeInfo{}, Workloads: map[string]json.RawMessage{}, Assignments: map[string]string{}, Cache: map[string]string{}}}
}
func (f *FSM) Apply(l *raft.Log) interface{} {
	var c Command
	if e := json.Unmarshal(l.Data, &c); e != nil {
		return fmt.Errorf("decode raft command: %w", e)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch c.Op {
	case OpNode:
		var n rpc.NodeInfo
		if e := json.Unmarshal(c.Value, &n); e != nil {
			return e
		}
		f.state.Nodes[c.Key] = n
	case OpWorkload:
		f.state.Workloads[c.Key] = append(json.RawMessage(nil), c.Value...)
	case OpAssignment:
		var v string
		if e := json.Unmarshal(c.Value, &v); e != nil {
			return e
		}
		f.state.Assignments[c.Key] = v
	case OpDeleteAssignment:
		delete(f.state.Assignments, c.Key)
	case OpDeleteWorkload:
		delete(f.state.Workloads, c.Key)
		for k := range f.state.Assignments {
			if strings.HasPrefix(k, c.Key+"/") {
				delete(f.state.Assignments, k)
			}
		}
	case OpCache:
		var v string
		if e := json.Unmarshal(c.Value, &v); e != nil {
			return e
		}
		f.state.Cache[c.Key] = v
	default:
		return fmt.Errorf("unknown raft op %q", c.Op)
	}
	return nil
}
func (f *FSM) State() State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s := State{Nodes: map[string]rpc.NodeInfo{}, Workloads: map[string]json.RawMessage{}, Assignments: map[string]string{}, Cache: map[string]string{}}
	for k, n := range f.state.Nodes {
		n.HeldLayers = append([]string(nil), n.HeldLayers...)
		s.Nodes[k] = n
	}
	for k, v := range f.state.Workloads {
		s.Workloads[k] = append(json.RawMessage(nil), v...)
	}
	for k, v := range f.state.Assignments {
		s.Assignments[k] = v
	}
	for k, v := range f.state.Cache {
		s.Cache[k] = v
	}
	return s
}
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) { return &snapshot{s: f.State()}, nil }
func (f *FSM) Restore(r io.ReadCloser) error {
	defer r.Close()
	var s State
	if e := json.NewDecoder(r).Decode(&s); e != nil {
		return fmt.Errorf("decode snapshot: %w", e)
	}
	if s.Nodes == nil {
		s.Nodes = map[string]rpc.NodeInfo{}
	}
	if s.Workloads == nil {
		s.Workloads = map[string]json.RawMessage{}
	}
	if s.Assignments == nil {
		s.Assignments = map[string]string{}
	}
	if s.Cache == nil {
		s.Cache = map[string]string{}
	}
	f.mu.Lock()
	f.state = s
	f.mu.Unlock()
	return nil
}

type snapshot struct{ s State }

func (s *snapshot) Persist(w raft.SnapshotSink) error {
	b, e := json.Marshal(s.s)
	if e != nil {
		_ = w.Cancel()
		return e
	}
	if _, e = w.Write(b); e != nil {
		_ = w.Cancel()
		return e
	}
	return w.Close()
}
func (s *snapshot) Release() {}
