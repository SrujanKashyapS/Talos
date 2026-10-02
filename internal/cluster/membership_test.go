package cluster

import (
	"context"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"testing"
	"time"
)

func TestMembershipDeadAfterMisses(t *testing.T) {
	m := NewMembership(time.Second, 3)
	if _, e := m.Register(context.Background(), rpc.NodeInfo{ID: "n1", Address: "127.0.0.1:1"}); e != nil {
		t.Fatal(e)
	}
	n := m.Nodes()[0]
	m.Tick(time.Unix(0, n.LastHeartbeat).Add(2 * time.Second))
	if !m.Nodes()[0].Alive {
		t.Fatal("dead too early")
	}
	m.Tick(time.Unix(0, n.LastHeartbeat).Add(3 * time.Second))
	if m.Nodes()[0].Alive {
		t.Fatal("node stayed alive")
	}
}
