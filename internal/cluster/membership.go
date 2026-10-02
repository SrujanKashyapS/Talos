package cluster

import (
	"context"
	"fmt"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"sort"
	"sync"
	"time"
)

type Membership struct {
	mu        sync.RWMutex
	nodes     map[string]rpc.NodeInfo
	heartbeat time.Duration
	deadAfter int
}

func NewMembership(interval time.Duration, n int) *Membership {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if n <= 0 {
		n = 3
	}
	return &Membership{nodes: map[string]rpc.NodeInfo{}, heartbeat: interval, deadAfter: n}
}
func (m *Membership) upsert(n rpc.NodeInfo) {
	n.LastHeartbeat = time.Now().UTC().UnixNano()
	n.Alive = true
	n.Missed = 0
	m.nodes[n.ID] = n
}
func (m *Membership) Register(ctx context.Context, n rpc.NodeInfo) (rpc.LeaderReply, error) {
	select {
	case <-ctx.Done():
		return rpc.LeaderReply{}, ctx.Err()
	default:
	}
	if n.ID == "" || n.Address == "" {
		return rpc.LeaderReply{}, fmt.Errorf("node id and address are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.upsert(n)
	return rpc.LeaderReply{}, nil
}
func (m *Membership) Heartbeat(ctx context.Context, n rpc.NodeInfo) (rpc.LeaderReply, error) {
	select {
	case <-ctx.Done():
		return rpc.LeaderReply{}, ctx.Err()
	default:
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.nodes[n.ID]; !ok {
		return rpc.LeaderReply{}, fmt.Errorf("node %s is not registered", n.ID)
	}
	m.upsert(n)
	return rpc.LeaderReply{}, nil
}
func (m *Membership) Tick(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cut := m.heartbeat * time.Duration(m.deadAfter)
	for id, n := range m.nodes {
		if n.Alive && now.Sub(time.Unix(0, n.LastHeartbeat)) >= cut {
			n.Alive = false
			n.Missed = int32(now.Sub(time.Unix(0, n.LastHeartbeat)) / m.heartbeat)
			m.nodes[id] = n
		}
	}
}
func (m *Membership) Nodes() []rpc.NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	o := make([]rpc.NodeInfo, 0, len(m.nodes))
	for _, n := range m.nodes {
		o = append(o, n)
	}
	sort.Slice(o, func(i, j int) bool { return o[i].ID < o[j].ID })
	return o
}

type Server struct{ M *Membership }

func (s *Server) RegisterNode(ctx context.Context, r *rpc.NodeRequest) (*rpc.LeaderReply, error) {
	x, e := s.M.Register(ctx, r.Node)
	return &x, e
}
func (s *Server) Heartbeat(ctx context.Context, r *rpc.NodeRequest) (*rpc.LeaderReply, error) {
	x, e := s.M.Heartbeat(ctx, r.Node)
	return &x, e
}
func (s *Server) ListNodes(context.Context, *rpc.Empty) (*rpc.NodeList, error) {
	return &rpc.NodeList{Nodes: s.M.Nodes()}, nil
}
