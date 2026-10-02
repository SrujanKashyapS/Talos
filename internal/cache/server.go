package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/SrujanKashyapS/Talos/internal/raftstore"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"strings"
	"time"
)

type Server struct {
	Node     *raftstore.Node
	OnLookup func(bool)
}

func (s *Server) GetCache(ctx context.Context, r *rpc.CacheRequest) (*rpc.CacheReply, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	d, ok := s.Node.FSM.State().Cache[r.Key]
	if s.OnLookup != nil {
		s.OnLookup(ok)
	}
	return &rpc.CacheReply{Digest: d, Found: ok}, nil
}
func (s *Server) PutCache(ctx context.Context, r *rpc.CacheRequest) (*rpc.Empty, error) {
	if !s.Node.IsLeader() {
		return nil, fmt.Errorf("not leader: %s", s.Node.Leader())
	}
	if strings.TrimSpace(r.Key) == "" || strings.TrimSpace(r.Digest) == "" {
		return nil, fmt.Errorf("cache key and digest are required")
	}
	b, _ := json.Marshal(r.Digest)
	if _, e := s.Node.Apply(raftstore.Command{Op: raftstore.OpCache, Key: r.Key, Value: b}, 3*time.Second); e != nil {
		return nil, e
	}
	return &rpc.Empty{}, nil
}
