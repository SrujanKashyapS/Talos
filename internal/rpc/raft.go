package rpc

import (
	"context"
	"google.golang.org/grpc"
)

type JoinRaftRequest struct {
	ID      string
	Address string
}
type RaftStatus struct {
	Leader string
	State  string
}
type RaftServer interface {
	JoinRaft(context.Context, *JoinRaftRequest) (*Empty, error)
	Leader(context.Context, *Empty) (*RaftStatus, error)
}

var RaftServiceDesc = grpc.ServiceDesc{ServiceName: "talos.v1.Raft", HandlerType: (*RaftServer)(nil), Methods: []grpc.MethodDesc{{MethodName: "JoinRaft", Handler: _Raft_Join_Handler}, {MethodName: "Leader", Handler: _Raft_Leader_Handler}}}

func RegisterRaftServer(s *grpc.Server, srv RaftServer) { s.RegisterService(&RaftServiceDesc, srv) }
func _Raft_Join_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(JoinRaftRequest)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(RaftServer).JoinRaft(ctx, in)
}
func _Raft_Leader_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(Empty)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(RaftServer).Leader(ctx, in)
}

type RaftClient interface {
	JoinRaft(context.Context, *JoinRaftRequest, ...grpc.CallOption) (*Empty, error)
	Leader(context.Context, *Empty, ...grpc.CallOption) (*RaftStatus, error)
}
type raftClient struct{ cc grpc.ClientConnInterface }

func NewRaftClient(cc grpc.ClientConnInterface) RaftClient { return &raftClient{cc} }
func (c *raftClient) JoinRaft(ctx context.Context, in *JoinRaftRequest, opts ...grpc.CallOption) (*Empty, error) {
	o := new(Empty)
	e := c.cc.Invoke(ctx, "/talos.v1.Raft/JoinRaft", in, o, opts...)
	return o, e
}
func (c *raftClient) Leader(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*RaftStatus, error) {
	o := new(RaftStatus)
	e := c.cc.Invoke(ctx, "/talos.v1.Raft/Leader", in, o, opts...)
	return o, e
}
