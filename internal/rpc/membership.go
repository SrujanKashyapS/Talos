package rpc

import (
	"context"
	"google.golang.org/grpc"
)

type NodeInfo struct {
	ID             string
	Address        string
	CPUCapacity    float64
	CPUUsed        float64
	MemoryCapacity int64
	MemoryUsed     int64
	Containers     int32
	HeldLayers     []string
	LastHeartbeat  int64
	Alive          bool
	Missed         int32
}
type NodeRequest struct{ Node NodeInfo }
type NodeList struct{ Nodes []NodeInfo }
type LeaderReply struct{ Address string }
type MembershipServer interface {
	RegisterNode(context.Context, *NodeRequest) (*LeaderReply, error)
	Heartbeat(context.Context, *NodeRequest) (*LeaderReply, error)
	ListNodes(context.Context, *Empty) (*NodeList, error)
}

var MembershipServiceDesc = grpc.ServiceDesc{ServiceName: "talos.v1.Membership", HandlerType: (*MembershipServer)(nil), Methods: []grpc.MethodDesc{{MethodName: "RegisterNode", Handler: _Membership_RegisterNode_Handler}, {MethodName: "Heartbeat", Handler: _Membership_Heartbeat_Handler}, {MethodName: "ListNodes", Handler: _Membership_ListNodes_Handler}}}

func RegisterMembershipServer(s *grpc.Server, srv MembershipServer) {
	s.RegisterService(&MembershipServiceDesc, srv)
}
func _Membership_RegisterNode_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(NodeRequest)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(MembershipServer).RegisterNode(ctx, in)
}
func _Membership_Heartbeat_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(NodeRequest)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(MembershipServer).Heartbeat(ctx, in)
}
func _Membership_ListNodes_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(Empty)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(MembershipServer).ListNodes(ctx, in)
}

type MembershipClient interface {
	RegisterNode(context.Context, *NodeRequest, ...grpc.CallOption) (*LeaderReply, error)
	Heartbeat(context.Context, *NodeRequest, ...grpc.CallOption) (*LeaderReply, error)
	ListNodes(context.Context, *Empty, ...grpc.CallOption) (*NodeList, error)
}
type membershipClient struct{ cc grpc.ClientConnInterface }

func NewMembershipClient(cc grpc.ClientConnInterface) MembershipClient { return &membershipClient{cc} }
func (c *membershipClient) RegisterNode(ctx context.Context, in *NodeRequest, opts ...grpc.CallOption) (*LeaderReply, error) {
	o := new(LeaderReply)
	e := c.cc.Invoke(ctx, "/talos.v1.Membership/RegisterNode", in, o, opts...)
	return o, e
}
func (c *membershipClient) Heartbeat(ctx context.Context, in *NodeRequest, opts ...grpc.CallOption) (*LeaderReply, error) {
	o := new(LeaderReply)
	e := c.cc.Invoke(ctx, "/talos.v1.Membership/Heartbeat", in, o, opts...)
	return o, e
}
func (c *membershipClient) ListNodes(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*NodeList, error) {
	o := new(NodeList)
	e := c.cc.Invoke(ctx, "/talos.v1.Membership/ListNodes", in, o, opts...)
	return o, e
}
