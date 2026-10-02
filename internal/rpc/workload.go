package rpc

import (
	"context"
	"google.golang.org/grpc"
)

type ApplyWorkloadRequest struct{ Spec []byte }
type ScaleRequest struct {
	Name     string
	Replicas int32
}
type DeleteWorkloadRequest struct{ Name string }
type WorkloadStatus struct {
	Name    string
	Image   string
	Desired int32
	Running int32
}
type WorkloadList struct{ Items []WorkloadStatus }
type LogsRequest struct {
	Name    string
	Replica int32
}
type LogsReply struct{ Data []byte }
type ControlServer interface {
	ApplyWorkload(context.Context, *ApplyWorkloadRequest) (*Empty, error)
	ScaleWorkload(context.Context, *ScaleRequest) (*Empty, error)
	DeleteWorkload(context.Context, *DeleteWorkloadRequest) (*Empty, error)
	ListWorkloads(context.Context, *Empty) (*WorkloadList, error)
	ListControlNodes(context.Context, *Empty) (*NodeList, error)
	GetWorkloadLogs(context.Context, *LogsRequest) (*LogsReply, error)
}

var ControlServiceDesc = grpc.ServiceDesc{ServiceName: "talos.v1.Control", HandlerType: (*ControlServer)(nil), Methods: []grpc.MethodDesc{{MethodName: "ApplyWorkload", Handler: _Control_Apply_Handler}, {MethodName: "ScaleWorkload", Handler: _Control_Scale_Handler}, {MethodName: "DeleteWorkload", Handler: _Control_Delete_Handler}, {MethodName: "ListWorkloads", Handler: _Control_List_Handler}, {MethodName: "ListControlNodes", Handler: _Control_Nodes_Handler}, {MethodName: "GetWorkloadLogs", Handler: _Control_Logs_Handler}}}

func RegisterControlServer(s *grpc.Server, srv ControlServer) {
	s.RegisterService(&ControlServiceDesc, srv)
}
func _Control_Apply_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(ApplyWorkloadRequest)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(ControlServer).ApplyWorkload(ctx, in)
}
func _Control_Scale_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(ScaleRequest)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(ControlServer).ScaleWorkload(ctx, in)
}
func _Control_Delete_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(DeleteWorkloadRequest)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(ControlServer).DeleteWorkload(ctx, in)
}
func _Control_List_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(Empty)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(ControlServer).ListWorkloads(ctx, in)
}
func _Control_Nodes_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(Empty)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(ControlServer).ListControlNodes(ctx, in)
}
func _Control_Logs_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(LogsRequest)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(ControlServer).GetWorkloadLogs(ctx, in)
}

type ControlClient interface {
	ApplyWorkload(context.Context, *ApplyWorkloadRequest, ...grpc.CallOption) (*Empty, error)
	ScaleWorkload(context.Context, *ScaleRequest, ...grpc.CallOption) (*Empty, error)
	DeleteWorkload(context.Context, *DeleteWorkloadRequest, ...grpc.CallOption) (*Empty, error)
	ListWorkloads(context.Context, *Empty, ...grpc.CallOption) (*WorkloadList, error)
	ListControlNodes(context.Context, *Empty, ...grpc.CallOption) (*NodeList, error)
	GetWorkloadLogs(context.Context, *LogsRequest, ...grpc.CallOption) (*LogsReply, error)
}
type controlClient struct{ cc grpc.ClientConnInterface }

func NewControlClient(cc grpc.ClientConnInterface) ControlClient { return &controlClient{cc} }
func (c *controlClient) ApplyWorkload(ctx context.Context, in *ApplyWorkloadRequest, opts ...grpc.CallOption) (*Empty, error) {
	o := new(Empty)
	e := c.cc.Invoke(ctx, "/talos.v1.Control/ApplyWorkload", in, o, opts...)
	return o, e
}
func (c *controlClient) ScaleWorkload(ctx context.Context, in *ScaleRequest, opts ...grpc.CallOption) (*Empty, error) {
	o := new(Empty)
	e := c.cc.Invoke(ctx, "/talos.v1.Control/ScaleWorkload", in, o, opts...)
	return o, e
}
func (c *controlClient) DeleteWorkload(ctx context.Context, in *DeleteWorkloadRequest, opts ...grpc.CallOption) (*Empty, error) {
	o := new(Empty)
	e := c.cc.Invoke(ctx, "/talos.v1.Control/DeleteWorkload", in, o, opts...)
	return o, e
}
func (c *controlClient) ListWorkloads(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*WorkloadList, error) {
	o := new(WorkloadList)
	e := c.cc.Invoke(ctx, "/talos.v1.Control/ListWorkloads", in, o, opts...)
	return o, e
}
func (c *controlClient) ListControlNodes(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*NodeList, error) {
	o := new(NodeList)
	e := c.cc.Invoke(ctx, "/talos.v1.Control/ListControlNodes", in, o, opts...)
	return o, e
}
func (c *controlClient) GetWorkloadLogs(ctx context.Context, in *LogsRequest, opts ...grpc.CallOption) (*LogsReply, error) {
	o := new(LogsReply)
	e := c.cc.Invoke(ctx, "/talos.v1.Control/GetWorkloadLogs", in, o, opts...)
	return o, e
}
