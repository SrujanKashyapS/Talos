package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (jsonCodec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
func (jsonCodec) Name() string                    { return "json" }
func init()                                       { encoding.RegisterCodec(jsonCodec{}) }
func Dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	cc, e := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})))
	if e != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, e)
	}
	return cc, nil
}
func Server(opts ...grpc.ServerOption) *grpc.Server {
	return grpc.NewServer(append([]grpc.ServerOption{grpc.ForceServerCodec(jsonCodec{})}, opts...)...)
}

type Empty struct{}
type DigestRequest struct{ Digest string }
type BoolReply struct{ OK bool }
type LayerChunk struct {
	Digest string
	Data   []byte
	Final  bool
	Total  uint64
}
type ManifestRequest struct {
	Name string
	Tag  string
}
type ManifestEnvelope struct{ JSON []byte }
type ResourceSpec struct {
	CPU    string
	Memory int64
}
type RunContainerRequest struct {
	ID        string
	ImageName string
	ImageTag  string
	Command   []string
	Env       []string
	Workload  string
	Replica   int32
	Resources ResourceSpec
	Chroot    bool
}
type RunContainerResponse struct {
	ID     string
	Status string
	PID    int32
}
type ContainerInfo struct {
	ID       string
	Status   string
	PID      int32
	Image    string
	Workload string
	Replica  int32
	ExitCode int32
}
type ContainerList struct{ Items []ContainerInfo }
type LogReply struct{ Data []byte }

type RegistryServer interface {
	HasLayer(context.Context, *DigestRequest) (*BoolReply, error)
	GetLayer(*DigestRequest, Registry_GetLayerServer) error
	PutLayer(Registry_PutLayerServer) error
	GetManifest(context.Context, *ManifestRequest) (*ManifestEnvelope, error)
	PutManifest(context.Context, *ManifestEnvelope) (*BoolReply, error)
}
type Registry_GetLayerServer interface {
	Send(*LayerChunk) error
	grpc.ServerStream
}
type Registry_PutLayerServer interface {
	Recv() (*LayerChunk, error)
	SendAndClose(*BoolReply) error
	grpc.ServerStream
}

var RegistryServiceDesc = grpc.ServiceDesc{ServiceName: "talos.v1.Registry", HandlerType: (*RegistryServer)(nil), Methods: []grpc.MethodDesc{{MethodName: "HasLayer", Handler: _Registry_HasLayer_Handler}, {MethodName: "GetManifest", Handler: _Registry_GetManifest_Handler}, {MethodName: "PutManifest", Handler: _Registry_PutManifest_Handler}}, Streams: []grpc.StreamDesc{{StreamName: "GetLayer", Handler: _Registry_GetLayer_Handler, ServerStreams: true}, {StreamName: "PutLayer", Handler: _Registry_PutLayer_Handler, ClientStreams: true}}}

func RegisterRegistryServer(s *grpc.Server, srv RegistryServer) {
	s.RegisterService(&RegistryServiceDesc, srv)
}
func _Registry_HasLayer_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(DigestRequest)
	if e := dec(in); e != nil {
		return nil, e
	}
	if interceptor == nil {
		return srv.(RegistryServer).HasLayer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/talos.v1.Registry/HasLayer"}
	h := func(ctx context.Context, req any) (any, error) {
		return srv.(RegistryServer).HasLayer(ctx, req.(*DigestRequest))
	}
	return interceptor(ctx, in, info, h)
}
func _Registry_GetManifest_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(ManifestRequest)
	if e := dec(in); e != nil {
		return nil, e
	}
	if interceptor == nil {
		return srv.(RegistryServer).GetManifest(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/talos.v1.Registry/GetManifest"}
	h := func(ctx context.Context, req any) (any, error) {
		return srv.(RegistryServer).GetManifest(ctx, req.(*ManifestRequest))
	}
	return interceptor(ctx, in, info, h)
}
func _Registry_PutManifest_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(ManifestEnvelope)
	if e := dec(in); e != nil {
		return nil, e
	}
	if interceptor == nil {
		return srv.(RegistryServer).PutManifest(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/talos.v1.Registry/PutManifest"}
	h := func(ctx context.Context, req any) (any, error) {
		return srv.(RegistryServer).PutManifest(ctx, req.(*ManifestEnvelope))
	}
	return interceptor(ctx, in, info, h)
}
func _Registry_GetLayer_Handler(srv any, stream grpc.ServerStream) error {
	in := new(DigestRequest)
	if e := stream.RecvMsg(in); e != nil {
		return e
	}
	return srv.(RegistryServer).GetLayer(in, &registryGetLayerServer{stream})
}
func _Registry_PutLayer_Handler(srv any, stream grpc.ServerStream) error {
	return srv.(RegistryServer).PutLayer(&registryPutLayerServer{stream})
}

type registryGetLayerServer struct{ grpc.ServerStream }

func (s *registryGetLayerServer) Send(v *LayerChunk) error { return s.SendMsg(v) }

type registryPutLayerServer struct{ grpc.ServerStream }

func (s *registryPutLayerServer) Recv() (*LayerChunk, error) {
	v := new(LayerChunk)
	e := s.RecvMsg(v)
	return v, e
}
func (s *registryPutLayerServer) SendAndClose(v *BoolReply) error { return s.SendMsg(v) }

type RegistryClient interface {
	HasLayer(context.Context, *DigestRequest, ...grpc.CallOption) (*BoolReply, error)
	GetLayer(context.Context, *DigestRequest, ...grpc.CallOption) (Registry_GetLayerClient, error)
	PutLayer(context.Context, ...grpc.CallOption) (Registry_PutLayerClient, error)
	GetManifest(context.Context, *ManifestRequest, ...grpc.CallOption) (*ManifestEnvelope, error)
	PutManifest(context.Context, *ManifestEnvelope, ...grpc.CallOption) (*BoolReply, error)
}
type registryClient struct{ cc grpc.ClientConnInterface }

func NewRegistryClient(cc grpc.ClientConnInterface) RegistryClient { return &registryClient{cc} }
func (c *registryClient) HasLayer(ctx context.Context, in *DigestRequest, opts ...grpc.CallOption) (*BoolReply, error) {
	out := new(BoolReply)
	e := c.cc.Invoke(ctx, "/talos.v1.Registry/HasLayer", in, out, opts...)
	return out, e
}
func (c *registryClient) GetManifest(ctx context.Context, in *ManifestRequest, opts ...grpc.CallOption) (*ManifestEnvelope, error) {
	out := new(ManifestEnvelope)
	e := c.cc.Invoke(ctx, "/talos.v1.Registry/GetManifest", in, out, opts...)
	return out, e
}
func (c *registryClient) PutManifest(ctx context.Context, in *ManifestEnvelope, opts ...grpc.CallOption) (*BoolReply, error) {
	out := new(BoolReply)
	e := c.cc.Invoke(ctx, "/talos.v1.Registry/PutManifest", in, out, opts...)
	return out, e
}

type Registry_GetLayerClient interface {
	Recv() (*LayerChunk, error)
	grpc.ClientStream
}

func (c *registryClient) GetLayer(ctx context.Context, in *DigestRequest, opts ...grpc.CallOption) (Registry_GetLayerClient, error) {
	stream, e := c.cc.NewStream(ctx, &RegistryServiceDesc.Streams[0], "/talos.v1.Registry/GetLayer", opts...)
	if e != nil {
		return nil, e
	}
	if e = stream.SendMsg(in); e != nil {
		return nil, e
	}
	if e = stream.CloseSend(); e != nil {
		return nil, e
	}
	return &registryGetLayerClient{stream}, nil
}

type registryGetLayerClient struct{ grpc.ClientStream }

func (c *registryGetLayerClient) Recv() (*LayerChunk, error) {
	v := new(LayerChunk)
	e := c.RecvMsg(v)
	return v, e
}

type Registry_PutLayerClient interface {
	Send(*LayerChunk) error
	CloseAndRecv() (*BoolReply, error)
	grpc.ClientStream
}

func (c *registryClient) PutLayer(ctx context.Context, opts ...grpc.CallOption) (Registry_PutLayerClient, error) {
	stream, e := c.cc.NewStream(ctx, &RegistryServiceDesc.Streams[1], "/talos.v1.Registry/PutLayer", opts...)
	if e != nil {
		return nil, e
	}
	return &registryPutLayerClient{stream}, nil
}

type registryPutLayerClient struct{ grpc.ClientStream }

func (c *registryPutLayerClient) Send(v *LayerChunk) error { return c.ClientStream.SendMsg(v) }
func (c *registryPutLayerClient) CloseAndRecv() (*BoolReply, error) {
	if e := c.ClientStream.CloseSend(); e != nil {
		return nil, e
	}
	out := new(BoolReply)
	e := c.ClientStream.RecvMsg(out)
	return out, e
}

type AgentServer interface {
	RunContainer(context.Context, *RunContainerRequest) (*RunContainerResponse, error)
	StopContainer(context.Context, *RunContainerRequest) (*Empty, error)
	ListContainers(context.Context, *Empty) (*ContainerList, error)
	GetLogs(context.Context, *RunContainerRequest) (*LogReply, error)
}

var AgentServiceDesc = grpc.ServiceDesc{ServiceName: "talos.v1.Agent", HandlerType: (*AgentServer)(nil), Methods: []grpc.MethodDesc{{MethodName: "RunContainer", Handler: _Agent_RunContainer_Handler}, {MethodName: "StopContainer", Handler: _Agent_StopContainer_Handler}, {MethodName: "ListContainers", Handler: _Agent_ListContainers_Handler}, {MethodName: "GetLogs", Handler: _Agent_GetLogs_Handler}}}

func RegisterAgentServer(s *grpc.Server, srv AgentServer) { s.RegisterService(&AgentServiceDesc, srv) }
func _Agent_RunContainer_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(RunContainerRequest)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(AgentServer).RunContainer(ctx, in)
}
func _Agent_StopContainer_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(RunContainerRequest)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(AgentServer).StopContainer(ctx, in)
}
func _Agent_ListContainers_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(Empty)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(AgentServer).ListContainers(ctx, in)
}
func _Agent_GetLogs_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(RunContainerRequest)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(AgentServer).GetLogs(ctx, in)
}

type AgentClient interface {
	RunContainer(context.Context, *RunContainerRequest, ...grpc.CallOption) (*RunContainerResponse, error)
	StopContainer(context.Context, *RunContainerRequest, ...grpc.CallOption) (*Empty, error)
	ListContainers(context.Context, *Empty, ...grpc.CallOption) (*ContainerList, error)
	GetLogs(context.Context, *RunContainerRequest, ...grpc.CallOption) (*LogReply, error)
}
type agentClient struct{ cc grpc.ClientConnInterface }

func NewAgentClient(cc grpc.ClientConnInterface) AgentClient { return &agentClient{cc} }
func (c *agentClient) RunContainer(ctx context.Context, in *RunContainerRequest, opts ...grpc.CallOption) (*RunContainerResponse, error) {
	out := new(RunContainerResponse)
	e := c.cc.Invoke(ctx, "/talos.v1.Agent/RunContainer", in, out, opts...)
	return out, e
}
func (c *agentClient) StopContainer(ctx context.Context, in *RunContainerRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	e := c.cc.Invoke(ctx, "/talos.v1.Agent/StopContainer", in, out, opts...)
	return out, e
}
func (c *agentClient) ListContainers(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*ContainerList, error) {
	out := new(ContainerList)
	e := c.cc.Invoke(ctx, "/talos.v1.Agent/ListContainers", in, out, opts...)
	return out, e
}
func (c *agentClient) GetLogs(ctx context.Context, in *RunContainerRequest, opts ...grpc.CallOption) (*LogReply, error) {
	out := new(LogReply)
	e := c.cc.Invoke(ctx, "/talos.v1.Agent/GetLogs", in, out, opts...)
	return out, e
}
