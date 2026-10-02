package rpc

import (
	"context"
	"google.golang.org/grpc"
)

type CacheRequest struct {
	Key    string
	Digest string
}
type CacheReply struct {
	Digest string
	Found  bool
}
type CacheServer interface {
	GetCache(context.Context, *CacheRequest) (*CacheReply, error)
	PutCache(context.Context, *CacheRequest) (*Empty, error)
}

var CacheServiceDesc = grpc.ServiceDesc{ServiceName: "talos.v1.Cache", HandlerType: (*CacheServer)(nil), Methods: []grpc.MethodDesc{{MethodName: "GetCache", Handler: _Cache_Get_Handler}, {MethodName: "PutCache", Handler: _Cache_Put_Handler}}}

func RegisterCacheServer(s *grpc.Server, srv CacheServer) { s.RegisterService(&CacheServiceDesc, srv) }
func _Cache_Get_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(CacheRequest)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(CacheServer).GetCache(ctx, in)
}
func _Cache_Put_Handler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(CacheRequest)
	if e := dec(in); e != nil {
		return nil, e
	}
	return srv.(CacheServer).PutCache(ctx, in)
}

type CacheClient interface {
	GetCache(context.Context, *CacheRequest, ...grpc.CallOption) (*CacheReply, error)
	PutCache(context.Context, *CacheRequest, ...grpc.CallOption) (*Empty, error)
}
type cacheClient struct{ cc grpc.ClientConnInterface }

func NewCacheClient(cc grpc.ClientConnInterface) CacheClient { return &cacheClient{cc} }
func (c *cacheClient) GetCache(ctx context.Context, in *CacheRequest, opts ...grpc.CallOption) (*CacheReply, error) {
	o := new(CacheReply)
	e := c.cc.Invoke(ctx, "/talos.v1.Cache/GetCache", in, o, opts...)
	return o, e
}
func (c *cacheClient) PutCache(ctx context.Context, in *CacheRequest, opts ...grpc.CallOption) (*Empty, error) {
	o := new(Empty)
	e := c.cc.Invoke(ctx, "/talos.v1.Cache/PutCache", in, o, opts...)
	return o, e
}
