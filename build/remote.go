package build

import "context"

type RemoteCache interface {
	Get(context.Context, string) (string, bool, error)
	Put(context.Context, string, string) error
	GetLayer(context.Context, string) ([]byte, error)
}
