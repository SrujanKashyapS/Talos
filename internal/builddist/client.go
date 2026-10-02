package builddist

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"io"
)

type Client struct {
	Cache    rpc.CacheClient
	Registry rpc.RegistryClient
}

func (c *Client) Get(ctx context.Context, k string) (string, bool, error) {
	r, e := c.Cache.GetCache(ctx, &rpc.CacheRequest{Key: k})
	if e != nil {
		return "", false, e
	}
	return r.Digest, r.Found, nil
}
func (c *Client) Put(ctx context.Context, k, d string) error {
	_, e := c.Cache.PutCache(ctx, &rpc.CacheRequest{Key: k, Digest: d})
	return e
}
func (c *Client) GetLayer(ctx context.Context, d string) ([]byte, error) {
	s, e := c.Registry.GetLayer(ctx, &rpc.DigestRequest{Digest: d})
	if e != nil {
		return nil, e
	}
	var b []byte
	h := sha256.New()
	for {
		c, e := s.Recv()
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		b = append(b, c.Data...)
		_, _ = h.Write(c.Data)
	}
	got := "sha256:" + hex.EncodeToString(h.Sum(nil))
	if got != d {
		return nil, fmt.Errorf("layer digest mismatch: got %s want %s", got, d)
	}
	return b, nil
}
