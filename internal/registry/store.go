package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/SrujanKashyapS/Talos/image"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type Store struct{}

func (Store) HasLayer(ctx context.Context, d string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}
	p, e := layerPath(d)
	if e != nil {
		return false, e
	}
	_, e = os.Stat(p)
	if e == nil {
		return true, nil
	}
	if os.IsNotExist(e) {
		return false, nil
	}
	return false, fmt.Errorf("stat layer: %w", e)
}
func (Store) GetLayer(ctx context.Context, d string) (io.ReadCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	p, e := layerPath(d)
	if e != nil {
		return nil, e
	}
	f, e := os.Open(p)
	if e != nil {
		return nil, fmt.Errorf("open layer %s: %w", d, e)
	}
	return f, nil
}
func (Store) PutLayer(ctx context.Context, d string, r io.Reader) (string, error) {
	b, e := io.ReadAll(io.LimitReader(r, 8<<30))
	if e != nil {
		return "", fmt.Errorf("read layer: %w", e)
	}
	h := sha256.Sum256(b)
	got := hex.EncodeToString(h[:])
	want := strings.TrimPrefix(d, "sha256:")
	if len(want) != 64 || !strings.EqualFold(got, want) {
		return "", fmt.Errorf("layer digest mismatch: got sha256:%s want %s", got, d)
	}
	if _, e = image.StoreLayer(b); e != nil {
		return "", fmt.Errorf("store layer: %w", e)
	}
	return "sha256:" + got, nil
}
func (Store) GetManifest(ctx context.Context, n, t string) (*image.Manifest, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return image.Load(n, t)
}
func (Store) PutManifest(ctx context.Context, m *image.Manifest) error {
	if m == nil {
		return fmt.Errorf("nil manifest")
	}
	for _, l := range m.Layers {
		ok, e := Store{}.HasLayer(ctx, l.Digest)
		if e != nil {
			return e
		}
		if !ok {
			return fmt.Errorf("missing layer %s", l.Digest)
		}
	}
	tmp := *m
	tmp.Digest = ""
	b, e := json.Marshal(tmp)
	if e != nil {
		return fmt.Errorf("marshal manifest: %w", e)
	}
	h := sha256.Sum256(b)
	want := "sha256:" + hex.EncodeToString(h[:])
	if m.Digest != want {
		return fmt.Errorf("manifest digest mismatch: got %s want %s", m.Digest, want)
	}
	return m.Save()
}
func layerPath(d string) (string, error) {
	d = strings.TrimPrefix(d, "sha256:")
	if len(d) != 64 {
		return "", fmt.Errorf("invalid layer digest %q", d)
	}
	if _, e := hex.DecodeString(d); e != nil {
		return "", fmt.Errorf("invalid layer digest %q: %w", d, e)
	}
	return imageLayerPath(d), nil
}
func imageLayerPath(d string) string {
	home, e := os.UserHomeDir()
	if e != nil {
		return ""
	}
	return filepath.Join(home, ".talos", "layers", d+".tar")
}

type RPCServer struct{ Store Store }

func (s *RPCServer) HasLayer(ctx context.Context, in *rpc.DigestRequest) (*rpc.BoolReply, error) {
	ok, e := s.Store.HasLayer(ctx, in.Digest)
	return &rpc.BoolReply{OK: ok}, e
}
func (s *RPCServer) GetLayer(in *rpc.DigestRequest, ss rpc.Registry_GetLayerServer) error {
	f, e := s.Store.GetLayer(ss.Context(), in.Digest)
	if e != nil {
		return e
	}
	defer f.Close()
	buf := make([]byte, 256<<10)
	for {
		n, re := f.Read(buf)
		if n > 0 {
			c := append([]byte(nil), buf[:n]...)
			if e := ss.Send(&rpc.LayerChunk{Digest: in.Digest, Data: c, Final: re == io.EOF}); e != nil {
				return e
			}
		}
		if re == io.EOF {
			return nil
		}
		if re != nil {
			return fmt.Errorf("read layer: %w", re)
		}
	}
}
func (s *RPCServer) PutLayer(ss rpc.Registry_PutLayerServer) error {
	var d string
	var b []byte
	for {
		c, e := ss.Recv()
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		if d == "" {
			d = c.Digest
		}
		if d != c.Digest {
			return fmt.Errorf("mixed layer digests")
		}
		b = append(b, c.Data...)
		if c.Final {
			break
		}
	}
	if d == "" {
		return fmt.Errorf("empty layer upload")
	}
	got, e := s.Store.PutLayer(ss.Context(), d, &bytesReader{b: b})
	if e != nil {
		return e
	}
	return ss.SendAndClose(&rpc.BoolReply{OK: got != ""})
}
func (s *RPCServer) GetManifest(ctx context.Context, in *rpc.ManifestRequest) (*rpc.ManifestEnvelope, error) {
	m, e := s.Store.GetManifest(ctx, in.Name, in.Tag)
	if e != nil {
		return nil, e
	}
	b, e := json.Marshal(m)
	if e != nil {
		return nil, e
	}
	return &rpc.ManifestEnvelope{JSON: b}, nil
}
func (s *RPCServer) PutManifest(ctx context.Context, in *rpc.ManifestEnvelope) (*rpc.BoolReply, error) {
	var m image.Manifest
	if e := json.Unmarshal(in.JSON, &m); e != nil {
		return nil, e
	}
	if e := s.Store.PutManifest(ctx, &m); e != nil {
		return nil, e
	}
	return &rpc.BoolReply{OK: true}, nil
}

type bytesReader struct {
	b []byte
	i int
}

func (b *bytesReader) Read(p []byte) (int, error) {
	if b.i >= len(b.b) {
		return 0, io.EOF
	}
	n := copy(p, b.b[b.i:])
	b.i += n
	return n, nil
}
