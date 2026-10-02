package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/SrujanKashyapS/Talos/build"
	"github.com/SrujanKashyapS/Talos/image"
	"github.com/SrujanKashyapS/Talos/internal/builddist"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"github.com/SrujanKashyapS/Talos/internal/spec"
	"github.com/SrujanKashyapS/Talos/runtime"
	"github.com/SrujanKashyapS/Talos/utils"
	"google.golang.org/grpc"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("talos: command required")
		return
	}
	var e error
	switch os.Args[1] {
	case "build":
		e = buildCmd(os.Args[2:])
	case "run":
		e = runCmd(os.Args[2:])
	case "push":
		e = pushCmd(os.Args[2:])
	case "pull":
		e = pullCmd(os.Args[2:])
	case "images":
		e = imagesCmd()
	case "rmi":
		e = rmiCmd(os.Args[2:])
	case "import":
		e = importCmd(os.Args[2:])
	case "deploy":
		e = deployCmd(os.Args[2:])
	case "ps":
		e = psCmd()
	case "nodes":
		e = nodesCmd()
	case "scale":
		e = scaleCmd(os.Args[2:])
	case "delete":
		e = deleteCmd(os.Args[2:])
	case "logs":
		e = logsCmd(os.Args[2:])
	case "leader":
		e = leaderCmd()
	default:
		fmt.Println("commands: build run push pull images rmi import deploy ps nodes scale delete logs leader")
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, "Error:", e)
		os.Exit(1)
	}
}
func buildCmd(a []string) error {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	tag := fs.String("t", "", "tag")
	no := fs.Bool("no-cache", false, "disable cache")
	server := fs.String("server", "", "shared cache/control server")
	if e := fs.Parse(a); e != nil {
		return e
	}
	d := fs.Arg(0)
	if *tag == "" || d == "" {
		return fmt.Errorf("build requires -t and context")
	}
	abs, e := filepath.Abs(d)
	if e != nil {
		return e
	}
	b, e := os.ReadFile(filepath.Join(abs, "Docksmithfile"))
	if e != nil {
		return e
	}
	n, t := utils.SplitImageRef(*tag)
	opts := build.BuildOptions{ContextDir: abs, ImageName: n, ImageTag: t, NoCache: *no}
	if *server != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cc, e := rpc.Dial(ctx, *server)
		if e != nil {
			return e
		}
		defer cc.Close()
		opts.RemoteCache = &builddist.Client{Cache: rpc.NewCacheClient(cc), Registry: rpc.NewRegistryClient(cc)}
		opts.Context = ctx
	}
	return build.Build(opts, string(b))
}
func runCmd(a []string) error {
	var env, cmd []string
	ref := ""
	for i := 0; i < len(a); i++ {
		if a[i] == "-e" {
			env = append(env, a[i+1])
			i++
			continue
		}
		if ref == "" {
			ref = a[i]
		} else {
			cmd = a[i:]
			break
		}
	}
	if ref == "" {
		return fmt.Errorf("run requires image")
	}
	n, t := utils.SplitImageRef(ref)
	return runtime.Run(runtime.RunOptions{ImageName: n, ImageTag: t, Cmd: cmd, Env: env, Remove: true})
}
func pushCmd(a []string) error {
	fs := flag.NewFlagSet("push", flag.ContinueOnError)
	server := fs.String("server", "127.0.0.1:5000", "registry/control server")
	if e := fs.Parse(a); e != nil {
		return e
	}
	ref := fs.Arg(0)
	if ref == "" {
		return fmt.Errorf("push IMAGE[:TAG]")
	}
	name, tag := utils.SplitImageRef(ref)
	m, e := image.Load(name, tag)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cc, e := rpc.Dial(ctx, *server)
	if e != nil {
		return e
	}
	defer cc.Close()
	reg := rpc.NewRegistryClient(cc)
	for _, l := range m.Layers {
		has, e := reg.HasLayer(ctx, &rpc.DigestRequest{Digest: l.Digest})
		if e != nil {
			return e
		}
		if has.OK {
			continue
		}
		p, e := utils.LayerPath(strings.TrimPrefix(l.Digest, "sha256:"))
		if e != nil {
			return e
		}
		f, e := os.Open(p)
		if e != nil {
			return e
		}
		stream, e := reg.PutLayer(ctx)
		if e != nil {
			f.Close()
			return e
		}
		buf := make([]byte, 256<<10)
		for {
			n, re := f.Read(buf)
			if n > 0 {
				chunk := &rpc.LayerChunk{Digest: l.Digest, Data: append([]byte(nil), buf[:n]...), Final: re == io.EOF}
				if e := stream.Send(chunk); e != nil {
					f.Close()
					return e
				}
			}
			if re == io.EOF {
				break
			}
			if re != nil {
				f.Close()
				return re
			}
		}
		f.Close()
		if _, e := stream.CloseAndRecv(); e != nil {
			return e
		}
	}
	b, e := json.Marshal(m)
	if e != nil {
		return e
	}
	_, e = reg.PutManifest(ctx, &rpc.ManifestEnvelope{JSON: b})
	return e
}
func pullCmd(a []string) error {
	fs := flag.NewFlagSet("pull", flag.ContinueOnError)
	server := fs.String("server", "127.0.0.1:5000", "registry/control server")
	if e := fs.Parse(a); e != nil {
		return e
	}
	ref := fs.Arg(0)
	if ref == "" {
		return fmt.Errorf("pull IMAGE[:TAG]")
	}
	name, tag := utils.SplitImageRef(ref)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cc, e := rpc.Dial(ctx, *server)
	if e != nil {
		return e
	}
	defer cc.Close()
	reg := rpc.NewRegistryClient(cc)
	env, e := reg.GetManifest(ctx, &rpc.ManifestRequest{Name: name, Tag: tag})
	if e != nil {
		return e
	}
	var m image.Manifest
	if e := json.Unmarshal(env.JSON, &m); e != nil {
		return e
	}
	for _, l := range m.Layers {
		if _, e := image.LayerSize(l.Digest); e == nil {
			continue
		}
		stream, e := reg.GetLayer(ctx, &rpc.DigestRequest{Digest: l.Digest})
		if e != nil {
			return e
		}
		var data []byte
		for {
			chunk, e := stream.Recv()
			if e == io.EOF {
				break
			}
			if e != nil {
				return e
			}
			data = append(data, chunk.Data...)
		}
		got, e := image.StoreLayer(data)
		if e != nil {
			return e
		}
		want := strings.TrimPrefix(l.Digest, "sha256:")
		if got != want {
			return fmt.Errorf("layer digest mismatch: got %s want %s", got, want)
		}
	}
	return m.Save()
}
func imagesCmd() error {
	ms, e := image.ListAll()
	if e != nil {
		return e
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "REPOSITORY\tTAG\tDIGEST\tCREATED")
	for _, m := range ms {
		d := m.Digest
		if len(d) > 19 {
			d = d[:19]
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", m.Name, m.Tag, d, m.Created.Format("2006-01-02 15:04:05"))
	}
	return w.Flush()
}
func rmiCmd(a []string) error {
	if len(a) == 0 {
		return fmt.Errorf("rmi requires image")
	}
	for _, r := range a {
		n, t := utils.SplitImageRef(r)
		if e := image.Delete(n, t); e != nil {
			return e
		}
	}
	return nil
}
func importCmd(a []string) error {
	if len(a) < 2 {
		return fmt.Errorf("import requires dir and image")
	}
	src, _ := filepath.Abs(a[0])
	b, e := utils.CreateTarFromDir(src)
	if e != nil {
		return e
	}
	d, e := image.StoreLayer(b)
	if e != nil {
		return e
	}
	sz, _ := image.LayerSize(d)
	n, t := utils.SplitImageRef(a[1])
	m := &image.Manifest{Name: n, Tag: t, Created: time.Now().UTC(), Config: image.Config{Cmd: []string{"/bin/sh"}, WorkingDir: "/"}, Layers: []image.LayerInfo{{Digest: d, Size: sz, CreatedBy: "import " + src}}}
	return m.Save()
}
func controlConn() (*grpc.ClientConn, error) { return rpc.Dial(context.Background(), "127.0.0.1:5000") }
func callRetry(fn func(rpc.ControlClient) error) error {
	for _, addr := range []string{"127.0.0.1:5000"} {
		cc, e := rpc.Dial(context.Background(), addr)
		if e != nil {
			continue
		}
		c := rpc.NewControlClient(cc)
		e = fn(c)
		cc.Close()
		if e == nil {
			return nil
		}
		s := e.Error()
		if i := strings.Index(s, "not leader: "); i >= 0 {
			leader := strings.TrimSpace(s[i+12:])
			if leader != "" {
				cc, e = rpc.Dial(context.Background(), leader)
				if e == nil {
					c = rpc.NewControlClient(cc)
					e = fn(c)
					cc.Close()
					if e == nil {
						return nil
					}
				}
			}
		}
	}
	return fmt.Errorf("control request failed")
}
func deployCmd(a []string) error {
	fs := flag.NewFlagSet("deploy", flag.ContinueOnError)
	f := fs.String("f", "", "manifest")
	if e := fs.Parse(a); e != nil {
		return e
	}
	if *f == "" {
		return fmt.Errorf("deploy -f file")
	}
	b, e := os.ReadFile(*f)
	if e != nil {
		return e
	}
	_, e = spec.Parse(b)
	if e != nil {
		return e
	}
	return callRetry(func(c rpc.ControlClient) error {
		_, e := c.ApplyWorkload(context.Background(), &rpc.ApplyWorkloadRequest{Spec: b})
		return e
	})
}
func psCmd() error {
	return callRetry(func(c rpc.ControlClient) error {
		r, e := c.ListWorkloads(context.Background(), &rpc.Empty{})
		if e != nil {
			return e
		}
		for _, x := range r.Items {
			fmt.Printf("%s\t%s\t%d/%d\n", x.Name, x.Image, x.Running, x.Desired)
		}
		return nil
	})
}
func nodesCmd() error {
	return callRetry(func(c rpc.ControlClient) error {
		r, e := c.ListControlNodes(context.Background(), &rpc.Empty{})
		if e != nil {
			return e
		}
		for _, n := range r.Nodes {
			fmt.Printf("%s\t%s\t%t\t%d containers\n", n.ID, n.Address, n.Alive, n.Containers)
		}
		return nil
	})
}
func scaleCmd(a []string) error {
	if len(a) < 2 {
		return fmt.Errorf("scale NAME REPLICAS")
	}
	n := a[0]
	var r int
	fmt.Sscan(a[1], &r)
	return callRetry(func(c rpc.ControlClient) error {
		_, e := c.ScaleWorkload(context.Background(), &rpc.ScaleRequest{Name: n, Replicas: int32(r)})
		return e
	})
}
func deleteCmd(a []string) error {
	if len(a) < 1 {
		return fmt.Errorf("delete NAME")
	}
	return callRetry(func(c rpc.ControlClient) error {
		_, e := c.DeleteWorkload(context.Background(), &rpc.DeleteWorkloadRequest{Name: a[0]})
		return e
	})
}
func logsCmd(a []string) error {
	if len(a) < 1 {
		return fmt.Errorf("logs NAME [REPLICA]")
	}
	r := 0
	if len(a) > 1 {
		fmt.Sscan(a[1], &r)
	}
	return callRetry(func(c rpc.ControlClient) error {
		x, e := c.GetWorkloadLogs(context.Background(), &rpc.LogsRequest{Name: a[0], Replica: int32(r)})
		if e == nil {
			_, e = io.Copy(os.Stdout, strings.NewReader(string(x.Data)))
		}
		return e
	})
}
func leaderCmd() error {
	cc, e := controlConn()
	if e != nil {
		return e
	}
	defer cc.Close()
	c := rpc.NewRaftClient(cc)
	r, e := c.Leader(context.Background(), &rpc.Empty{})
	if e != nil {
		return e
	}
	fmt.Printf("%s\t%s\n", r.Leader, r.State)
	return nil
}

var _ = json.Valid
