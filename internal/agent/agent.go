package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/SrujanKashyapS/Talos/image"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"github.com/SrujanKashyapS/Talos/utils"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

var terminationGracePeriod = 5 * time.Second

type record struct {
	ID       string
	Status   string
	PID      int
	Image    string
	Workload string
	Replica  int32
	ExitCode int
	Started  time.Time
	Finished time.Time
	Rootfs   string
	Log      string
	CPU      float64
	Memory   int64
}
type Manager struct {
	mu    sync.Mutex
	items map[string]*record
	path  string
}

func New() (*Manager, error) {
	if e := utils.EnsureDirs(); e != nil {
		return nil, e
	}
	root, e := utils.DocksmithRoot()
	if e != nil {
		return nil, e
	}
	m := &Manager{items: map[string]*record{}, path: filepath.Join(root, "containers.json")}
	if b, e := os.ReadFile(m.path); e == nil {
		_ = json.Unmarshal(b, &m.items)
	}
	for _, r := range m.items {
		if r.Status == "running" && !alive(r.PID) {
			r.Status = "exited"
			r.Finished = time.Now().UTC()
		}
	}
	return m, m.save()
}
func (m *Manager) save() error {
	b, e := json.MarshalIndent(m.items, "", "  ")
	if e != nil {
		return e
	}
	tmp := m.path + ".tmp"
	if e = os.WriteFile(tmp, b, 0o644); e != nil {
		return e
	}
	return os.Rename(tmp, m.path)
}
func newID() string {
	b := make([]byte, 8)
	if _, e := rand.Read(b); e == nil {
		return hex.EncodeToString(b)
	}
	return fmt.Sprintf("%x", time.Now().UnixNano())
}
func alive(pid int) bool { return pid > 0 && syscall.Kill(pid, 0) == nil }
func validID(id string) bool {
	return id != "" && filepath.Base(id) == id && id != "."
}
func (m *Manager) runningResponse(id string) (*rpc.RunContainerResponse, bool) {
	r, ok := m.items[id]
	if !ok || r.Status != "running" {
		return nil, false
	}
	return &rpc.RunContainerResponse{ID: r.ID, Status: r.Status, PID: int32(r.PID)}, true
}

func (m *Manager) prepareID(id string) error {
	r, ok := m.items[id]
	if !ok {
		return nil
	}
	if r.Status == "running" || alive(r.PID) {
		return fmt.Errorf("container %s already exists", id)
	}
	containerDir := filepath.Join(filepath.Dir(m.path), "containers", id)
	if err := os.RemoveAll(containerDir); err != nil {
		return fmt.Errorf("remove previous container %s: %w", id, err)
	}
	delete(m.items, id)
	if err := m.save(); err != nil {
		m.items[id] = r
		return fmt.Errorf("clear previous container %s: %w", id, err)
	}
	return nil
}
func (m *Manager) Run(ctx context.Context, req *rpc.RunContainerRequest) (*rpc.RunContainerResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := req.ID
	if id == "" {
		id = "talos-" + newID()
	}
	if !validID(id) {
		return nil, fmt.Errorf("invalid container id %q", id)
	}
	if response, ok := m.runningResponse(id); ok {
		return response, nil
	}
	if err := m.prepareID(id); err != nil {
		return nil, err
	}
	man, e := image.Load(req.ImageName, req.ImageTag)
	if e != nil {
		return nil, e
	}
	root := filepath.Join(filepath.Dir(m.path), "containers", id, "rootfs")
	if e = os.MkdirAll(root, 0o755); e != nil {
		return nil, e
	}
	if e = image.ExtractManifestLayers(man, root); e != nil {
		return nil, fmt.Errorf("extracting image: %w", e)
	}
	ensure(root)
	cmd := man.Config.Cmd
	if len(req.Command) > 0 {
		cmd = req.Command
	}
	if len(cmd) == 0 {
		return nil, fmt.Errorf("no command")
	}
	work := man.Config.WorkingDir
	if work == "" {
		work = "/"
	}
	dir := filepath.Join(root, strings.TrimPrefix(work, "/"))
	if e = os.MkdirAll(dir, 0o755); e != nil {
		return nil, e
	}
	bin := cmd[0]
	if filepath.IsAbs(bin) {
		bin = filepath.Join(root, strings.TrimPrefix(bin, "/"))
	} else {
		bin = filepath.Join(dir, bin)
	}
	c := exec.CommandContext(ctx, bin, cmd[1:]...)
	c.Dir = dir
	c.Env = merge(man.Config.Env, req.Env)
	logPath := filepath.Join(filepath.Dir(m.path), "containers", id, "console.log")
	if e = os.MkdirAll(filepath.Dir(logPath), 0o755); e != nil {
		return nil, e
	}
	lf, e := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if e != nil {
		return nil, e
	}
	c.Stdout = lf
	c.Stderr = lf
	if e = c.Start(); e != nil {
		lf.Close()
		return nil, fmt.Errorf("start container: %w", e)
	}
	r := &record{ID: id, Status: "running", PID: c.Process.Pid, Image: req.ImageName + ":" + req.ImageTag, Workload: req.Workload, Replica: req.Replica, Started: time.Now().UTC(), Rootfs: root, Log: logPath}
	m.items[id] = r
	if e = m.save(); e != nil {
		return nil, e
	}
	go func() {
		e := c.Wait()
		lf.Close()
		m.mu.Lock()
		defer m.mu.Unlock()
		x, ok := m.items[id]
		if !ok || x.PID != c.Process.Pid {
			return
		}
		x.Finished = time.Now().UTC()
		x.Status = "exited"
		if e != nil {
			if ee, ok := e.(*exec.ExitError); ok {
				x.ExitCode = ee.ExitCode()
			} else {
				x.ExitCode = -1
			}
		}
		_ = m.save()
	}()
	return &rpc.RunContainerResponse{ID: id, Status: "running", PID: int32(c.Process.Pid)}, nil
}
func (m *Manager) Stop(ctx context.Context, id string) error {
	m.mu.Lock()
	r, ok := m.items[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("container %s not found", id)
	}
	if r.Status != "running" {
		m.mu.Unlock()
		return nil
	}
	pid := r.PID
	m.mu.Unlock()
	if e := terminate(pid, terminationGracePeriod); e != nil {
		return fmt.Errorf("stop container: %w", e)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok = m.items[id]; ok && r.Status == "running" {
		r.Status = "stopped"
		r.Finished = time.Now().UTC()
	}
	return m.save()
}

func terminate(pid int, grace time.Duration) error {
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	deadline := time.Now().Add(grace)
	for alive(pid) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !alive(pid) {
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}
func (m *Manager) List() []*record {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*record, 0, len(m.items))
	for _, r := range m.items {
		x := *r
		out = append(out, &x)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (m *Manager) AllocatedResources() (float64, int64, int32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var cpu float64
	var memory int64
	var containers int32
	for _, r := range m.items {
		if r.Status != "running" {
			continue
		}
		cpu += r.CPU
		memory += r.Memory
		containers++
	}
	return cpu, memory, containers
}
func (m *Manager) Logs(id string) ([]byte, error) {
	m.mu.Lock()
	r, ok := m.items[id]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("container %s not found", id)
	}
	b, e := os.ReadFile(r.Log)
	if e != nil {
		return nil, fmt.Errorf("read logs: %w", e)
	}
	return b, nil
}

type RPCServer struct{ Manager *Manager }

func (s *RPCServer) RunContainer(ctx context.Context, r *rpc.RunContainerRequest) (*rpc.RunContainerResponse, error) {
	return s.Manager.Run(ctx, r)
}
func (s *RPCServer) StopContainer(ctx context.Context, r *rpc.RunContainerRequest) (*rpc.Empty, error) {
	return &rpc.Empty{}, s.Manager.Stop(ctx, r.ID)
}
func (s *RPCServer) ListContainers(context.Context, *rpc.Empty) (*rpc.ContainerList, error) {
	out := &rpc.ContainerList{}
	for _, r := range s.Manager.List() {
		out.Items = append(out.Items, rpc.ContainerInfo{ID: r.ID, Status: r.Status, PID: int32(r.PID), Image: r.Image, Workload: r.Workload, Replica: r.Replica, ExitCode: int32(r.ExitCode)})
	}
	return out, nil
}
func (s *RPCServer) GetLogs(ctx context.Context, r *rpc.RunContainerRequest) (*rpc.LogReply, error) {
	b, e := s.Manager.Logs(r.ID)
	return &rpc.LogReply{Data: b}, e
}
func ensure(root string) {
	for _, d := range []string{"bin", "etc", "dev", "proc", "sys", "tmp", "usr/bin", "usr/lib", "lib", "lib64", "var", "root", "home"} {
		_ = os.MkdirAll(filepath.Join(root, d), 0o755)
	}
	_ = os.Chmod(filepath.Join(root, "tmp"), 0o1777)
}
func merge(base, over []string) []string {
	m := map[string]string{"PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME": "/root", "TERM": "xterm"}
	for _, list := range [][]string{base, over} {
		for _, e := range list {
			p := strings.SplitN(e, "=", 2)
			if len(p) == 2 {
				m[p[0]] = p[1]
			}
		}
	}
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}
