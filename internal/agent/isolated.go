package agent

import (
	"context"
	"errors"
	"fmt"
	"github.com/SrujanKashyapS/Talos/image"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	rt "github.com/SrujanKashyapS/Talos/runtime"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func (m *Manager) RunIsolated(ctx context.Context, r *rpc.RunContainerRequest) (*rpc.RunContainerResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := r.ID
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
	man, e := image.Load(r.ImageName, r.ImageTag)
	if e != nil {
		return nil, e
	}
	root := filepath.Join(filepath.Dir(m.path), "containers", id, "rootfs")
	if e = os.MkdirAll(root, 0o755); e != nil {
		return nil, e
	}
	if e = image.ExtractManifestLayers(man, root); e != nil {
		return nil, fmt.Errorf("extract image: %w", e)
	}
	ensure(root)
	cmd := man.Config.Cmd
	if len(r.Command) > 0 {
		cmd = r.Command
	}
	if len(cmd) == 0 {
		return nil, fmt.Errorf("no command")
	}
	work := man.Config.WorkingDir
	if work == "" {
		work = "/"
	}
	// The request ID is stable for retries, but a cgroup must belong to one
	// process generation so delayed cleanup cannot remove a replacement's group.
	cgroupID := id + "-" + newID()
	cg, e := rt.CreateCGroup(cgroupID, rt.Resources{CPU: r.Resources.CPU, Memory: r.Resources.Memory})
	if e != nil {
		return nil, e
	}
	c, lf, e := rt.Start(ctx, rt.IsolationOptions{Rootfs: root, Workdir: work, Command: cmd, Env: merge(man.Config.Env, r.Env), Chroot: r.Chroot})
	if e != nil {
		if cg != nil {
			if removeErr := cg.Remove(); removeErr != nil {
				return nil, errors.Join(e, fmt.Errorf("cleanup cgroup: %w", removeErr))
			}
		}
		return nil, e
	}
	if cg != nil {
		if e = cg.Attach(c.Process.Pid); e != nil {
			cleanupErr := cleanupStartedProcess(c, lf, cg)
			if cleanupErr != nil {
				return nil, errors.Join(fmt.Errorf("attach cgroup: %w", e), cleanupErr)
			}
			return nil, fmt.Errorf("attach cgroup: %w", e)
		}
	}
	logPath := filepath.Join(filepath.Dir(m.path), "containers", id, "console.log")
	m.items[id] = &record{ID: id, Status: "running", PID: c.Process.Pid, Image: r.ImageName + ":" + r.ImageTag, Workload: r.Workload, Replica: r.Replica, Started: time.Now().UTC(), Rootfs: root, Log: logPath, CPU: cpuLimit(r.Resources.CPU), Memory: r.Resources.Memory}
	if e = m.save(); e != nil {
		cleanupErr := cleanupStartedProcess(c, lf, cg)
		if cleanupErr != nil {
			return nil, errors.Join(e, cleanupErr)
		}
		return nil, e
	}
	go func() {
		e := c.Wait()
		if closeErr := lf.Close(); closeErr != nil {
			fmt.Printf("close container log %s: %v\n", id, closeErr)
		}
		if cg != nil {
			if removeErr := cg.Remove(); removeErr != nil {
				fmt.Printf("remove cgroup for container %s: %v\n", id, removeErr)
			}
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		x, ok := m.items[id]
		if !ok || x.PID != c.Process.Pid {
			return
		}
		x.Finished = time.Now().UTC()
		x.Status = "exited"
		if e != nil {
			if z, ok := e.(*exec.ExitError); ok {
				x.ExitCode = z.ExitCode()
			} else {
				x.ExitCode = -1
			}
		}
		_ = m.save()
	}()
	return &rpc.RunContainerResponse{ID: id, Status: "running", PID: int32(c.Process.Pid)}, nil
}

func cleanupStartedProcess(c *exec.Cmd, logFile *os.File, cg *rt.CGroup) error {
	var cleanupErr error
	if err := c.Process.Kill(); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("kill container: %w", err))
	}
	if err := c.Wait(); err != nil {
		if _, exited := err.(*exec.ExitError); !exited {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("wait for container: %w", err))
		}
	}
	if err := logFile.Close(); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("close container log: %w", err))
	}
	if cg != nil {
		if err := cg.Remove(); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove cgroup: %w", err))
		}
	}
	return cleanupErr
}

func cpuLimit(value string) float64 {
	fields := strings.Fields(value)
	if len(fields) != 2 || fields[0] == "max" {
		return 0
	}
	quota, quotaErr := strconv.ParseFloat(fields[0], 64)
	period, periodErr := strconv.ParseFloat(fields[1], 64)
	if quotaErr != nil || periodErr != nil || quota < 0 || period <= 0 {
		return 0
	}
	return quota / period
}

type IsolatedRPCServer struct {
	*RPCServer
	Chroot bool
}

func (s *IsolatedRPCServer) RunContainer(ctx context.Context, r *rpc.RunContainerRequest) (*rpc.RunContainerResponse, error) {
	if !r.Chroot {
		r.Chroot = s.Chroot
	}
	return s.Manager.RunIsolated(ctx, r)
}
