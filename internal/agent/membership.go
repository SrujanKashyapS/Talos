package agent

import (
	"context"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"google.golang.org/grpc"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func MemoryTotal() int64 {
	b, e := os.ReadFile("/proc/meminfo")
	if e != nil {
		return 0
	}
	for _, l := range strings.Split(string(b), "\n") {
		f := strings.Fields(l)
		if len(f) >= 2 && f[0] == "MemTotal:" {
			n, _ := strconv.ParseInt(f[1], 10, 64)
			return n * 1024
		}
	}
	return 0
}
func MemoryUsed() int64 {
	t := MemoryTotal()
	if t == 0 {
		return 0
	}
	b, e := os.ReadFile("/proc/meminfo")
	if e != nil {
		return 0
	}
	var a int64
	for _, l := range strings.Split(string(b), "\n") {
		f := strings.Fields(l)
		if len(f) >= 2 && f[0] == "MemAvailable:" {
			a, _ = strconv.ParseInt(f[1], 10, 64)
			a *= 1024
			break
		}
	}
	return t - a
}
func (m *Manager) NodeInfo(id, addr string) rpc.NodeInfo {
	cpu, memory, containers := m.AllocatedResources()
	return rpc.NodeInfo{ID: id, Address: addr, CPUCapacity: float64(runtime.NumCPU()), CPUUsed: cpu, MemoryCapacity: MemoryTotal(), MemoryUsed: memory, Containers: containers, HeldLayers: m.HeldLayers()}
}
func RunMembership(ctx context.Context, cc *grpc.ClientConn, id, addr string, interval time.Duration, m *Manager) error {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	c := rpc.NewMembershipClient(cc)
	n := m.NodeInfo(id, addr)
	if _, e := c.RegisterNode(ctx, &rpc.NodeRequest{Node: n}); e != nil {
		slog.Error("register node", "error", e)
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			n = m.NodeInfo(id, addr)
			if _, e := c.Heartbeat(ctx, &rpc.NodeRequest{Node: n}); e != nil {
				slog.Error("heartbeat", "error", e)
			}
		}
	}
}
