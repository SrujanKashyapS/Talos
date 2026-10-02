package runtime

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type CGroup struct{ Path string }

func CreateCGroup(id string, r Resources) (*CGroup, error) {
	if r.CPU == "" && r.Memory <= 0 {
		return nil, nil
	}
	if id == "" || filepath.Base(id) != id || id == "." {
		return nil, fmt.Errorf("invalid cgroup id %q", id)
	}
	p := filepath.Join("/sys/fs/cgroup/talos", id)
	if e := os.MkdirAll(p, 0o755); e != nil {
		return nil, fmt.Errorf("create cgroup: %w", e)
	}
	g := &CGroup{Path: p}
	if r.CPU != "" {
		v := r.CPU
		if !strings.Contains(v, " ") {
			v = v + " 100000"
		}
		if e := os.WriteFile(filepath.Join(p, "cpu.max"), []byte(v), 0o644); e != nil {
			if removeErr := g.Remove(); removeErr != nil {
				return nil, fmt.Errorf("cpu.max: %w (cleanup: %v)", e, removeErr)
			}
			return nil, fmt.Errorf("cpu.max: %w", e)
		}
	}
	if r.Memory > 0 {
		if e := os.WriteFile(filepath.Join(p, "memory.max"), []byte(strconv.FormatInt(r.Memory, 10)), 0o644); e != nil {
			if removeErr := g.Remove(); removeErr != nil {
				return nil, fmt.Errorf("memory.max: %w (cleanup: %v)", e, removeErr)
			}
			return nil, fmt.Errorf("memory.max: %w", e)
		}
	}
	return g, nil
}
func (g *CGroup) Attach(pid int) error {
	if g == nil {
		return nil
	}
	return os.WriteFile(filepath.Join(g.Path, "cgroup.procs"), []byte(strconv.Itoa(pid)), 0o644)
}
func (g *CGroup) Remove() error {
	if g == nil {
		return nil
	}
	if e := os.Remove(g.Path); e != nil && !os.IsNotExist(e) {
		return fmt.Errorf("remove cgroup: %w", e)
	}
	return nil
}
