package scheduler

import (
	"fmt"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"sort"
)

type Request struct {
	CPU    float64
	Memory int64
	Layers []string
}
type NodeView struct{ Node rpc.NodeInfo }
type Scheduler interface {
	Pick([]NodeView, Request) (string, error)
}
type LeastLoaded struct{}

func (LeastLoaded) Pick(nodes []NodeView, r Request) (string, error) {
	var a []rpc.NodeInfo
	for _, v := range nodes {
		n := v.Node
		if !n.Alive {
			continue
		}
		if r.CPU > 0 && n.CPUCapacity-n.CPUUsed < r.CPU {
			continue
		}
		if r.Memory > 0 && n.MemoryCapacity-n.MemoryUsed < r.Memory {
			continue
		}
		a = append(a, n)
	}
	if len(a) == 0 {
		return "", fmt.Errorf("no node satisfies resource request")
	}
	sort.Slice(a, func(i, j int) bool {
		si, sj := score(a[i], r), score(a[j], r)
		if si == sj {
			return a[i].ID < a[j].ID
		}
		return si < sj
	})
	return a[0].ID, nil
}
func score(n rpc.NodeInfo, r Request) float64 {
	cpu, mem := 0.0, 0.0
	if n.CPUCapacity > 0 {
		cpu = n.CPUUsed / n.CPUCapacity
	}
	if n.MemoryCapacity > 0 {
		mem = float64(n.MemoryUsed) / float64(n.MemoryCapacity)
	}
	s := cpu + mem + float64(n.Containers)*0.05
	held := map[string]bool{}
	for _, d := range n.HeldLayers {
		held[d] = true
	}
	for _, d := range r.Layers {
		if held[d] {
			s -= 0.2
		}
	}
	return s
}
