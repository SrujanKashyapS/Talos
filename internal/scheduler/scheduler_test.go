package scheduler

import (
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"testing"
)

func TestLeastLoaded(t *testing.T) {
	n, e := LeastLoaded{}.Pick([]NodeView{{Node: rpc.NodeInfo{ID: "a", Alive: true, CPUCapacity: 4, CPUUsed: 2, MemoryCapacity: 8, MemoryUsed: 2, Containers: 3}}, {Node: rpc.NodeInfo{ID: "b", Alive: true, CPUCapacity: 4, CPUUsed: 1, MemoryCapacity: 8, MemoryUsed: 2, Containers: 1}}}, Request{})
	if e != nil || n != "b" {
		t.Fatalf("%q %v", n, e)
	}
}
func TestAffinity(t *testing.T) {
	n, e := LeastLoaded{}.Pick([]NodeView{{Node: rpc.NodeInfo{ID: "a", Alive: true, CPUCapacity: 4, CPUUsed: 1, MemoryCapacity: 8, MemoryUsed: 2, HeldLayers: []string{"x"}}}, {Node: rpc.NodeInfo{ID: "b", Alive: true, CPUCapacity: 4, CPUUsed: 1, MemoryCapacity: 8, MemoryUsed: 2}}}, Request{Layers: []string{"x"}})
	if e != nil || n != "a" {
		t.Fatalf("%q %v", n, e)
	}
}
func TestSkipDead(t *testing.T) {
	n, e := LeastLoaded{}.Pick([]NodeView{{Node: rpc.NodeInfo{ID: "a", Alive: false}}, {Node: rpc.NodeInfo{ID: "b", Alive: true, CPUCapacity: 1}}}, Request{})
	if e != nil || n != "b" {
		t.Fatalf("%q %v", n, e)
	}
}

func TestCapacityConstraints(t *testing.T) {
	nodes := []NodeView{
		{Node: rpc.NodeInfo{ID: "cpu-full", Alive: true, CPUCapacity: 2, CPUUsed: 1.8, MemoryCapacity: 8 << 30}},
		{Node: rpc.NodeInfo{ID: "memory-full", Alive: true, CPUCapacity: 4, MemoryCapacity: 1024, MemoryUsed: 900}},
		{Node: rpc.NodeInfo{ID: "fits", Alive: true, CPUCapacity: 2, CPUUsed: 1, MemoryCapacity: 2048, MemoryUsed: 512}},
	}
	node, err := LeastLoaded{}.Pick(nodes, Request{CPU: 0.5, Memory: 1024})
	if err != nil || node != "fits" {
		t.Fatalf("Pick() = %q, %v; want fits, nil", node, err)
	}
	if _, err := (LeastLoaded{}).Pick(nodes, Request{CPU: 5}); err == nil {
		t.Fatal("Pick() succeeded despite no node having enough CPU")
	}
}
