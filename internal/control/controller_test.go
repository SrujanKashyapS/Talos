package control

import (
	"testing"

	"github.com/SrujanKashyapS/Talos/internal/raftstore"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"github.com/SrujanKashyapS/Talos/internal/scheduler"
)

func TestControllerHasScheduler(t *testing.T) { var _ scheduler.Scheduler = scheduler.LeastLoaded{} }

func TestNeedsPlacementAfterNodeFailure(t *testing.T) {
	state := raftstore.State{Nodes: map[string]rpc.NodeInfo{
		"live": {ID: "live", Alive: true},
		"dead": {ID: "dead", Alive: false},
	}}
	if needsPlacement(state, assignment{NodeID: "live", ContainerID: "c1"}) {
		t.Fatal("healthy assignment should not be replaced")
	}
	if !needsPlacement(state, assignment{NodeID: "dead", ContainerID: "c1"}) {
		t.Fatal("assignment on dead node must be replaced")
	}
	if !needsPlacement(state, assignment{NodeID: "missing", ContainerID: "c1"}) {
		t.Fatal("assignment on missing node must be replaced")
	}
}

func TestContainerIDIsDeterministicAndReplicaScoped(t *testing.T) {
	first := containerID("web", 0)
	if first != containerID("web", 0) {
		t.Fatal("container ID changed for identical workload replica")
	}
	if first == containerID("web", 1) || first == containerID("api", 0) {
		t.Fatal("container ID does not distinguish workload replicas")
	}
}
