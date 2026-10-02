package agent

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/SrujanKashyapS/Talos/internal/rpc"
)

func TestAllocatedResourcesCountsOnlyRunningContainers(t *testing.T) {
	m := &Manager{items: map[string]*record{
		"running": {Status: "running", CPU: 0.5, Memory: 256},
		"stopped": {Status: "stopped", CPU: 4, Memory: 4096},
	}}
	cpu, memory, containers := m.AllocatedResources()
	if cpu != 0.5 || memory != 256 || containers != 1 {
		t.Fatalf("AllocatedResources() = (%v, %d, %d), want (0.5, 256, 1)", cpu, memory, containers)
	}
}

func TestCPULimit(t *testing.T) {
	for input, want := range map[string]float64{"50000 100000": 0.5, "100000 100000": 1, "": 0, "max 100000": 0, "bad value": 0} {
		if got := cpuLimit(input); got != want {
			t.Errorf("cpuLimit(%q) = %v, want %v", input, got, want)
		}
	}
}

func TestCleanupStartedProcessWaitsBeforeReturning(t *testing.T) {
	command := exec.Command("sh", "-c", "sleep 30")
	logFile, err := os.CreateTemp(t.TempDir(), "container.log")
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	if err := cleanupStartedProcess(command, logFile, nil); err != nil {
		t.Fatal(err)
	}
	if command.ProcessState == nil {
		t.Fatal("process was not reaped during cleanup")
	}
}

func TestRunningResponseDeduplicatesOnlyRunningContainer(t *testing.T) {
	m := &Manager{items: map[string]*record{
		"request": {ID: "request", Status: "running", PID: 42},
		"stopped": {ID: "stopped", Status: "stopped", PID: 43},
	}}
	response, ok := m.runningResponse("request")
	if !ok || response.ID != "request" || response.PID != 42 {
		t.Fatalf("runningResponse() = %#v, %t; want running container", response, ok)
	}
	if _, ok := m.runningResponse("stopped"); ok {
		t.Fatal("stopped container must not deduplicate a new request")
	}
	response, err := m.Run(context.Background(), &rpc.RunContainerRequest{ID: "request"})
	if err != nil || response.ID != "request" {
		t.Fatalf("Run() retry = %#v, %v; want existing running container", response, err)
	}
	response, err = m.RunIsolated(context.Background(), &rpc.RunContainerRequest{ID: "request"})
	if err != nil || response.ID != "request" {
		t.Fatalf("RunIsolated() retry = %#v, %v; want existing running container", response, err)
	}
	for _, id := range []string{"", ".", "../escape", "nested/id"} {
		if validID(id) {
			t.Errorf("validID(%q) = true, want false", id)
		}
	}
}

func TestTerminateForcesProcessAfterGracePeriod(t *testing.T) {
	command := exec.Command("sh", "-c", "trap '' TERM; while :; do sleep 1; done")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	if err := terminate(command.Process.Pid, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("forced termination unexpectedly reported a clean exit")
	}
}

func TestPrepareIDAllowsRecreationAfterExit(t *testing.T) {
	root := t.TempDir()
	containerDir := filepath.Join(root, "containers", "request")
	if err := os.MkdirAll(containerDir, 0o755); err != nil {
		t.Fatal(err)
	}
	m := &Manager{path: filepath.Join(root, "containers.json"), items: map[string]*record{
		"request": {ID: "request", Status: "exited"},
	}}
	if err := m.prepareID("request"); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.items["request"]; ok {
		t.Fatal("exited container record was not cleared")
	}
	if _, err := os.Stat(containerDir); !os.IsNotExist(err) {
		t.Fatalf("previous container directory still exists: %v", err)
	}
	m.items["running"] = &record{ID: "running", Status: "running"}
	if err := m.prepareID("running"); err == nil {
		t.Fatal("prepareID allowed replacement of running container")
	}
}

func TestConcurrentSameIDRetriesReturnOneRunningRecord(t *testing.T) {
	m := &Manager{items: map[string]*record{
		"request": {ID: "request", Status: "running", PID: 42},
	}}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(isolated bool) {
			defer wg.Done()
			var (
				response *rpc.RunContainerResponse
				err      error
			)
			if isolated {
				response, err = m.RunIsolated(context.Background(), &rpc.RunContainerRequest{ID: "request"})
			} else {
				response, err = m.Run(context.Background(), &rpc.RunContainerRequest{ID: "request"})
			}
			if err != nil || response.ID != "request" || response.PID != 42 {
				errs <- fmt.Errorf("retry = %#v, %v", response, err)
			}
		}(i%2 == 0)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
