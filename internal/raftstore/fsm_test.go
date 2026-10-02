package raftstore

import (
	"bytes"
	"encoding/json"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"github.com/hashicorp/raft"
	"io"
	"testing"
)

type sink struct {
	bytes.Buffer
	closed bool
}

func (s *sink) ID() string    { return "test" }
func (s *sink) Cancel() error { return nil }
func (s *sink) Close() error  { s.closed = true; return nil }
func apply(f *FSM, c Command) error {
	b, e := json.Marshal(c)
	if e != nil {
		return e
	}
	v := f.Apply(&raft.Log{Data: b})
	if e, ok := v.(error); ok {
		return e
	}
	return nil
}
func TestFSMDeterministicApply(t *testing.T) {
	a, b := NewFSM(), NewFSM()
	v, _ := json.Marshal(rpc.NodeInfo{ID: "n", Address: "a", Alive: true})
	c := Command{Op: OpNode, Key: "n", Value: v}
	if e := apply(a, c); e != nil {
		t.Fatal(e)
	}
	if e := apply(b, c); e != nil {
		t.Fatal(e)
	}
	x, _ := json.Marshal(a.State())
	y, _ := json.Marshal(b.State())
	if !bytes.Equal(x, y) {
		t.Fatal("states differ")
	}
}
func TestFSMSnapshotRestore(t *testing.T) {
	f := NewFSM()
	v, _ := json.Marshal("layer")
	if e := apply(f, Command{Op: OpCache, Key: "k", Value: v}); e != nil {
		t.Fatal(e)
	}
	sn, _ := f.Snapshot()
	s := &sink{}
	if e := sn.Persist(s); e != nil {
		t.Fatal(e)
	}
	if !s.closed {
		t.Fatal("snapshot not closed")
	}
	g := NewFSM()
	if e := g.Restore(io.NopCloser(bytes.NewReader(s.Bytes()))); e != nil {
		t.Fatal(e)
	}
	if g.State().Cache["k"] != "layer" {
		t.Fatal("restore lost cache")
	}
}

func TestFSMSnapshotRestorePreservesWorkloadsAndAssignments(t *testing.T) {
	f := NewFSM()
	workload := json.RawMessage(`{"name":"web","replicas":1}`)
	assignment, err := json.Marshal(`{"NodeID":"node1","ContainerID":"container1"}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := apply(f, Command{Op: OpWorkload, Key: "web", Value: workload}); err != nil {
		t.Fatal(err)
	}
	if err := apply(f, Command{Op: OpAssignment, Key: "web/0", Value: assignment}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	s := &sink{}
	if err := snapshot.Persist(s); err != nil {
		t.Fatal(err)
	}
	restored := NewFSM()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(s.Bytes()))); err != nil {
		t.Fatal(err)
	}
	state := restored.State()
	if string(state.Workloads["web"]) != string(workload) || state.Assignments["web/0"] == "" {
		t.Fatalf("restored state = %#v, want workload and assignment", state)
	}
}

func TestFSMDeleteAssignment(t *testing.T) {
	f := NewFSM()
	v, err := json.Marshal("container1")
	if err != nil {
		t.Fatal(err)
	}
	if err := apply(f, Command{Op: OpAssignment, Key: "web/0", Value: v}); err != nil {
		t.Fatal(err)
	}
	if err := apply(f, Command{Op: OpDeleteAssignment, Key: "web/0"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.State().Assignments["web/0"]; ok {
		t.Fatal("assignment was not deleted")
	}
}

func TestFSMSnapshotAfterAssignmentDeletion(t *testing.T) {
	f := NewFSM()
	value, err := json.Marshal("container1")
	if err != nil {
		t.Fatal(err)
	}
	if err := apply(f, Command{Op: OpAssignment, Key: "web/0", Value: value}); err != nil {
		t.Fatal(err)
	}
	if err := apply(f, Command{Op: OpDeleteAssignment, Key: "web/0"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	s := &sink{}
	if err := snapshot.Persist(s); err != nil {
		t.Fatal(err)
	}
	restored := NewFSM()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(s.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if _, ok := restored.State().Assignments["web/0"]; ok {
		t.Fatal("deleted assignment reappeared after snapshot restore")
	}
}

func TestFSMDeleteWorkloadDoesNotDeleteSimilarNameAssignments(t *testing.T) {
	f := NewFSM()
	value, err := json.Marshal("container1")
	if err != nil {
		t.Fatal(err)
	}
	if err := apply(f, Command{Op: OpAssignment, Key: "web-api/0", Value: value}); err != nil {
		t.Fatal(err)
	}
	if err := apply(f, Command{Op: OpDeleteWorkload, Key: "web"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.State().Assignments["web-api/0"]; !ok {
		t.Fatal("deleting web removed web-api assignment")
	}
}
