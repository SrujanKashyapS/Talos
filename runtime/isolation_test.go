package runtime

import (
	"os"
	"testing"
)

func TestCGroupNoLimits(t *testing.T) {
	g, e := CreateCGroup("none", Resources{})
	if e != nil {
		t.Fatal(e)
	}
	if g != nil {
		_ = g.Remove()
		t.Fatal("expected nil")
	}
}
func TestCGroupRequiresRootForLimits(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip()
	}
	g, e := CreateCGroup("test", Resources{CPU: "10000 100000", Memory: 1 << 20})
	if e == nil && g != nil {
		_ = g.Remove()
		t.Fatal("expected unavailable cgroup")
	}
}

func TestRootPathRejectsTraversal(t *testing.T) {
	for _, path := range []string{"../host", "nested/../../host", "/../../host"} {
		if _, err := rootPath(t.TempDir(), path); err == nil {
			t.Errorf("rootPath accepted traversal path %q", path)
		}
	}
	root := t.TempDir()
	got, err := rootPath(root, "/work")
	if err != nil || got != root+"/work" {
		t.Fatalf("rootPath() = %q, %v; want %q, nil", got, err, root+"/work")
	}
}
