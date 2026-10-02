package utils_test

import (
	"archive/tar"
	"bytes"
	"github.com/SrujanKashyapS/Talos/utils"
	"testing"
)

func makeTar(t *testing.T, n string, typ byte, link string) []byte {
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	if e := w.WriteHeader(&tar.Header{Name: n, Typeflag: typ, Linkname: link, Mode: 0o644, Size: 1}); e != nil {
		t.Fatal(e)
	}
	if typ == tar.TypeReg {
		_, _ = w.Write([]byte("x"))
	}
	if e := w.Close(); e != nil {
		t.Fatal(e)
	}
	return b.Bytes()
}
func TestTarTraversal(t *testing.T) {
	for _, n := range []string{"../x", "/tmp/x", "../../x"} {
		if e := utils.ExtractTarReader(bytes.NewReader(makeTar(t, n, tar.TypeReg, "")), t.TempDir()); e == nil {
			t.Fatal(n)
		}
	}
}
func TestTarSymlinkTraversal(t *testing.T) {
	if e := utils.ExtractTarReader(bytes.NewReader(makeTar(t, "x", tar.TypeSymlink, "../../outside")), t.TempDir()); e == nil {
		t.Fatal("accepted symlink traversal")
	}
}
