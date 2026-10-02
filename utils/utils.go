package utils

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func DocksmithRoot() (string, error) {
	h, e := os.UserHomeDir()
	if e != nil {
		return "", fmt.Errorf("cannot determine home directory: %w", e)
	}
	return filepath.Join(h, ".talos"), nil
}
func EnsureDirs() error {
	r, e := DocksmithRoot()
	if e != nil {
		return e
	}
	for _, d := range []string{"images", "layers", "cache", "containers", "raft", "nodes"} {
		if e = os.MkdirAll(filepath.Join(r, d), 0o755); e != nil {
			return fmt.Errorf("creating %s dir: %w", d, e)
		}
	}
	return nil
}
func ImagesDir() (string, error) {
	r, e := DocksmithRoot()
	if e != nil {
		return "", e
	}
	return filepath.Join(r, "images"), nil
}
func LayersDir() (string, error) {
	r, e := DocksmithRoot()
	if e != nil {
		return "", e
	}
	return filepath.Join(r, "layers"), nil
}
func CacheDir() (string, error) {
	r, e := DocksmithRoot()
	if e != nil {
		return "", e
	}
	return filepath.Join(r, "cache"), nil
}
func SHA256File(p string) (string, error) {
	f, e := os.Open(p)
	if e != nil {
		return "", e
	}
	defer f.Close()
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return "", e
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func SHA256Bytes(b []byte) string  { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func SHA256String(s string) string { return SHA256Bytes([]byte(s)) }
func LayerPath(d string) (string, error) {
	r, e := LayersDir()
	if e != nil {
		return "", e
	}
	return filepath.Join(r, d+".tar"), nil
}

type FileEntry struct {
	RelPath string
	Hash    string
	Mode    os.FileMode
	IsDir   bool
	Symlink string
}

func ScanDir(root string) (map[string]FileEntry, error) {
	o := map[string]FileEntry{}
	e := filepath.Walk(root, func(p string, fi os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		rel, x := filepath.Rel(root, p)
		if x != nil {
			return x
		}
		if rel == "." {
			return nil
		}
		v := FileEntry{RelPath: rel, Mode: fi.Mode(), IsDir: fi.IsDir()}
		if fi.Mode()&os.ModeSymlink != 0 {
			v.Symlink, _ = os.Readlink(p)
			v.Hash = SHA256String(v.Symlink)
		} else if !fi.IsDir() {
			v.Hash, e = SHA256File(p)
			if e != nil {
				return e
			}
		}
		o[rel] = v
		return nil
	})
	return o, e
}
func CreateTarFromPaths(root string, paths []string, dest string) ([]byte, error) {
	sort.Strings(paths)
	pr, pw := io.Pipe()
	tw := tar.NewWriter(pw)
	ch := make(chan error, 1)
	go func() {
		defer pw.Close()
		for _, rel := range paths {
			src := filepath.Join(root, rel)
			fi, e := os.Lstat(src)
			if e != nil {
				ch <- e
				return
			}
			name := filepath.ToSlash(strings.TrimPrefix(rel, "/"))
			if dest != "" {
				name = filepath.ToSlash(strings.TrimPrefix(filepath.Join(dest, rel), "/"))
			}
			var h *tar.Header
			if fi.Mode()&os.ModeSymlink != 0 {
				link, e := os.Readlink(src)
				if e != nil {
					ch <- e
					return
				}
				h = &tar.Header{Typeflag: tar.TypeSymlink, Name: name, Linkname: link, Mode: 0o777}
			} else {
				h, e = tar.FileInfoHeader(fi, "")
				if e != nil {
					ch <- e
					return
				}
				h.Name = name
			}
			h.ModTime = h.ModTime.Add(-h.ModTime.Sub(h.ModTime))
			h.AccessTime = h.ModTime
			h.ChangeTime = h.ModTime
			h.ModTime = h.ModTime.Truncate(0)
			h.AccessTime = h.ModTime
			h.ChangeTime = h.ModTime
			h.Uid = 0
			h.Gid = 0
			h.Uname = ""
			h.Gname = ""
			if fi.IsDir() && !strings.HasSuffix(h.Name, "/") {
				h.Name += "/"
			}
			if e = tw.WriteHeader(h); e != nil {
				ch <- e
				return
			}
			if fi.Mode().IsRegular() {
				f, e := os.Open(src)
				if e != nil {
					ch <- e
					return
				}
				_, e = io.Copy(tw, f)
				f.Close()
				if e != nil {
					ch <- e
					return
				}
			}
		}
		ch <- tw.Close()
	}()
	b, e := io.ReadAll(pr)
	we := <-ch
	if we != nil {
		return nil, we
	}
	return b, e
}
func CreateTarFromDir(root string) ([]byte, error) {
	var p []string
	e := filepath.Walk(root, func(x string, fi os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		r, e := filepath.Rel(root, x)
		if e != nil {
			return e
		}
		if r != "." {
			p = append(p, r)
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	return CreateTarFromPaths(root, p, "")
}
func ExtractTar(p, d string) error {
	f, e := os.Open(p)
	if e != nil {
		return fmt.Errorf("opening tar %s: %w", p, e)
	}
	defer f.Close()
	return ExtractTarReader(f, d)
}
func cleanTarPath(n string) (string, error) {
	if n == "" || filepath.IsAbs(filepath.FromSlash(n)) {
		return "", fmt.Errorf("unsafe tar path %q", n)
	}
	c := filepath.Clean(filepath.FromSlash(n))
	if c == "." || c == ".." || strings.HasPrefix(c, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("tar entry %q escapes destination", n)
	}
	return c, nil
}
func safeTarget(base, name string) bool {
	if name == "" || filepath.IsAbs(filepath.FromSlash(name)) {
		return false
	}
	p := filepath.Clean(filepath.Join(base, filepath.FromSlash(name)))
	r := filepath.Clean(base) + string(os.PathSeparator)
	return p == filepath.Clean(base) || strings.HasPrefix(p, r)
}
func ExtractTarReader(r io.Reader, d string) error {
	if e := os.MkdirAll(d, 0o755); e != nil {
		return e
	}
	tr := tar.NewReader(r)
	for {
		h, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return fmt.Errorf("reading tar: %w", e)
		}
		n, e := cleanTarPath(h.Name)
		if e != nil {
			return e
		}
		target := filepath.Join(d, n)
		switch h.Typeflag {
		case tar.TypeDir:
			if e := os.MkdirAll(target, h.FileInfo().Mode().Perm()); e != nil {
				return e
			}
		case tar.TypeReg, tar.TypeRegA:
			if e := os.MkdirAll(filepath.Dir(target), 0o755); e != nil {
				return e
			}
			f, e := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, h.FileInfo().Mode().Perm())
			if e != nil {
				return e
			}
			if _, e = io.Copy(f, tr); e != nil {
				f.Close()
				return e
			}
			if e = f.Close(); e != nil {
				return e
			}
		case tar.TypeSymlink:
			if !safeTarget(filepath.Dir(target), h.Linkname) {
				return fmt.Errorf("unsafe symlink %q -> %q", h.Name, h.Linkname)
			}
			if e := os.MkdirAll(filepath.Dir(target), 0o755); e != nil {
				return e
			}
			if e = os.Remove(target); e != nil && !os.IsNotExist(e) {
				return e
			}
			if e = os.Symlink(h.Linkname, target); e != nil {
				return e
			}
		case tar.TypeLink:
			ln, e := cleanTarPath(h.Linkname)
			if e != nil {
				return e
			}
			if !safeTarget(d, ln) {
				return fmt.Errorf("unsafe hardlink %q -> %q", h.Name, h.Linkname)
			}
			if e := os.MkdirAll(filepath.Dir(target), 0o755); e != nil {
				return e
			}
			if e = os.Remove(target); e != nil && !os.IsNotExist(e) {
				return e
			}
			if e = os.Link(filepath.Join(d, ln), target); e != nil {
				return e
			}
		default:
			return fmt.Errorf("unsupported tar entry %d for %q", h.Typeflag, h.Name)
		}
	}
	return nil
}
func HashFiles(paths []string) (string, error) {
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		x, e := SHA256File(p)
		if e != nil {
			return "", fmt.Errorf("hashing %s: %w", p, e)
		}
		fmt.Fprintf(h, "%s:%s\n", p, x)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func GlobFiles(base, pat string) ([]string, error) {
	if filepath.IsAbs(pat) {
		return filepath.Glob(pat)
	}
	if strings.Contains(pat, "**") {
		return doubleStarGlob(base, pat)
	}
	return filepath.Glob(filepath.Join(base, pat))
}
func doubleStarGlob(base, pat string) ([]string, error) {
	p := strings.SplitN(pat, "**", 2)
	root := filepath.Join(base, strings.TrimSuffix(p[0], "/"))
	suffix := strings.TrimPrefix(p[1], "/")
	var o []string
	e := filepath.Walk(root, func(x string, fi os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		if fi.IsDir() {
			return nil
		}
		if suffix == "" {
			o = append(o, x)
			return nil
		}
		ok, e := filepath.Match(suffix, filepath.Base(x))
		if e != nil {
			return e
		}
		if ok {
			o = append(o, x)
		}
		return nil
	})
	sort.Strings(o)
	return o, e
}
func SplitImageRef(r string) (string, string) {
	slash := strings.LastIndex(r, "/")
	colon := strings.LastIndex(r, ":")
	if colon > slash {
		return r[:colon], r[colon+1:]
	}
	return r, "latest"
}
func ImageKey(n, t string) string { return strings.ReplaceAll(n, "/", "_") + "_" + t }
