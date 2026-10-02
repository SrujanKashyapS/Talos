package runtime

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

type Resources struct {
	CPU    string
	Memory int64
}
type IsolationOptions struct {
	Rootfs  string
	Workdir string
	Command []string
	Env     []string
	Chroot  bool
}

func Start(ctx context.Context, o IsolationOptions) (*exec.Cmd, *os.File, error) {
	if len(o.Command) == 0 {
		return nil, nil, fmt.Errorf("empty command")
	}
	w := o.Workdir
	if w == "" {
		w = "/"
	}
	d, err := rootPath(o.Rootfs, w)
	if err != nil {
		return nil, nil, err
	}
	if e := os.MkdirAll(d, 0o755); e != nil {
		return nil, nil, e
	}
	b := o.Command[0]
	if filepath.IsAbs(b) {
		if !o.Chroot {
			b, err = rootPath(o.Rootfs, b)
			if err != nil {
				return nil, nil, err
			}
		}
	} else {
		if strings.Contains(b, string(filepath.Separator)) {
			return nil, nil, fmt.Errorf("relative command must not contain a path: %q", b)
		}
		b = filepath.Join(d, b)
	}
	c := exec.CommandContext(ctx, b, o.Command[1:]...)
	c.Dir = d
	c.Env = o.Env
	c.SysProcAttr = &syscall.SysProcAttr{}
	if o.Chroot {
		c.SysProcAttr.Chroot = o.Rootfs
		c.SysProcAttr.Cloneflags = syscall.CLONE_NEWPID | syscall.CLONE_NEWNS | syscall.CLONE_NEWUTS | syscall.CLONE_NEWNET
		c.Dir = w
	}
	f, e := os.OpenFile(filepath.Join(o.Rootfs, "..", "console.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if e != nil {
		return nil, nil, e
	}
	c.Stdout = f
	c.Stderr = f
	if e = c.Start(); e != nil {
		f.Close()
		return nil, nil, fmt.Errorf("start isolated process: %w", e)
	}
	return c, f, nil
}

func rootPath(root, path string) (string, error) {
	if root == "" {
		return "", fmt.Errorf("empty rootfs")
	}
	clean := filepath.Clean(strings.TrimPrefix(path, "/"))
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes rootfs: %q", path)
	}
	return filepath.Join(root, clean), nil
}
