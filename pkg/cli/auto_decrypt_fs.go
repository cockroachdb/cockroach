// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/tool"
	"github.com/cockroachdb/pebble/vfs"
)

// autoDecryptFS is a filesystem which automatically detects paths that are
// registered as encrypted stores and uses the appropriate encryptedFS.
type autoDecryptFS struct {
	encryptedDirs map[string]*encryptedDir
	resolveFn     resolveEncryptedDirFn
}

var _ vfs.FS = (*autoDecryptFS)(nil)

type encryptedDir struct {
	once       sync.Once
	env        *fs.Env
	resolveErr error
}

type resolveEncryptedDirFn func(dir string) (*fs.Env, error)

// Init sets up the given paths as encrypted and provides a callback that can
// resolve an encrypted directory path into an FS.
//
// Any FS operations inside encrypted paths will use the corresponding resolved FS.
//
// resolveFN is lazily called at most once for each encrypted dir.
func (afs *autoDecryptFS) Init(encryptedDirs []string, resolveFn resolveEncryptedDirFn) {
	afs.resolveFn = resolveFn

	afs.encryptedDirs = make(map[string]*encryptedDir)
	for _, dir := range encryptedDirs {
		if absDir, err := filepath.Abs(dir); err == nil {
			dir = absDir
		}
		afs.encryptedDirs[dir] = &encryptedDir{}
	}
}

func (afs *autoDecryptFS) Close() error {
	for _, eDir := range afs.encryptedDirs {
		eDir.env.Close()
	}
	return nil
}

func (afs *autoDecryptFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	name, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	fs, err := afs.maybeSwitchFS(name)
	if err != nil {
		return nil, err
	}
	return fs.Create(name, category)
}

func (afs *autoDecryptFS) Link(oldname, newname string) error {
	oldname, err := filepath.Abs(oldname)
	if err != nil {
		return err
	}
	newname, err = filepath.Abs(newname)
	if err != nil {
		return err
	}
	fs, err := afs.maybeSwitchFS(oldname)
	if err != nil {
		return err
	}
	return fs.Link(oldname, newname)
}

func (afs *autoDecryptFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	name, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	fs, err := afs.maybeSwitchFS(name)
	if err != nil {
		return nil, err
	}
	return fs.Open(name, opts...)
}

func (afs *autoDecryptFS) OpenReadWrite(
	name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption,
) (vfs.File, error) {
	name, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	fs, err := afs.maybeSwitchFS(name)
	if err != nil {
		return nil, err
	}
	return fs.OpenReadWrite(name, category, opts...)
}

func (afs *autoDecryptFS) OpenDir(name string) (vfs.File, error) {
	name, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	fs, err := afs.maybeSwitchFS(name)
	if err != nil {
		return nil, err
	}
	return fs.OpenDir(name)
}

func (afs *autoDecryptFS) Remove(name string) error {
	name, err := filepath.Abs(name)
	if err != nil {
		return err
	}
	fs, err := afs.maybeSwitchFS(name)
	if err != nil {
		return err
	}
	return fs.Remove(name)
}

func (afs *autoDecryptFS) RemoveAll(name string) error {
	name, err := filepath.Abs(name)
	if err != nil {
		return err
	}
	fs, err := afs.maybeSwitchFS(name)
	if err != nil {
		return err
	}
	return fs.RemoveAll(name)
}

func (afs *autoDecryptFS) Rename(oldname, newname string) error {
	fs, err := afs.maybeSwitchFS(oldname)
	if err != nil {
		return err
	}
	return fs.Rename(oldname, newname)
}

func (afs *autoDecryptFS) ReuseForWrite(
	oldname, newname string, category vfs.DiskWriteCategory,
) (vfs.File, error) {
	oldname, err := filepath.Abs(oldname)
	if err != nil {
		return nil, err
	}
	newname, err = filepath.Abs(newname)
	if err != nil {
		return nil, err
	}
	fs, err := afs.maybeSwitchFS(oldname)
	if err != nil {
		return nil, err
	}
	return fs.ReuseForWrite(oldname, newname, category)
}

func (afs *autoDecryptFS) MkdirAll(dir string, perm os.FileMode) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	fs, err := afs.maybeSwitchFS(dir)
	if err != nil {
		return err
	}
	return fs.MkdirAll(dir, perm)
}

func (afs *autoDecryptFS) Lock(name string) (io.Closer, error) {
	name, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	fs, err := afs.maybeSwitchFS(name)
	if err != nil {
		return nil, err
	}
	return fs.Lock(name)
}

func (afs *autoDecryptFS) List(dir string) ([]string, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	fs, err := afs.maybeSwitchFS(dir)
	if err != nil {
		return nil, err
	}
	return fs.List(dir)
}

func (afs *autoDecryptFS) Stat(name string) (vfs.FileInfo, error) {
	name, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	fs, err := afs.maybeSwitchFS(name)
	if err != nil {
		return nil, err
	}
	return fs.Stat(name)
}

func (afs *autoDecryptFS) PathBase(path string) string {
	return filepath.Base(path)
}

func (afs *autoDecryptFS) PathJoin(elem ...string) string {
	return filepath.Join(elem...)
}

func (afs *autoDecryptFS) PathDir(path string) string {
	return filepath.Dir(path)
}

func (afs *autoDecryptFS) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return vfs.DiskUsage{}, err
	}
	fs, err := afs.maybeSwitchFS(path)
	if err != nil {
		return vfs.DiskUsage{}, err
	}
	return fs.GetDiskUsage(path)
}

// Unwrap is part of the vfs.FS interface.
func (afs *autoDecryptFS) Unwrap() vfs.FS {
	return nil
}

// maybeSwitchFS finds the first ancestor of path that is registered as an
// encrypted FS; if there is such a path, returns the decrypted FS for that
// path. Otherwise, returns the default FS.
//
// Assumes that path is absolute and clean (i.e. filepath.Abs was run on it);
// the returned FS also assumes that any paths used are absolute and clean. This
// is needed when using encryptedFS, since the FileRegistry used in that
// context attempts to convert function input paths to relative paths using the
// DBDir. Both the DBDir and function input paths in a CockroachDB node are
// absolute paths, but when using the Pebble tool, the function input paths are
// based on what the cli user passed to the pebble command. We do not wish for a
// user using the cli to remember to pass an absolute path to the various pebble
// tool commands that accept paths. Note that the pebble tool commands taking a
// path parameter are quite varied: ranging from "pebble db" to "pebble lsm", so
// it is simplest to intercept the function input paths here.
func (afs *autoDecryptFS) maybeSwitchFS(path string) (vfs.FS, error) {
	if e, err := afs.getEnv(path); err != nil {
		return nil, err
	} else if e != nil {
		return e, nil
	}
	return vfs.Default, nil
}

func (afs *autoDecryptFS) getEnv(path string) (*fs.Env, error) {
	for {
		if e := afs.encryptedDirs[path]; e != nil {
			e.once.Do(func() {
				e.env, e.resolveErr = afs.resolveFn(path)
			})
			return e.env, e.resolveErr
		}
		parent := filepath.Dir(path)
		if path == parent {
			break
		}
		path = parent
	}
	return nil, nil
}

// pebbleOpenOptionLockDir implements pebble/tool.OpenOption, updating the
// *pebble.Options with a *pebble.Lock for the relevant directory. When
// initializing an fs.Env for a data directory, CockroachDB must manually lock
// the directory in order to protect additional CockroachDB internal state (eg,
// encryption-at-rest). pebble.Open will also try to acquire the directory lock
// if the caller hasn't passed an existing lock through the *pebble.Options.
// This OpenOption will acquire the relevant lock (by opening the Env, if it
// isn't already open) and pass it into the *pebble.Options.
type pebbleOpenOptionLockDir struct {
	*autoDecryptFS
}

// Assert that pebbleOpenOptionLockDir implements tool.OpenOption.
var _ tool.OpenOption = pebbleOpenOptionLockDir{}

// Apply implements tool.OpenOption.
func (o pebbleOpenOptionLockDir) Apply(dirname string, opts *pebble.Options) {
	absolutePath, err := filepath.Abs(dirname)
	if err != nil {
		panic(err)
	}
	e, err := o.getEnv(absolutePath)
	if err != nil {
		panic(err)
	}
	if e != nil {
		opts.Lock = e.DirectoryLock
	}
}
