// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestAutoDecryptFS(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if runtime.GOOS == "windows" {
		skip.IgnoreLint(t, "expected output uses unix paths")
	}
	dir, err := os.MkdirTemp("", "auto-decrypt-test")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(dir) }()

	path1 := filepath.Join(dir, "path1")
	path2 := filepath.Join(dir, "foo", "path2")

	var buf bytes.Buffer
	resolveFn := func(dir string) (vfs.FS, error) {
		if dir != path1 && dir != path2 {
			t.Fatalf("unexpected dir %s", dir)
		}
		fs := vfs.NewMem()
		require.NoError(t, fs.MkdirAll(dir, 0755))
		return WithLogging(fs, func(format string, args ...interface{}) {
			fmt.Fprintf(&buf, dir+": "+format+"\n", args...)
		}), nil
	}

	var fs autoDecryptFS
	fs.Init([]string{path1, path2}, resolveFn)

	create := func(pathElems ...string) {
		file, err := fs.Create(filepath.Join(pathElems...))
		require.NoError(t, err)
		file.Close()
	}

	create(dir, "foo")
	create(path1, "bar")
	create(path2, "baz")
	require.NoError(t, fs.MkdirAll(filepath.Join(path2, "a", "b"), 0755))
	create(path2, "a", "b", "xx")

	// Check that operations inside the two paths happen using the resolved FSes.
	output := strings.ReplaceAll(buf.String(), dir, "$TMPDIR")
	expected :=
		`$TMPDIR/path1: create: $TMPDIR/path1/bar
$TMPDIR/path1: close: $TMPDIR/path1/bar
$TMPDIR/foo/path2: create: $TMPDIR/foo/path2/baz
$TMPDIR/foo/path2: close: $TMPDIR/foo/path2/baz
$TMPDIR/foo/path2: mkdir-all: $TMPDIR/foo/path2/a/b 0755
$TMPDIR/foo/path2: create: $TMPDIR/foo/path2/a/b/xx
$TMPDIR/foo/path2: close: $TMPDIR/foo/path2/a/b/xx
`
	require.Equal(t, expected, output)
}

// WithLogging wraps an FS and logs filesystem modification operations to the
// given logFn.
func WithLogging(fs vfs.FS, logFn LogFn) vfs.FS {
	return &loggingFS{
		FS:    fs,
		logFn: logFn,
	}
}

// LogFn is a function that is used to capture a log when WithLogging is used.
type LogFn func(fmt string, args ...interface{})

type loggingFS struct {
	vfs.FS
	logFn LogFn
}

var _ vfs.FS = (*loggingFS)(nil)

func (fs *loggingFS) Create(name string) (vfs.File, error) {
	fs.logFn("create: %s", name)
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return newLoggingFile(f, name, fs.logFn), nil
}

func (fs *loggingFS) Link(oldname, newname string) error {
	fs.logFn("link: %s -> %s", oldname, newname)
	return fs.FS.Link(oldname, newname)
}

func (fs *loggingFS) OpenDir(name string) (vfs.File, error) {
	fs.logFn("open-dir: %s", name)
	f, err := fs.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return newLoggingFile(f, name, fs.logFn), nil
}

func (fs *loggingFS) Rename(oldname, newname string) error {
	fs.logFn("rename: %s -> %s", oldname, newname)
	return fs.FS.Rename(oldname, newname)
}

func (fs *loggingFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	fs.logFn("reuseForWrite: %s -> %s", oldname, newname)
	f, err := fs.FS.ReuseForWrite(oldname, newname)
	if err != nil {
		return nil, err
	}
	return newLoggingFile(f, newname, fs.logFn), nil
}

func (fs *loggingFS) MkdirAll(dir string, perm os.FileMode) error {
	fs.logFn("mkdir-all: %s %#o", dir, perm)
	return fs.FS.MkdirAll(dir, perm)
}

func (fs *loggingFS) Lock(name string) (io.Closer, error) {
	fs.logFn("lock: %s", name)
	return fs.FS.Lock(name)
}

func (fs loggingFS) Remove(name string) error {
	fs.logFn("remove: %s", name)
	err := fs.FS.Remove(name)
	return err
}

func (fs loggingFS) RemoveAll(name string) error {
	fs.logFn("remove-all: %s", name)
	err := fs.FS.RemoveAll(name)
	return err
}

type loggingFile struct {
	vfs.File
	name  string
	logFn LogFn
}

var _ vfs.File = (*loggingFile)(nil)

func newLoggingFile(f vfs.File, name string, logFn LogFn) *loggingFile {
	return &loggingFile{
		File:  f,
		name:  name,
		logFn: logFn,
	}
}

func (f *loggingFile) Close() error {
	f.logFn("close: %s", f.name)
	return f.File.Close()
}

func (f *loggingFile) Sync() error {
	f.logFn("sync: %s", f.name)
	return f.File.Sync()
}

func (f *loggingFile) SyncData() error {
	f.logFn("sync-data: %s", f.name)
	return f.File.SyncData()
}

func (f *loggingFile) SyncTo(length int64) (fullSync bool, err error) {
	f.logFn("sync-to(%d): %s", length, f.name)
	return f.File.SyncTo(length)
}
