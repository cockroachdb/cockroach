// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"io"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestSafeWriteToFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use an in-memory FS that strictly enforces syncs.
	mem := vfs.NewCrashableMem()
	syncDir := func(dir string) {
		fdir, err := mem.OpenDir(dir)
		require.NoError(t, err)
		require.NoError(t, fdir.Sync())
		require.NoError(t, fdir.Close())
	}
	readFile := func(mem *vfs.MemFS, filename string) []byte {
		f, err := mem.Open("foo/bar")
		require.NoError(t, err)
		b, err := io.ReadAll(f)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		return b
	}

	require.NoError(t, mem.MkdirAll("foo", os.ModePerm))
	syncDir("")
	f, err := mem.Create("foo/bar", UnspecifiedWriteCategory)
	require.NoError(t, err)
	_, err = io.WriteString(f, "Hello world")
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
	syncDir("foo")

	// Discard any unsynced writes to make sure we set up the test
	// preconditions correctly.
	crashFS := mem.CrashClone(vfs.CrashCloneCfg{})
	require.Equal(t, []byte("Hello world"), readFile(crashFS, "foo/bar"))

	// Use SafeWriteToFile to atomically, durably change the contents of the
	// file.
	require.NoError(t, SafeWriteToFile(crashFS, "foo", "foo/bar", []byte("Hello everyone"), UnspecifiedWriteCategory))

	// Discard any unsynced writes.
	crashFS = crashFS.CrashClone(vfs.CrashCloneCfg{})
	require.Equal(t, []byte("Hello everyone"), readFile(crashFS, "foo/bar"))
}
