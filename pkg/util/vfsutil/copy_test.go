// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vfsutil_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/vfsutil"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestCopyRecursive(t *testing.T) {
	// Create source memFS with test data
	srcFS := vfs.NewMem()

	// Create a directory structure
	require.NoError(t, srcFS.MkdirAll("testdir/subdir", 0755))

	// Create some test files
	f1, err := srcFS.Create("testdir/file1.txt", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	_, err = f1.Write([]byte("hello world"))
	require.NoError(t, err)
	require.NoError(t, f1.Close())

	f2, err := srcFS.Create("testdir/subdir/file2.txt", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	_, err = f2.Write([]byte("nested file"))
	require.NoError(t, err)
	require.NoError(t, f2.Close())

	// Create destination FS (real filesystem for this example)
	dstFS := vfs.Default

	// Copy from memFS to real filesystem in a temp directory
	tempDir := t.TempDir()
	require.NoError(t, vfsutil.CopyRecursive(srcFS, dstFS, "testdir", dstFS.PathJoin(tempDir, "copied")))

	// Verify the copy worked
	files, err := dstFS.List(dstFS.PathJoin(tempDir, "copied"))
	require.NoError(t, err)
	require.Contains(t, files, "file1.txt")
	require.Contains(t, files, "subdir")

	// Verify nested file
	content, err := dstFS.Open(dstFS.PathJoin(tempDir, "copied", "subdir", "file2.txt"))
	require.NoError(t, err)
	defer content.Close()
	buf := make([]byte, 100)
	n, err := content.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "nested file", string(buf[:n]))
}
