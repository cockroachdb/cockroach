// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"archive/zip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMoveToZipArchive(t *testing.T) {
	dir := t.TempDir()
	p := func(elems ...string) string {
		return filepath.Join(append([]string{dir}, elems...)...)
	}
	require.NoError(t, os.WriteFile(p("a1"), []byte("foo"), 0777))
	require.NoError(t, os.WriteFile(p("a2"), []byte("foo"), 0777))
	require.NoError(t, os.WriteFile(p("a3"), []byte("foo"), 0777))
	require.NoError(t, os.Mkdir(p("dir1"), 0777))
	require.NoError(t, os.WriteFile(p("dir1", "file1"), []byte("foo"), 0777))
	require.NoError(t, os.WriteFile(p("dir1", "file2"), []byte("foo"), 0777))
	require.NoError(t, os.Mkdir(p("dir2"), 0777))
	require.NoError(t, os.WriteFile(p("dir2", "file1"), []byte("foo"), 0777))
	require.NoError(t, os.WriteFile(p("dir2", "file2"), []byte("foo"), 0777))

	// expectLs checks the current directory listing of dir.
	expectLs := func(expected ...string) {
		t.Helper()
		var actual []string
		require.NoError(t, filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			require.NoError(t, err)
			if !info.IsDir() {
				rel, err := filepath.Rel(dir, path)
				require.NoError(t, err)
				actual = append(actual, rel)
			}
			return nil
		}))
		require.Equal(t, expected, actual)
	}
	// expectZip checks the files contained in the given archive; paths must use
	// slashes.
	expectZip := func(archiveName string, expected ...string) {
		r, err := zip.OpenReader(p(archiveName))
		require.NoError(t, err)
		var actual []string
		for _, f := range r.File {
			actual = append(actual, f.Name)
		}
		require.Equal(t, expected, actual)
		require.NoError(t, r.Close())
	}
	j := filepath.Join
	expectLs("a1", "a2", "a3", j("dir1", "file1"), j("dir1", "file2"), j("dir2", "file1"), j("dir2", "file2"))

	list, err := filterDirEntries(dir, func(entry os.DirEntry) bool {
		return entry.Name() != "a2" && entry.Name() != "dir2"
	})
	require.NoError(t, err)
	require.Equal(t, []string{"a1", "a3", "dir1"}, list)
	require.NoError(t, moveToZipArchive("first.zip", dir, list...))
	expectZip("first.zip", "a1", "a3", "dir1/file1", "dir1/file2")
	expectLs("a2", j("dir2", "file1"), j("dir2", "file2"), "first.zip")

	list, err = filterDirEntries(dir, func(entry os.DirEntry) bool {
		return !strings.HasSuffix(entry.Name(), ".zip")
	})
	require.NoError(t, err)
	require.NoError(t, moveToZipArchive("second.zip", dir, list...))
	expectZip("second.zip", "a2", "dir2/file1", "dir2/file2")
	expectLs("first.zip", "second.zip")
}
