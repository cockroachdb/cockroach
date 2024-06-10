// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	basedir := t.TempDir()
	require.NoError(t, os.WriteFile(p(basedir, "a1"), []byte("foo"), 0777))
	require.NoError(t, os.WriteFile(p(basedir, "a2"), []byte("foo"), 0777))
	require.NoError(t, os.WriteFile(p(basedir, "a3"), []byte("foo"), 0777))
	require.NoError(t, os.Mkdir(p(basedir, "dir1"), 0777))
	require.NoError(t, os.WriteFile(p(basedir, "dir1", "file1"), []byte("foo"), 0777))
	require.NoError(t, os.WriteFile(p(basedir, "dir1", "file2"), []byte("foo"), 0777))
	require.NoError(t, os.Mkdir(p(basedir, "dir2"), 0777))
	require.NoError(t, os.WriteFile(p(basedir, "dir2", "file1"), []byte("foo"), 0777))
	require.NoError(t, os.WriteFile(p(basedir, "dir2", "file2"), []byte("foo"), 0777))

	// expectLs checks the current directory listing of dir.
	expectLs := func(expected ...string) {
		t.Helper()
		var actual []string
		require.NoError(t, filepath.Walk(basedir, func(path string, info os.FileInfo, err error) error {
			require.NoError(t, err)
			if !info.IsDir() {
				rel, err := filepath.Rel(basedir, path)
				require.NoError(t, err)
				actual = append(actual, rel)
			}
			return nil
		}))
		require.Equal(t, expected, actual)
	}
	j := filepath.Join
	expectLs("a1", "a2", "a3", j("dir1", "file1"), j("dir1", "file2"), j("dir2", "file1"), j("dir2", "file2"))

	list, err := filterDirEntries(basedir, func(entry os.DirEntry) bool {
		return entry.Name() != "a2" && entry.Name() != "dir2"
	})
	require.NoError(t, err)
	require.Equal(t, []string{"a1", "a3", "dir1"}, list)
	require.NoError(t, moveToZipArchive("first.zip", basedir, list...))
	expectZip(t, basedir, "first.zip", "a1", "a3", "dir1/file1", "dir1/file2")
	expectLs("a2", j("dir2", "file1"), j("dir2", "file2"), "first.zip")

	list, err = filterDirEntries(basedir, func(entry os.DirEntry) bool {
		return !strings.HasSuffix(entry.Name(), ".zip")
	})
	require.NoError(t, err)
	require.NoError(t, moveToZipArchive("second.zip", basedir, list...))
	expectZip(t, basedir, "second.zip", "a2", "dir2/file1", "dir2/file2")
	expectLs("first.zip", "second.zip")
}

func TestZipArtifacts(t *testing.T) {
	tmp := t.TempDir()
	artifactsDir := filepath.Join(tmp, "someTestRun")
	require.NoError(t, os.Mkdir(artifactsDir, 0777))
	require.NoError(t, os.WriteFile(filepath.Join(artifactsDir, "test.log"), []byte("foobar"), 0777))
	require.NoError(t, os.WriteFile(filepath.Join(artifactsDir, "test-teardown.log"), []byte{}, 0777))
	perfArtifactsDir := filepath.Join(artifactsDir, "1.perf")
	require.NoError(t, os.Mkdir(perfArtifactsDir, 0777))
	require.NoError(t, os.WriteFile(filepath.Join(perfArtifactsDir, "stats.json"), []byte("{}"), 0777))
	tt := testImpl{
		artifactsDir: artifactsDir,
	}
	zipArtifacts(&tt)
	expectZip(t, artifactsDir, "artifacts.zip", "test.log", "test-teardown.log")
}

// expectZip checks the files contained in the given archive; paths must use
// slashes.
func expectZip(t *testing.T, basedir string, archiveName string, expected ...string) {
	r, err := zip.OpenReader(p(basedir, archiveName))
	require.NoError(t, err)
	var actual []string
	for _, f := range r.File {
		actual = append(actual, f.Name)
	}
	require.ElementsMatch(t, expected, actual)
	require.NoError(t, r.Close())
}

func p(basedir string, elems ...string) string {
	return filepath.Join(append([]string{basedir}, elems...)...)
}
