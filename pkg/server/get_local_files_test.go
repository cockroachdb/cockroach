// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestGetLocalFiles tests that retrieving local heap, goroutine, and cpu
// profiles returns and reads files when they exist and returns the expected
// error when the file on encountered errors.
// Note that there is more extensive end to end testing of this done in
// pkg/server/storage_api/files_test.go. This test was created to test the
// error handling in response to #114453.
func TestGetLocalFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We'll read a file successfully first to ensure the read logic is working.
	tempDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	testHeapDir := filepath.Join(tempDir, "heap_profiles")
	if err := os.MkdirAll(testHeapDir, os.ModePerm); err != nil {
		t.Fatal(err)
	}
	testCPUDir := filepath.Join(tempDir, "cpu_profiles")
	if err := os.MkdirAll(testCPUDir, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	t.Run("read files successfully", func(t *testing.T) {
		for _, test := range []string{"heap", "cpu"} {
			t.Run(test, func(t *testing.T) {
				testDir, typ := testHeapDir, serverpb.FileType_HEAP
				if test == "cpu" {
					testDir, typ = testCPUDir, serverpb.FileType_CPU
				}
				fileName := fmt.Sprintf("im-in-the-%s-dir", test)
				testFile := filepath.Join(testDir, fileName)
				if err := os.WriteFile(testFile, []byte(fmt.Sprintf("%s stuff", test)), 0644); err != nil {
					t.Fatal(err)
				}
				req := &serverpb.GetFilesRequest{NodeId: "local", Type: typ, Patterns: []string{"*"}}
				res, err := getLocalFiles(req, testHeapDir, "", testCPUDir, os.Stat, os.ReadFile)
				require.NoError(t, err)
				require.Equal(t, 1, len(res.Files))
				require.Equal(t, fileName, res.Files[0].Name)
			})
		}
	})

	t.Run("stat file error", func(t *testing.T) {
		testHeapFile := filepath.Join(testHeapDir, "im-in-the-heap-dir")
		if err := os.WriteFile(testHeapFile, []byte("heap stuff"), 0644); err != nil {
			t.Fatal(err)
		}
		// Test that getLocalFiles returns an error when the file cannot be read with stat.
		statFileWithErr := func(name string) (os.FileInfo, error) {
			return nil, errors.Errorf("stat error")
		}
		req := &serverpb.GetFilesRequest{
			NodeId: "local", Type: serverpb.FileType_HEAP, Patterns: []string{"*"}}
		_, err := getLocalFiles(req, testHeapDir, "", "", statFileWithErr, os.ReadFile)
		require.ErrorContains(t, err, "stat error")
	})

	t.Run("read file error", func(t *testing.T) {
		testHeapFile := filepath.Join(testHeapDir, "im-in-the-heap-dir")
		if err := os.WriteFile(testHeapFile, []byte("heap stuff"), 0644); err != nil {
			t.Fatal(err)
		}
		// Test that getLocalFiles returns an error when the file cannot be read.
		readFileWithErr := func(name string) ([]byte, error) {
			return nil, errors.Errorf("read error")
		}
		req := &serverpb.GetFilesRequest{
			NodeId: "local", Type: serverpb.FileType_HEAP, Patterns: []string{"*"}}
		_, err := getLocalFiles(req, testHeapDir, "", "", os.Stat, readFileWithErr)
		require.ErrorContains(t, err, "read error")
	})

	t.Run("dirs not implemented", func(t *testing.T) {
		req := &serverpb.GetFilesRequest{
			NodeId: "local", Type: serverpb.FileType_HEAP, Patterns: []string{"*"}}
		_, err := getLocalFiles(req, "", "nonexistent", "nonexistent", os.Stat, os.ReadFile)
		require.ErrorContains(t, err, "dump directory not configured")

		req = &serverpb.GetFilesRequest{
			NodeId: "local", Type: serverpb.FileType_GOROUTINES, Patterns: []string{"*"}}
		_, err = getLocalFiles(req, "nonexistent", "", "nonexistent", os.Stat, os.ReadFile)
		require.ErrorContains(t, err, "dump directory not configured")

		req = &serverpb.GetFilesRequest{
			NodeId: "local", Type: serverpb.FileType_CPU, Patterns: []string{"*"}}
		_, err = getLocalFiles(req, "nonexistent", "nonexistent", "", os.Stat, os.ReadFile)
		require.ErrorContains(t, err, "dump directory not configured")
	})
}
