// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage_api_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestStatusGetFiles tests the GetFiles endpoint.
func TestStatusGetFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tempDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	storeSpec := base.StoreSpec{Path: tempDir}

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			storeSpec,
		},
	})
	defer srv.Stopper().Stop(context.Background())

	ts := srv.ApplicationLayer()

	client := ts.GetStatusClient(t)

	// Test fetching heap files.
	t.Run("heap", func(t *testing.T) {
		const testFilesNo = 3
		for i := 0; i < testFilesNo; i++ {
			testHeapDir := filepath.Join(storeSpec.Path, "logs", base.HeapProfileDir)
			testHeapFile := filepath.Join(testHeapDir, fmt.Sprintf("heap%d.pprof", i))
			if err := os.MkdirAll(testHeapDir, os.ModePerm); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(testHeapFile, []byte(fmt.Sprintf("I'm heap file %d", i)), 0644); err != nil {
				t.Fatal(err)
			}
		}

		request := serverpb.GetFilesRequest{
			NodeId: "local", Type: serverpb.FileType_HEAP, Patterns: []string{"heap*"}}
		response, err := client.GetFiles(context.Background(), &request)
		if err != nil {
			t.Fatal(err)
		}

		if a, e := len(response.Files), testFilesNo; a != e {
			t.Errorf("expected %d files(s), found %d", e, a)
		}

		for i, file := range response.Files {
			expectedFileName := fmt.Sprintf("heap%d.pprof", i)
			if file.Name != expectedFileName {
				t.Fatalf("expected file name %s, found %s", expectedFileName, file.Name)
			}
			expectedFileContents := []byte(fmt.Sprintf("I'm heap file %d", i))
			if !bytes.Equal(file.Contents, expectedFileContents) {
				t.Fatalf("expected file contents %s, found %s", expectedFileContents, file.Contents)
			}
		}
	})

	// Test fetching goroutine files.
	t.Run("goroutines", func(t *testing.T) {

		// regex for goroutine file names manually added
		reDump := regexp.MustCompile(`goroutine_dump\d+.txt.gz`)
		// regex for goroutine file names dumped by goroutinedumper
		reOOMDump := regexp.MustCompile("goroutine_dump.*.double_since_last_dump.*.txt.gz")
		// regex for content of goroutine files manually added
		reDumpContent := regexp.MustCompile(`Goroutine dump \d+`)

		const testFilesNo = 3
		for i := 0; i < testFilesNo; i++ {
			testGoroutineDir := filepath.Join(storeSpec.Path, "logs", base.GoroutineDumpDir)
			testGoroutineFile := filepath.Join(testGoroutineDir, fmt.Sprintf("goroutine_dump%d.txt.gz", i))
			if err := os.MkdirAll(testGoroutineDir, os.ModePerm); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(testGoroutineFile, []byte(fmt.Sprintf("Goroutine dump %d", i)), 0644); err != nil {
				t.Fatal(err)
			}
		}

		request := serverpb.GetFilesRequest{
			NodeId: "local", Type: serverpb.FileType_GOROUTINES, Patterns: []string{"*"}}
		response, err := client.GetFiles(context.Background(), &request)
		if err != nil {
			t.Fatal(err)
		}

		if a, e := len(response.Files), testFilesNo; a < e {
			t.Errorf("expected at least %d files(s), found %d", e, a)
		}

		for _, file := range response.Files {
			if reOOMDump.MatchString(file.Name) {
				continue
			}
			if reDump.MatchString(file.Name) {
				if !reDumpContent.Match(file.Contents) {
					t.Fatalf("expected file content of form %s, found %s", reDumpContent,
						file.Contents)
				}
			} else {
				t.Fatalf("expected file name of form %s, found %s", reDump,
					file.Name)
			}
		}
	})

	// Testing path separators in pattern.
	t.Run("path separators", func(t *testing.T) {
		request := serverpb.GetFilesRequest{NodeId: "local", ListOnly: true,
			Type: serverpb.FileType_HEAP, Patterns: []string{"pattern/with/separators"}}
		_, err := client.GetFiles(context.Background(), &request)
		if !testutils.IsError(err, "invalid pattern: cannot have path seperators") {
			t.Errorf("GetFiles: path separators allowed in pattern")
		}
	})

	// Testing invalid filetypes.
	t.Run("filetypes", func(t *testing.T) {
		request := serverpb.GetFilesRequest{NodeId: "local", ListOnly: true,
			Type: -1, Patterns: []string{"*"}}
		_, err := client.GetFiles(context.Background(), &request)
		if !testutils.IsError(err, "unknown file type: -1") {
			t.Errorf("GetFiles: invalid file type allowed")
		}
	})
}
