// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestVersions verifies that both getVersions() and writeVersionFile work
// correctly.
func TestVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, err := ioutil.TempDir("", "testing")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}()

	// First test when no file exists yet.
	ver, err := getVersion(dir)
	if err != nil {
		t.Fatal(err)
	}
	if ver != versionNoFile {
		t.Errorf("no version file version should be %d, got %d", versionNoFile, ver)
	}

	// Write the current versions to the file.
	if err := writeVersionFile(dir, versionCurrent); err != nil {
		t.Fatal(err)
	}
	ver, err = getVersion(dir)
	if err != nil {
		t.Fatal(err)
	}
	if ver != versionCurrent {
		t.Errorf("current versions do not match, expected %d got %d", versionCurrent, ver)
	}

	// Write gibberish to the file.
	filename := getVersionFilename(dir)
	if err := os.Remove(filename); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(filename, []byte("cause an error please"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := getVersion(dir); !testutils.IsError(err, "is not formatted correctly") {
		t.Errorf("expected error contains '%s', got '%v'", "is not formatted correctly", err)
	}
}
