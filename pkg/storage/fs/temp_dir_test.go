// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"bytes"
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors/oserror"
)

func TestCreateTempDir(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	// Temporary parent directory to test this.
	dir := t.TempDir()

	tempDir, err := CreateTempDir(dir, "test-create-temp", stopper)
	if err != nil {
		t.Fatal(err)
	}

	if dir != filepath.Dir(tempDir) {
		t.Fatalf("unexpected parent directory of temp subdirectory.\nexpected: %s\nactual: %s", dir, filepath.Dir(tempDir))
	}

	_, err = os.Stat(tempDir)
	if oserror.IsNotExist(err) {
		t.Fatalf("expected %s temp subdirectory to exist", tempDir)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestRecordTempDir(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	recordFile := "foobar"

	f, err := os.CreateTemp("", "record-file")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			t.Fatal(err)
		}
	}()

	// We should close this since RecordTempDir should open the file.
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	if err = RecordTempDir(f.Name(), recordFile); err != nil {
		t.Fatal(err)
	}

	actual, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	expected := append([]byte(recordFile), '\n')
	if !bytes.Equal(expected, actual) {
		t.Fatalf("unexpected record file content after recording temp dir.\nexpected: %s\nactual: %s", expected, actual)
	}
}

func TestCleanupTempDirs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	recordFile, err := os.CreateTemp("", "record-file")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Remove(recordFile.Name()); err != nil {
			t.Fatal(err)
		}
	}()

	// Generate some temporary directories.
	var tempDirs []string
	for i := 0; i < 5; i++ {
		tempDir := t.TempDir()
		tempDirs = append(tempDirs, tempDir)
		// Record the temporary directories to the file.
		if _, err = recordFile.Write(append([]byte(tempDir), '\n')); err != nil {
			t.Fatal(err)
		}
	}

	if err = recordFile.Close(); err != nil {
		t.Fatal(err)
	}

	// Generate some temporary files inside the temporary directories.
	var tempFiles []string
	content := []byte("whatisthemeaningoflife\n")
	for i := 0; i < 10; i++ {
		dir := tempDirs[rand.Intn(len(tempDirs))]
		tempFile, err := os.CreateTemp(dir, "temp-file")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tempFile.Write(content); err != nil {
			t.Fatal(err)
		}
		if err = tempFile.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if err = CleanupTempDirs(recordFile.Name()); err != nil {
		t.Fatal(err)
	}

	// We check if all the temporary subdirectories and files were removed.
	for _, fname := range append(tempDirs, tempFiles...) {
		_, err = os.Stat(fname)
		if !oserror.IsNotExist(err) {
			t.Fatalf("file %s expected to be removed by cleanup", fname)
		}
		if err != nil {
			// We expect the files to not exist anymore.
			if oserror.IsNotExist(err) {
				continue
			}

			t.Fatal(err)
		}
	}
}
