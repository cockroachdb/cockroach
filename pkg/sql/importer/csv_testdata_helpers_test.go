// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/errors"
)

var rewriteCSVTestData = envutil.EnvOrDefaultBool("COCKROACH_REWRITE_CSV_TESTDATA", false)

type csvTestFiles struct {
	files, gzipFiles, bzipFiles, filesWithOpts, filesWithDups, fileWithShadowKeys,
	fileWithDupKeySameValue []string
	filesUsingWildcard, gzipFilesUsingWildcard, bzipFilesUsingWildcard []string
}

// Returns a single CSV file with a previously imported key sandiwched between
// a set of unqiue keys. This is used to ensure that IMPORT does not allow
// ingestion of shadowing keys.
func makeShadowKeyTestFile(t testing.TB, numRowsImportedBefore int, suffix string) {
	if numRowsImportedBefore < 1 {
		t.Fatal(errors.Errorf("table has no existing rows to shadow"))
	}
	padding := 10
	dir := testutils.TestDataPath(t, "csv")
	fileName := filepath.Join(dir, fmt.Sprintf("shadow-data%s", suffix))
	f, err := os.Create(fileName)
	if err != nil {
		t.Fatal(err)
	}
	// Start the file with some non-colliding rows.
	for i := numRowsImportedBefore; i < numRowsImportedBefore+padding; i++ {
		if _, err := fmt.Fprintf(f, "%d,%c\n", i, 'A'+i%26); err != nil {
			t.Fatal(err)
		}
	}
	numRowsImportedBefore += padding

	// Insert colliding row.
	if _, err := fmt.Fprintf(f, "%d,%c\n", 0, 'A'); err != nil {
		t.Fatal(err)
	}

	// Pad file with some more non-colliding rows.
	for i := numRowsImportedBefore; i < numRowsImportedBefore+padding; i++ {
		if _, err := fmt.Fprintf(f, "%d,%c\n", i, 'A'+i%26); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func makeDupWithSameValueFile(t testing.TB, suffix string) {
	dir := testutils.TestDataPath(t, "csv")
	fileName := filepath.Join(dir, fmt.Sprintf("dup-key-same-value%s", suffix))
	f, err := os.Create(fileName)
	if err != nil {
		t.Fatal(err)
	}
	// Start the file with some non-colliding rows.
	for i := 0; i < 200; i++ {
		if _, err := fmt.Fprintf(f, "%d,%c\n", i, 'A'+i%26); err != nil {
			t.Fatal(err)
		}
	}

	// Insert dup keys with same value.
	for i := 0; i < 200; i++ {
		if _, err := fmt.Fprintf(f, "%d,%c\n", i, 'A'+i%26); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func getTestFiles(numFiles int) csvTestFiles {
	var testFiles csvTestFiles
	suffix := ""
	if util.RaceEnabled {
		suffix = "-race"
	}
	for i := 0; i < numFiles; i++ {
		testFiles.files = append(testFiles.files, fmt.Sprintf(`'nodelocal://0/%s'`, fmt.Sprintf("data-%d%s", i, suffix)+"?nonsecret=nosecrets"))
		testFiles.gzipFiles = append(testFiles.gzipFiles, fmt.Sprintf(`'nodelocal://0/%s'`, fmt.Sprintf("data-%d%s.gz", i, suffix)+"?AWS_SESSION_TOKEN=secrets"))
		testFiles.bzipFiles = append(testFiles.bzipFiles, fmt.Sprintf(`'nodelocal://0/%s'`, fmt.Sprintf("data-%d%s.bz2", i, suffix)))
		testFiles.filesWithOpts = append(testFiles.filesWithOpts, fmt.Sprintf(`'nodelocal://0/%s'`, fmt.Sprintf("data-%d-opts%s", i, suffix)))
		testFiles.filesWithDups = append(testFiles.filesWithDups, fmt.Sprintf(`'nodelocal://0/%s'`, fmt.Sprintf("data-%d-dup%s", i, suffix)))
	}

	testFiles.fileWithDupKeySameValue = append(testFiles.fileWithDupKeySameValue, fmt.Sprintf(`'nodelocal://0/%s'`, fmt.Sprintf("dup-key-same-value%s", suffix)))
	testFiles.fileWithShadowKeys = append(testFiles.fileWithShadowKeys, fmt.Sprintf(`'nodelocal://0/%s'`, fmt.Sprintf("shadow-data%s", suffix)))

	wildcardFileName := "data-[0-9]"
	testFiles.filesUsingWildcard = append(testFiles.filesUsingWildcard, fmt.Sprintf(`'nodelocal://0/%s%s'`, wildcardFileName, suffix))
	testFiles.gzipFilesUsingWildcard = append(testFiles.gzipFilesUsingWildcard, fmt.Sprintf(`'nodelocal://0/%s%s.gz'`, wildcardFileName, suffix))
	testFiles.bzipFilesUsingWildcard = append(testFiles.gzipFilesUsingWildcard, fmt.Sprintf(`'nodelocal://0/%s%s.bz2'`, wildcardFileName, suffix))

	return testFiles
}

func makeFiles(t testing.TB, numFiles, rowsPerFile int, dir string, makeRaceFiles bool) {
	suffix := ""
	if makeRaceFiles {
		suffix = "-race"
	}

	for fn := 0; fn < numFiles; fn++ {
		// Create normal CSV file.
		fileName := filepath.Join(dir, fmt.Sprintf("data-%d%s", fn, suffix))
		f, err := os.Create(fileName)
		if err != nil {
			t.Fatal(err)
		}

		// Create CSV file which tests query options.
		fWithOpts, err := os.Create(filepath.Join(dir, fmt.Sprintf("data-%d-opts%s", fn, suffix)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprint(fWithOpts, "This is a header line to be skipped\n"); err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprint(fWithOpts, "So is this\n"); err != nil {
			t.Fatal(err)
		}

		// Create CSV file with duplicate entries.
		fDup, err := os.Create(filepath.Join(dir, fmt.Sprintf("data-%d-dup%s", fn, suffix)))
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < rowsPerFile; i++ {
			x := fn*rowsPerFile + i
			if _, err := fmt.Fprintf(f, "%d,%c\n", x, 'A'+x%26); err != nil {
				t.Fatal(err)
			}
			if _, err := fmt.Fprintf(fDup, "1,%c\n", 'A'+x%26); err != nil {
				t.Fatal(err)
			}

			// Write a comment.
			if _, err := fmt.Fprintf(fWithOpts, "# %d\n", x); err != nil {
				t.Fatal(err)
			}
			// Write a pipe-delim line with trailing delim.
			if x%4 == 0 { // 1/4 of rows have blank val for b
				if _, err := fmt.Fprintf(fWithOpts, "%d||\n", x); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := fmt.Fprintf(fWithOpts, "%d|%c|\n", x, 'A'+x%26); err != nil {
					t.Fatal(err)
				}
			}
		}

		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if err := fDup.Close(); err != nil {
			t.Fatal(err)
		}
		if err := fWithOpts.Close(); err != nil {
			t.Fatal(err)
		}

		// Check in zipped versions of CSV file fileName.
		_ = gzipFile(t, fileName)
		_ = bzipFile(t, "", fileName)
	}

	makeDupWithSameValueFile(t, suffix)
	makeShadowKeyTestFile(t, rowsPerFile, suffix)
}

func makeCSVData(
	t testing.TB, numFiles, rowsPerFile, numRaceFiles, rowsPerRaceFile int,
) csvTestFiles {
	if rewriteCSVTestData {
		dir := testutils.TestDataPath(t, "csv")
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(dir, 0777); err != nil {
			t.Fatal(err)
		}

		makeFiles(t, numFiles, rowsPerFile, dir, false /* makeRaceFiles */)
		makeFiles(t, numRaceFiles, rowsPerRaceFile, dir, true)
	}

	if util.RaceEnabled {
		return getTestFiles(numRaceFiles)
	}
	return getTestFiles(numFiles)
}

func gzipFile(t testing.TB, in string) string {
	r, err := os.Open(in)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	name := in + ".gz"
	f, err := os.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := gzip.NewWriter(f)
	if _, err := io.Copy(w, r); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return name
}

func bzipFile(t testing.TB, dir, in string) string {
	_, err := exec.Command("bzip2", "-k", filepath.Join(dir, in)).CombinedOutput()
	if err != nil {
		if strings.Contains(err.Error(), "executable file not found") {
			return ""
		}
		t.Fatal(err)
	}
	return in + ".bz2"
}
