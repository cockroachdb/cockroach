// Copyright 2019 The Cockroach Authors.
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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func Example_nodelocal() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	file, cleanUp := createTestFile("test.csv", "content")
	defer cleanUp()

	c.Run(fmt.Sprintf("nodelocal upload %s /test/file1.csv", file))
	c.Run(fmt.Sprintf("nodelocal upload %s /test/file2.csv", file))
	c.Run(fmt.Sprintf("nodelocal upload %s /test/file1.csv", file))
	c.Run(fmt.Sprintf("nodelocal upload %s /test/../../file1.csv", file))
	c.Run("nodelocal upload notexist.csv /test/file1.csv")

	// Output:
	// nodelocal upload test.csv /test/file1.csv
	// successfully uploaded to nodelocal://1/test/file1.csv
	// nodelocal upload test.csv /test/file2.csv
	// successfully uploaded to nodelocal://1/test/file2.csv
	// nodelocal upload test.csv /test/file1.csv
	// ERROR: destination file already exists for /test/file1.csv
	// nodelocal upload test.csv /test/../../file1.csv
	// ERROR: local file access to paths outside of external-io-dir is not allowed: ../file1.csv
	// nodelocal upload notexist.csv /test/file1.csv
	// ERROR: open notexist.csv: no such file or directory
}

func Example_nodelocal_disabled() {
	c := NewCLITest(TestCLIParams{NoNodelocal: true})
	defer c.Cleanup()

	file, cleanUp := createTestFile("test.csv", "non-empty-file")
	defer cleanUp()

	empty, cleanUpEmpty := createTestFile("empty.csv", "")
	defer cleanUpEmpty()

	c.Run(fmt.Sprintf("nodelocal upload %s /test/file1.csv", empty))
	c.Run(fmt.Sprintf("nodelocal upload %s /test/file1.csv", file))

	// Output:
	// nodelocal upload empty.csv /test/file1.csv
	// ERROR: local file access is disabled
	// nodelocal upload test.csv /test/file1.csv
	// ERROR: local file access is disabled
}

func TestNodeLocalFileUpload(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := NewCLITest(TestCLIParams{T: t})
	defer c.Cleanup()

	dir, cleanFn := testutils.TempDir(t)
	defer cleanFn()

	for i, tc := range []struct {
		name        string
		fileContent []byte
	}{
		{
			"empty",
			[]byte{},
		},
		{
			"exactly-one-chunk",
			make([]byte, chunkSize),
		},
		{
			"exactly-five-chunks",
			make([]byte, chunkSize*5),
		},
		{
			"less-than-one-chunk",
			make([]byte, chunkSize-100),
		},
		{
			"more-than-one-chunk",
			make([]byte, chunkSize+100),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(dir, fmt.Sprintf("file%d.csv", i))
			err := ioutil.WriteFile(filePath, tc.fileContent, 0666)
			if err != nil {
				t.Fatal(err)
			}
			destination := fmt.Sprintf("/test/file%d.csv", i)

			_, err = c.RunWithCapture(fmt.Sprintf("nodelocal upload %s %s", filePath, destination))
			if err != nil {
				t.Fatal(err)
			}
			writtenContent, err := ioutil.ReadFile(filepath.Join(c.Cfg.Settings.ExternalIODir, destination))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(tc.fileContent, writtenContent) {
				t.Fatalf("expected:\n%s\nbut got:\n%s", tc.fileContent, writtenContent)
			}
		})
	}
}

func createTestFile(name, content string) (string, func()) {
	tmpDir, err := ioutil.TempDir("", "")
	tmpFile := filepath.Join(tmpDir, testTempFilePrefix+name)
	if err == nil {
		err = ioutil.WriteFile(tmpFile, []byte(content), 0666)
	}
	if err != nil {
		return "", func() {}
	}
	return tmpFile, func() {
		_ = os.RemoveAll(tmpDir)
	}
}
