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
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	file, cleanUp := createTestFile()
	defer cleanUp()

	c.Run(fmt.Sprintf("nodelocal upload %s /test/file1.csv", file))
	c.Run(fmt.Sprintf("nodelocal upload %s /test/file2.csv", file))
	c.Run(fmt.Sprintf("nodelocal upload %s /test/file1.csv", file))
	c.Run(fmt.Sprintf("nodelocal upload %s /test/../../file1.csv", file))
	c.Run(fmt.Sprintf("nodelocal upload notexist.csv /test/file1.csv"))

	// Output:
	// nodelocal upload test.csv /test/file1.csv
	// successfully uploaded to nodelocal://1/test/file1.csv
	// nodelocal upload test.csv /test/file2.csv
	// successfully uploaded to nodelocal://1/test/file2.csv
	// nodelocal upload test.csv /test/file1.csv
	// ERROR: destination file already exists for /test/file1.csv
	// nodelocal upload test.csv /test/../../file1.csv
	// ERROR: current transaction is aborted, commands ignored until end of transaction block
	// SQLSTATE: 25P02
	// nodelocal upload notexist.csv /test/file1.csv
	// ERROR: open notexist.csv: no such file or directory
}

func TestNodeLocalFileUpload(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	dir, cleanFn := testutils.TempDir(t)
	defer cleanFn()

	for i, tc := range []struct {
		name        string
		fileContent []byte
	}{
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

func createTestFile() (string, func()) {
	filePath := "test.csv"
	err := ioutil.WriteFile(filePath, []byte("file content"), 0666)
	if err != nil {
		return "", func() {}
	}
	return filePath, func() {
		_ = os.Remove(filePath)
	}
}
