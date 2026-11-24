// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
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
			err := os.WriteFile(filePath, tc.fileContent, 0666)
			if err != nil {
				t.Fatal(err)
			}
			destination := fmt.Sprintf("/test/file%d.csv", i)

			_, err = c.RunWithCapture(fmt.Sprintf("nodelocal upload %s %s", filePath, destination))
			if err != nil {
				t.Fatal(err)
			}
			writtenContent, err := os.ReadFile(filepath.Join(c.Server.ExternalIODir(), destination))
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
	tmpDir, err := os.MkdirTemp("", "")
	tmpFile := filepath.Join(tmpDir, testTempFilePrefix+name)
	if err == nil {
		err = os.WriteFile(tmpFile, []byte(content), 0666)
	}
	if err != nil {
		return "", func() {}
	}
	return tmpFile, func() {
		_ = os.RemoveAll(tmpDir)
	}
}

func TestEscapingReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("escapes newlines", func(t *testing.T) {
		er := escapingReader{r: bytes.NewReader([]byte("1\n34"))}
		buf := make([]byte, 5)
		n, err := er.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, []byte{'1', '\\', 'n', '3', '4'}, buf[:n])
	})
	t.Run("escapes carriage returns", func(t *testing.T) {
		er := escapingReader{r: bytes.NewReader([]byte("1\r34"))}
		buf := make([]byte, 5)
		n, err := er.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, []byte{'1', '\\', 'r', '3', '4'}, buf[:n])
	})
	t.Run("escapes tabs", func(t *testing.T) {
		er := escapingReader{r: bytes.NewReader([]byte("1\t34"))}
		buf := make([]byte, 5)
		n, err := er.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, []byte{'1', '\\', 't', '3', '4'}, buf[:n])
	})
	t.Run("escapes backslashes", func(t *testing.T) {
		er := escapingReader{r: bytes.NewReader([]byte("1\\34"))}
		buf := make([]byte, 5)
		n, err := er.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, []byte{'1', '\\', '\\', '3', '4'}, buf[:n])
	})
	t.Run("correctly returns escaped characters that overflow buffer", func(t *testing.T) {
		er := escapingReader{r: bytes.NewReader([]byte("1\n3"))}
		buf := make([]byte, 2)
		n, err := er.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, []byte{'1', '\\'}, buf[:n])
		n, err = er.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, []byte{'n', '3'}, buf[:n])
	})
	t.Run("correctly returns remainer when buffer is larger", func(t *testing.T) {
		er := escapingReader{r: bytes.NewReader([]byte("1\n3"))}
		buf := make([]byte, 2)
		n, err := er.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, []byte{'1', '\\'}, buf[:n])
		buf = make([]byte, 8)
		n, err = er.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, []byte{'n', '3'}, buf[:n])
	})
	t.Run("correctly returns remainder when chunksize is small", func(t *testing.T) {
		oldChunkSize := chunkSize
		defer func() { chunkSize = oldChunkSize }()
		chunkSize = 1
		er := escapingReader{r: bytes.NewReader([]byte("1\n34"))}
		buf := make([]byte, 5)
		n, err := er.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, []byte{'1', '\\', 'n', '3', '4'}, buf[:n])
	})
	t.Run("correctly returns EOF when underlying reader returns it", func(t *testing.T) {
		er := escapingReader{r: bytes.NewReader([]byte("1\n34"))}
		buf := make([]byte, 5)
		n, err := er.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, []byte{'1', '\\', 'n', '3', '4'}, buf[:n])
		_, err = er.Read(buf)
		require.ErrorIs(t, err, io.EOF)
	})
}
