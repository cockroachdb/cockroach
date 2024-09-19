// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package assetbundle

import (
	"archive/tar"
	"bytes"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestAsFS(t *testing.T) {
	var tarContents bytes.Buffer
	tarWriter := tar.NewWriter(&tarContents)
	aContents := "razzledazzle"
	err := tarWriter.WriteHeader(&tar.Header{Name: "a", Size: int64(len(aContents))})
	require.NoError(t, err)
	_, err = tarWriter.Write([]byte(aContents))
	require.NoError(t, err)

	bContents := "foobar"
	err = tarWriter.WriteHeader(&tar.Header{Name: "b", Size: int64(len(bContents))})
	require.NoError(t, err)
	_, err = tarWriter.Write([]byte(bContents))
	require.NoError(t, err)

	cdTxtContents := "lorem ipsum"
	err = tarWriter.WriteHeader(&tar.Header{Name: "c/d.txt", Size: int64(len(cdTxtContents))})
	require.NoError(t, err)
	_, err = tarWriter.Write([]byte(cdTxtContents))
	require.NoError(t, err)

	require.NoError(t, tarWriter.Close())
	var tarGzContents bytes.Buffer
	writer, _ := zstd.NewWriter(&tarGzContents)
	_, err = writer.Write(tarContents.Bytes())
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	fs, err := AsFS(bytes.NewBuffer(tarGzContents.Bytes()))
	require.NoError(t, err)

	aFile, err := fs.Open("a")
	require.NoError(t, err)
	aFileStat, err := aFile.Stat()
	require.NoError(t, err)
	require.Equal(t, aFileStat.Size(), int64(len(aContents)))
	var aFileContents bytes.Buffer
	_, err = io.Copy(&aFileContents, aFile)
	require.NoError(t, err)
	require.Equal(t, aFileContents.String(), aContents)

	bFile, err := fs.Open("b")
	require.NoError(t, err)
	bFileStat, err := bFile.Stat()
	require.NoError(t, err)
	require.Equal(t, bFileStat.Size(), int64(len(bContents)))
	var bFileContents bytes.Buffer
	_, err = io.Copy(&bFileContents, bFile)
	require.NoError(t, err)
	require.Equal(t, bFileContents.String(), bContents)

	cdTxtFile, err := fs.Open("c/d.txt")
	require.NoError(t, err)
	cdTxtStat, err := cdTxtFile.Stat()
	require.NoError(t, err)
	require.Equal(t, cdTxtStat.Size(), int64(len(cdTxtContents)))
	var cdTxtFileContents bytes.Buffer
	_, err = io.Copy(&cdTxtFileContents, cdTxtFile)
	require.NoError(t, err)
	require.Equal(t, cdTxtFileContents.String(), cdTxtContents)
}
