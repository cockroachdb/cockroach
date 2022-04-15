// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package targz

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"testing"

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
	gzipWriter := gzip.NewWriter(&tarGzContents)
	_, err = gzipWriter.Write(tarContents.Bytes())
	require.NoError(t, err)
	require.NoError(t, gzipWriter.Close())

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
