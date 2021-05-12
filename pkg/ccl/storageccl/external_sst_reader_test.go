// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestSSTReaderCache(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var openCalls, expectedOpenCalls int
	const sz, suffix = 100, 10
	raw := &sstReader{
		sz:   sizeStat(sz),
		body: ioutil.NopCloser(bytes.NewReader(nil)),
		openAt: func(offset int64) (io.ReadCloser, error) {
			openCalls++
			return ioutil.NopCloser(bytes.NewReader(make([]byte, sz-int(offset)))), nil
		},
	}

	require.Equal(t, 0, openCalls)
	_ = raw.readAndCacheSuffix(suffix)
	expectedOpenCalls++

	discard := make([]byte, 5)

	// Reading in the suffix doesn't make another call.
	_, _ = raw.ReadAt(discard, 90)
	require.Equal(t, expectedOpenCalls, openCalls)

	// Reading in the suffix again doesn't make another call.
	_, _ = raw.ReadAt(discard, 95)
	require.Equal(t, expectedOpenCalls, openCalls)

	// Reading outside the suffix makes a new call.
	_, _ = raw.ReadAt(discard, 85)
	expectedOpenCalls++
	require.Equal(t, expectedOpenCalls, openCalls)

	// Reading at same offset, outside the suffix, does make a new call to rewind.
	_, _ = raw.ReadAt(discard, 85)
	expectedOpenCalls++
	require.Equal(t, expectedOpenCalls, openCalls)

	// Read at new pos does makes a new call.
	_, _ = raw.ReadAt(discard, 0)
	expectedOpenCalls++
	require.Equal(t, expectedOpenCalls, openCalls)

	// Read at cur pos (where last read stopped) does not reposition.
	_, _ = raw.ReadAt(discard, 5)
	require.Equal(t, expectedOpenCalls, openCalls)

	// Read at in suffix between non-suffix reads does not make a call.
	_, _ = raw.ReadAt(discard, 92)
	require.Equal(t, expectedOpenCalls, openCalls)

	// Read at where prior non-suffix read finished does not make a new call.
	_, _ = raw.ReadAt(discard, 10)
	require.Equal(t, expectedOpenCalls, openCalls)
}
