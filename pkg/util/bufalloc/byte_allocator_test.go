// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bufalloc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestByteAllocator(t *testing.T) {
	var a ByteAllocator
	// Must always allocate at least chunkAllocMinSize.
	a, _ = a.Copy([]byte{1})
	require.Equal(t, chunkAllocMinSize, cap(a.b))
	// Must use already allocated capacity.
	a, _ = a.Copy([]byte{2})
	require.Equal(t, 2, len(a.b))
	require.Equal(t, chunkAllocMinSize, cap(a.b))
	// Allocated capacity grows exponentially.
	a, _ = a.Copy(make([]byte, chunkAllocMinSize))
	require.Equal(t, 2*chunkAllocMinSize, cap(a.b))
	// The requested capacity is always allocated, skiping some exponential
	// steps if necessary.
	a, _ = a.Copy(make([]byte, chunkAllocMaxSize))
	require.Equal(t, chunkAllocMaxSize, cap(a.b))
	// Once the maximum allocation size is reached, we never allocate less.
	a, _ = a.Copy(make([]byte, 1))
	require.Equal(t, chunkAllocMaxSize, cap(a.b))
	// Ensure that Copy2 encounters only a single allocation, regardless of the
	// total size.
	src1, src2 := make([]byte, chunkAllocMaxSize), make([]byte, chunkAllocMaxSize)
	for i := range src1 {
		src1[i] = 1
		src2[i] = 2
	}
	var dst1, dst2 []byte
	a, dst1, dst2 = a.Copy2(src1, src2)
	require.Equal(t, 2*chunkAllocMaxSize, cap(a.b))
	require.Equal(t, src1, dst1)
	require.Equal(t, src2, dst2)
	// We reduce allocation size if the next allocation is below max chunk size.
	a, _ = a.Copy(make([]byte, 1))
	require.Equal(t, chunkAllocMaxSize, cap(a.b))
}
