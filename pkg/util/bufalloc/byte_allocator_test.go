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
	// Allocate space that exceeds the max size.
	var buf []byte
	a, buf = a.Alloc(2 * chunkAllocMaxSize)
	require.Equal(t, 2*chunkAllocMaxSize, cap(a.b))
	require.Equal(t, 2*chunkAllocMaxSize, len(buf))
	// We reduce allocation size if the next allocation is below max chunk size.
	a, _ = a.Copy(make([]byte, 1))
	require.Equal(t, chunkAllocMaxSize, cap(a.b))
}
