// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workspace

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkspace(t *testing.T) {
	var workspace T

	// Test alloc/free vectors.
	vectors := workspace.AllocVectorSet(3, 2)
	require.Equal(t, 3, vectors.Count)
	require.Equal(t, 2, vectors.Dims)
	require.Equal(t, 6, len(vectors.Data))
	require.Equal(t, 6, len(workspace.floatStack))

	// Test alloc/free floats.
	floats := workspace.AllocFloats(5)
	require.Equal(t, 5, len(floats))
	require.Equal(t, 11, len(workspace.floatStack))

	workspace.FreeFloats(floats)
	require.Equal(t, 6, len(workspace.floatStack))
	workspace.FreeVectorSet(vectors)
	require.Equal(t, 0, len(workspace.floatStack))

	// Test alloc/free uint64s.
	uint64s := workspace.AllocUint64s(4)
	require.Equal(t, 4, len(uint64s))
	require.Equal(t, 4, len(workspace.uint64Stack))
	workspace.FreeUint64s(uint64s)
	require.Equal(t, 0, len(workspace.uint64Stack))
}
