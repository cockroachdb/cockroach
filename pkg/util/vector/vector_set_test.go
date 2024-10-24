// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vector

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVectorSet(t *testing.T) {
	vs := MakeSet(2)
	require.Equal(t, 2, vs.Dims)
	require.Equal(t, 0, vs.Count)

	// Add methods.
	v1 := T{1, 2}
	v2 := T{5, 3}
	v3 := T{6, 6}
	vs.Add(v1)
	vs.Add(v2)
	vs.Add(v3)
	require.Equal(t, 3, vs.Count)
	require.Equal(t, []float32{1, 2, 5, 3, 6, 6}, vs.Data)

	vs.AddSet(&vs)
	require.Equal(t, 6, vs.Count)
	require.Equal(t, []float32{1, 2, 5, 3, 6, 6, 1, 2, 5, 3, 6, 6}, vs.Data)

	vs.AddZero(2)
	vs.AddZero(0)
	require.Equal(t, 8, vs.Count)
	require.Equal(t, []float32{1, 2, 5, 3, 6, 6, 1, 2, 5, 3, 6, 6, 0, 0, 0, 0}, vs.Data)

	// Remove.
	vs.Remove(1)
	vs.Remove(4)
	require.Equal(t, 6, vs.Count)
	require.Equal(t, []float32{1, 2, 0, 0, 6, 6, 1, 2, 0, 0, 6, 6}, vs.Data)

	vs3 := MakeSetFromRawData(vs.Data, 2)
	require.Equal(t, vs, vs3)

	// Ensure capacity.
	vs4 := MakeSet(3)
	vs4.EnsureCapacity(5)
	require.Equal(t, 0, len(vs4.Data))
	require.GreaterOrEqual(t, cap(vs4.Data), 15)
	vs4.AddZero(2)
	require.Equal(t, 2, vs4.Count)
	require.Equal(t, 6, len(vs4.Data))

	// AsSet.
	vs5 := T{1, 2, 3}.AsSet()
	require.Equal(t, 3, cap(vs5.Data))
	vs4.AddSet(&vs5)
	require.Equal(t, 3, vs4.Count)
	require.Equal(t, []float32{0, 0, 0, 0, 0, 0, 1, 2, 3}, vs4.Data)
}
