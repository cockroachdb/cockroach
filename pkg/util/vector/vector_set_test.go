// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vector

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/num32"
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

	// ReplaceWithLast.
	vs.ReplaceWithLast(1)
	vs.ReplaceWithLast(4)
	vs.ReplaceWithLast(5)
	require.Equal(t, 5, vs.Count)
	require.Equal(t, []float32{1, 2, 0, 0, 6, 6, 1, 2, 0, 0}, vs.Data)

	// Add zero again, to ensure that reusing memory still zeroes it.
	vs.AddZero(1)
	require.Equal(t, []float32{1, 2, 0, 0, 6, 6, 1, 2, 0, 0, 0, 0}, vs.Data)

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

	// SplitAt.
	vs6 := MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
	vs7 := vs6.SplitAt(0)
	require.Equal(t, 0, vs6.Count)
	require.Equal(t, []float32{}, vs6.Data)
	require.Equal(t, 3, vs7.Count)
	require.Equal(t, []float32{1, 2, 3, 4, 5, 6}, vs7.Data)

	// Append to vs6 and ensure that it does not affect vs7.
	vs6.Add([]float32{7, 8})
	require.Equal(t, []float32{1, 2, 3, 4, 5, 6}, vs7.Data)

	vs8 := vs7.SplitAt(2)
	require.Equal(t, 2, vs7.Count)
	require.Equal(t, []float32{1, 2, 3, 4}, vs7.Data)
	require.Equal(t, 1, vs8.Count)
	require.Equal(t, []float32{5, 6}, vs8.Data)

	vs9 := vs7.SplitAt(2)
	require.Equal(t, 2, vs7.Count)
	require.Equal(t, []float32{1, 2, 3, 4}, vs7.Data)
	require.Equal(t, 0, vs9.Count)
	require.Equal(t, []float32{}, vs9.Data)

	// AsMatrix.
	vs10 := MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
	mat := vs10.AsMatrix()
	require.Equal(t, num32.Matrix{Rows: 3, Cols: 2, Stride: 2, Data: vs10.Data}, mat)

	// Check that invalid operations will panic.
	vs11 := MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
	require.Panics(t, func() { vs11.At(-1) })
	require.Panics(t, func() { vs11.SplitAt(-1) })
	require.Panics(t, func() { vs11.AddZero(-1) })
	require.Panics(t, func() { vs11.AddSet(nil) })
	require.Panics(t, func() { vs11.ReplaceWithLast(-1) })

	vs12 := MakeSet(2)
	require.Panics(t, func() { vs12.At(0) })
	require.Panics(t, func() { vs12.SplitAt(1) })
	require.Panics(t, func() { vs12.ReplaceWithLast(0) })

	vs13 := MakeSet(-1)
	require.Panics(t, func() { vs13.Add(v1) })
	require.Panics(t, func() { vs13.AddZero(1) })
}
