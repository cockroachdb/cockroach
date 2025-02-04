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
	t.Run("Add methods", func(t *testing.T) {
		vs := MakeSet(2)
		require.Equal(t, 2, vs.Dims)
		require.Equal(t, 0, vs.Count)
		require.Equal(t, T{0, 0}, vs.Centroid(T{-1, -1}))

		// Add methods.
		v1 := T{1, 2}
		v2 := T{5, 3}
		v3 := T{6, 6}
		vs.Add(v1)
		vs.Add(v2)
		vs.Add(v3)
		require.Equal(t, 3, vs.Count)
		require.Equal(t, []float32{1, 2, 5, 3, 6, 6}, vs.Data)

		vs.AddSet(vs)
		require.Equal(t, 6, vs.Count)
		require.Equal(t, []float32{1, 2, 5, 3, 6, 6, 1, 2, 5, 3, 6, 6}, vs.Data)

		vs.AddUndefined(2)
		copy(vs.At(6), []float32{3, 1})
		copy(vs.At(7), []float32{4, 4})
		vs.AddUndefined(0)
		require.Equal(t, 8, vs.Count)
		require.Equal(t, []float32{1, 2, 5, 3, 6, 6, 1, 2, 5, 3, 6, 6, 3, 1, 4, 4}, vs.Data)

		vs2 := MakeSetFromRawData([]float32{0, 1, -1, 3}, 2)
		vs2.AddSet(vs)
		require.Equal(t, 10, vs2.Count)
		require.Equal(t, []float32{0, 1, -1, 3, 1, 2, 5, 3, 6, 6, 1, 2, 5, 3, 6, 6, 3, 1, 4, 4}, vs2.Data)
	})

	t.Run("Centroid method", func(t *testing.T) {
		vs := MakeSetFromRawData([]float32{1, 4, 5, 3, 6, 2, 0, 0}, 2)
		require.Equal(t, T{3, 2.25}, vs.Centroid(T{-1, -1}))

		vs2 := T{-10.5}.AsSet()
		require.Equal(t, T{-10.5}, vs2.Centroid(T{-1}))
	})

	t.Run("ReplaceWithLast and Clear methods", func(t *testing.T) {
		vs := MakeSetFromRawData([]float32{1, 2, 5, 3, 6, 6, 1, 2, 5, 3, 6, 6, 3, 1, 4, 4}, 2)

		// ReplaceWithLast.
		vs.ReplaceWithLast(1)
		vs.ReplaceWithLast(4)
		vs.ReplaceWithLast(5)
		require.Equal(t, 5, vs.Count)
		require.Equal(t, []float32{1, 2, 4, 4, 6, 6, 1, 2, 3, 1}, vs.Data)

		// Clear.
		vs.Clear()
		require.Equal(t, 2, vs.Dims)
		require.Equal(t, 0, vs.Count)
	})

	t.Run("EnsureCapacity method", func(t *testing.T) {
		vs := MakeSet(3)
		vs.EnsureCapacity(5)
		require.Equal(t, 0, len(vs.Data))
		require.GreaterOrEqual(t, cap(vs.Data), 15)
		vs.AddUndefined(2)
		copy(vs.At(0), []float32{3, 1, 2})
		copy(vs.At(1), []float32{4, 4, 4})
		require.Equal(t, 2, vs.Count)
		require.Equal(t, 6, len(vs.Data))
	})

	t.Run("AsSet method", func(t *testing.T) {
		vs5 := T{1, 2, 3}.AsSet()
		require.Equal(t, 3, cap(vs5.Data))
	})

	t.Run("SplitAt method", func(t *testing.T) {
		vs := MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
		vs2 := vs.SplitAt(0)
		require.Equal(t, 0, vs.Count)
		require.Equal(t, []float32{}, vs.Data)
		require.Equal(t, 3, vs2.Count)
		require.Equal(t, []float32{1, 2, 3, 4, 5, 6}, vs2.Data)

		// Append to vs and ensure that it does not affect vs2.
		vs.Add([]float32{7, 8})
		require.Equal(t, []float32{1, 2, 3, 4, 5, 6}, vs2.Data)

		vs3 := vs2.SplitAt(2)
		require.Equal(t, 2, vs2.Count)
		require.Equal(t, []float32{1, 2, 3, 4}, vs2.Data)
		require.Equal(t, 1, vs3.Count)
		require.Equal(t, []float32{5, 6}, vs3.Data)

		vs4 := vs2.SplitAt(2)
		require.Equal(t, 2, vs2.Count)
		require.Equal(t, []float32{1, 2, 3, 4}, vs2.Data)
		require.Equal(t, 0, vs4.Count)
		require.Equal(t, []float32{}, vs4.Data)
	})

	t.Run("AsMatrix method", func(t *testing.T) {
		vs := MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
		mat := vs.AsMatrix()
		require.Equal(t, num32.Matrix{Rows: 3, Cols: 2, Stride: 2, Data: vs.Data}, mat)
	})

	t.Run("Clone method", func(t *testing.T) {
		vs := MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
		vs2 := vs.Clone()

		vs.Add(T{0, 1})
		vs.ReplaceWithLast(1)
		add := MakeSetFromRawData([]float32{7, 8, 9, 10}, 2)
		vs2.AddSet(add)
		vs2.ReplaceWithLast(0)

		// Ensure that changes to each did not impact the other.
		require.Equal(t, 3, vs.Count)
		require.Equal(t, []float32{1, 2, 0, 1, 5, 6}, vs.Data)

		require.Equal(t, 4, vs2.Count)
		require.Equal(t, []float32{9, 10, 3, 4, 5, 6, 7, 8}, vs2.Data)
	})

	t.Run("Equal method", func(t *testing.T) {
		vs := MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
		vs2 := MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
		vs3 := vs.Clone()
		require.True(t, vs.Equal(&vs))
		require.True(t, vs.Equal(&vs2))
		require.True(t, vs.Equal(&vs3))

		vs.Add(T{7, 8})
		require.False(t, vs.Equal(&vs2))
		require.True(t, vs.Equal(&vs))
		vs2.Add(T{7, 8})
		require.True(t, vs.Equal(&vs2))
		vs.ReplaceWithLast(1)
		require.False(t, vs.Equal(&vs2))
		require.True(t, vs.Equal(&vs))
		vs2.ReplaceWithLast(1)
		require.True(t, vs.Equal(&vs2))
	})

	t.Run("check that invalid operations will panic", func(t *testing.T) {
		vs := MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
		require.Panics(t, func() { vs.At(-1) })
		require.Panics(t, func() { vs.SplitAt(-1) })
		require.Panics(t, func() { vs.AddUndefined(-1) })
		require.Panics(t, func() { vs.ReplaceWithLast(-1) })
		require.Panics(t, func() { vs.Centroid([]float32{0, 0, 0}) })

		vs2 := MakeSet(2)
		require.Panics(t, func() { vs2.At(0) })
		require.Panics(t, func() { vs2.SplitAt(1) })
		require.Panics(t, func() { vs2.ReplaceWithLast(0) })

		vs3 := MakeSet(-1)
		require.Panics(t, func() { vs3.Add(vs.At(0)) })
		require.Panics(t, func() { vs3.AddUndefined(1) })
	})
}
