// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package num32

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// This is just a pass-through function, so just need to test wrapper.
func TestMulMatrixByVector(t *testing.T) {
	// Don't transpose matrix:
	//   1 2
	//   3 4
	//   5 6
	matrix := Matrix{Rows: 3, Cols: 2, Stride: 2, Data: []float32{1, 2, 3, 4, 5, 6}}
	vector := []float32{7, 8}
	output := []float32{0, 0, 0}
	ret := MulMatrixByVector(&matrix, vector, output, NoTranspose)
	require.Equal(t, []float32{23, 53, 83}, output)
	require.Equal(t, output, ret)

	// Transpose matrix:
	//   1 3 5
	//   2 4 6
	vectorT := []float32{7, 8, 9}
	outputT := []float32{0, 0}
	MulMatrixByVector(&matrix, vectorT, outputT, Transpose)
	require.Equal(t, []float32{76, 100}, outputT)
}

func TestMakeRandOrthoMatrix(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	// Zero-length matrix.
	require.Panics(t, func() { MakeRandomOrthoMatrix(rng, 0) })

	// 1x1 matrix.
	matrix := MakeRandomOrthoMatrix(rng, 1)
	require.InDelta(t, float32(1), matrix.Data[0], 0.0001)

	// Generate 10 random orthogonal 4x4 matrices and verify expected properties.
	for range 10 {
		matrix = MakeRandomOrthoMatrix(rng, 4)

		// Validate that each row vector has length 1.
		for i := range matrix.Rows {
			offset := i * matrix.Cols
			row := matrix.Data[offset : offset+matrix.Cols]
			require.InDelta(t, float32(1), Dot(row, row), 0.0001)
		}

		// Validate that each column vector has length 1.
		for i := range matrix.Cols {
			var norm float32
			for j := range matrix.Rows {
				val := matrix.Data[j*matrix.Cols+i]
				norm += val * val
			}
			require.InDelta(t, float32(1), norm, 0.0001)
		}

		// Check that first two rows and columns are perpendicular by computing
		// dot products (perpendicular vectors have dot product of zero).
		dot := Dot(matrix.Data[:matrix.Cols], matrix.Data[matrix.Cols:matrix.Cols*2])
		require.InDelta(t, float32(0), dot, 0.0001)

		dot = 0
		for i := range matrix.Rows {
			dot += matrix.Data[i*matrix.Cols] * matrix.Data[i*matrix.Cols+1]
		}
		require.InDelta(t, float32(0), dot, 0.0001)

		// Test that distance is preserved by the random orthogonal transformation.
		v1 := MulMatrixByVector(&matrix, []float32{1, 2, 3, 4}, []float32{0, 0, 0, 0}, NoTranspose)
		v2 := MulMatrixByVector(&matrix, []float32{8, 7, 6, 5}, []float32{0, 0, 0, 0}, NoTranspose)
		dist := L2SquaredDistance(v1, v2)
		require.InDelta(t, float32(84), dist, 0.0001)
	}
}

func BenchmarkMulMatrixNoTranspose(b *testing.B) {
	benchmarkMul(b, NoTranspose)
}

func BenchmarkMulMatrixTranspose(b *testing.B) {
	benchmarkMul(b, Transpose)
}

func benchmarkMul(b *testing.B, transpose MatrixTranspose) {
	rng := rand.New(rand.NewSource(42))
	data := Matrix{Rows: 512, Cols: 512, Stride: 512, Data: make([]float32, 512*512)}
	for i := range data.Data {
		data.Data[i] = float32(rng.NormFloat64())
	}
	vector := data.Data[:512]
	output := make([]float32, 512)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		MulMatrixByVector(&data, vector, output, transpose)
	}
}
