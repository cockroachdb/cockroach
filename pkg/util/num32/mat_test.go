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
	MulMatrixByVector(&matrix, vector, output, NoTranspose)
	require.Equal(t, []float32{23, 53, 83}, output)

	// Transpose matrix:
	//   1 3 5
	//   2 4 6
	vectorT := []float32{7, 8, 9}
	outputT := []float32{0, 0}
	MulMatrixByVector(&matrix, vectorT, outputT, Transpose)
	require.Equal(t, []float32{76, 100}, outputT)
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
