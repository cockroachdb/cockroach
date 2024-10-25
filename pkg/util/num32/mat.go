// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package num32

import (
	"math/rand"

	"github.com/cockroachdb/errors"
	"gonum.org/v1/gonum/blas"
	"gonum.org/v1/gonum/blas/blas32"
	"gonum.org/v1/gonum/blas/blas64"
	"gonum.org/v1/gonum/lapack/lapack64"
)

// MatrixTranspose indicates whether a matrix operation should operate on the
// untransposed or transposed form of the matrix.
type MatrixTranspose byte

const (
	NoTranspose = MatrixTranspose(blas.NoTrans)
	Transpose   = MatrixTranspose(blas.Trans)
)

// Matrix represents an N x M matrix that stores float32 values in row-wise
// format.
type Matrix = blas32.General

// MulMatrixByVector multiplies a matrix (or its transpose) by the "s" vector,
// stores the result in the "dst" vector, and returns "dst". The vectors are
// treated as column matrices:
//
//	matrix = rows x cols
//	s = cols x 1
//	dst = rows x 1 (by rules of matrix multiplication)
//
// Therefore, len(s) must equal matrix.Cols and len(dst) must equal matrix.Rows.
// If Transpose is specified, then the transposed matrix is multiplied by the
// vector. In that case, len(s) must equal matrix.Rows and len(dst) must equal
// matrix.Cols.
func MulMatrixByVector(
	matrix *Matrix, s []float32, dst []float32, transpose MatrixTranspose,
) []float32 {
	blasInput := blas32.Vector{N: len(s), Data: s, Inc: 1}
	blasOutput := blas32.Vector{N: len(dst), Data: dst, Inc: 1}
	blas32.Gemv(blas.Transpose(transpose), 1, *matrix, blasInput, 0, blasOutput)
	return dst
}

// MakeRandomOrthoMatrix returns a square N x N random orthogonal matrix. Rows
// and columns are orthonormal vectors - they have length 1 and any two rows or
// columns are perpendicular. Multiplying a vector by this matrix will randomly
// transform it while preserving its relative distance and angle to any other
// vector that's also multiplied by the same matrix.
func MakeRandomOrthoMatrix(rng *rand.Rand, dims int) Matrix {
	if dims <= 0 {
		panic(errors.AssertionFailedf("matrix cannot have %d dimensions", dims))
	}

	// The BLAS library only supports float64, so do work using float64 and later
	// convert to float32. Start by allocating working memory. Allocate "dims"
	// memory for tau, dims * dims memory for data, and dims * dims / 2 for the
	// working memory that QR decomposition needs.
	mem := make([]float64, dims+dims*dims+(dims*dims+1)/2)
	tau := mem[:dims]
	data := mem[len(tau) : len(tau)+dims*dims]
	work := mem[len(tau)+len(data):]

	for i := range data {
		// Draw random values from the standard normal distribution (mean = 0,
		// stdev = 1).
		data[i] = rng.NormFloat64()
	}
	matrix64 := blas64.General{
		Rows:   dims,
		Cols:   dims,
		Data:   data,
		Stride: dims,
	}

	// Compute the QR decomposition of the random matrix. "Q" is an orthogonal
	// matrix, while "R" will just be discarded.
	lapack64.Geqrf(matrix64, tau, work, len(work))

	// Construct Q from the elementary reflectors.
	lapack64.Orgqr(matrix64, tau, work, len(work))

	// Convert to a float32 matrix.
	matrix32 := Matrix{
		Rows:   dims,
		Cols:   dims,
		Data:   make([]float32, dims*dims),
		Stride: dims,
	}
	for i := range matrix64.Data {
		matrix32.Data[i] = float32(matrix64.Data[i])
	}
	return matrix32
}
