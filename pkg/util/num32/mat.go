// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package num32

import (
	"gonum.org/v1/gonum/blas"
	"gonum.org/v1/gonum/blas/blas32"
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

// MulMatrixByVector multiplies a matrix (or its transpose) by the "s" vector
// and stores the result in the "dst" vector. The vectors are treated as column
// matrices:
//
//	matrix = rows x cols
//	s = cols x 1
//	dst = rows x 1 (by rules of matrix multiplication)
//
// Therefore, len(s) must equal matrix.Cols and len(dst) must equal matrix.Rows.
// If Transpose is specified, then the transposed matrix is multiplied by the
// vector. In that case, len(s) must equal matrix.Rows and len(dst) must equal
// matrix.Cols.
func MulMatrixByVector(matrix *Matrix, s []float32, dst []float32, transpose MatrixTranspose) {
	blasInput := blas32.Vector{N: len(s), Data: s, Inc: 1}
	blasOutput := blas32.Vector{N: len(dst), Data: dst, Inc: 1}
	blas32.Gemv(blas.Transpose(transpose), 1, *matrix, blasInput, 0, blasOutput)
}
