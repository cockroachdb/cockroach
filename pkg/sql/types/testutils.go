// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

// The following variables are useful for testing.
var (
	// OneIntCol is a slice of one IntType.
	OneIntCol = []*T{Int}
	// TwoIntCols is a slice of two IntTypes.
	TwoIntCols = []*T{Int, Int}
	// ThreeIntCols is a slice of three IntTypes.
	ThreeIntCols = []*T{Int, Int, Int}
	// FourIntCols is a slice of four IntTypes.
	FourIntCols = []*T{Int, Int, Int, Int}
)

// MakeIntCols makes a slice of numCols IntTypes.
func MakeIntCols(numCols int) []*T {
	ret := make([]*T, numCols)
	for i := 0; i < numCols; i++ {
		ret[i] = Int
	}
	return ret
}
