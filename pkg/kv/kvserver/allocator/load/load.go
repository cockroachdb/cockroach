// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package load

// Load represents a named collection of load dimensions. It is used for
// performing arithmetic and comparison between comparable objects which have
// load.
type Load interface {
	// Dim returns the value of the Dimension given.
	Dim(dim Dimension) float64
	// Greater returns true if for every dim given the calling load is greater
	// than the other element-wise, false otherwise.
	Greater(other Load, dims ...Dimension) bool
	// Less returns true if for every dim given the calling load is less than
	// the other element-wise, false otherwise.
	Less(other Load, dims ...Dimension) bool
	// Sub takes the element-wise subtraction of the calling load with the
	// other element-wise and returns the result.
	Sub(other Load) Load
	// Add takes the element-wise addition of each dimension and returns the
	// result.
	Add(other Load) Load
	// Max takes the element-wise maximum of each dimension and returns the result.
	Max(other Load) Load
	// Min takes the element-wise minimum of each dimension and returns the result.
	Min(other Load) Load
	// ElementWiseProduct multiplies the calling Load with other and returns
	// the result. The multiplication is done entry-wise:
	// ElementWiseProduct([a1,a2,a3], [b1,b2,b3]) = [a1*b1,a2*b2,a3*b3]
	ElementWiseProduct(other Load) Load
	// String returns a string representation of Load.
	String() string
}
