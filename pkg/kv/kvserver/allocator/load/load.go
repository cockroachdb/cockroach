// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import (
	"math"

	"github.com/cockroachdb/redact"
)

// Load represents a named collection of load dimensions. It is used for
// performing arithmetic and comparison between comparable objects which have
// load.
type Load interface {
	redact.SafeFormatter
	// Dim returns the value of the Dimension given.
	Dim(dim Dimension) float64
	// String returns a string representation of Load.
	String() string
}

// Greater returns true if for every dim given in a is greater than the dim in
// b element-wise, false otherwise. Unspecified dimensions are ignored.
func Greater(a, b Load, dims ...Dimension) bool {
	for _, dim := range dims {
		if a.Dim(dim) <= b.Dim(dim) {
			return false
		}
	}
	return true
}

// Less returns true if for every dim given in a is less than the dim in b
// element-wise, false otherwise. Unspecified dimensions are ignored.
func Less(a, b Load, dims ...Dimension) bool {
	for _, dim := range dims {
		if a.Dim(dim) >= b.Dim(dim) {
			return false
		}
	}
	return true
}

// Sub takes the element-wise subtraction of the calling load with the
// other element-wise and returns the result.
func Sub(a, b Load) Load {
	return bimap(a, b, func(ai, bi float64) float64 { return ai - bi })
}

// Add takes the element-wise addition of each dimension and returns the
// result.
func Add(a, b Load) Load {
	return bimap(a, b, func(ai, bi float64) float64 { return ai + bi })
}

// Max takes the element-wise maximum of each dimension and returns the result.
func Max(a, b Load) Load {
	return bimap(a, b, func(ai, bi float64) float64 { return math.Max(ai, bi) })
}

// Min takes the element-wise minimum of each dimension and returns the result.
func Min(a, b Load) Load {
	return bimap(a, b, func(ai, bi float64) float64 { return math.Min(ai, bi) })
}

// ElementWiseProduct multiplies the calling Load with other and returns
// the result. The multiplication is done element-wise:
// ElementWiseProduct([a1,a2,a3], [b1,b2,b3]) = [a1*b1,a2*b2,a3*b3]
func ElementWiseProduct(a, b Load) Load {
	return bimap(a, b, func(ai, bi float64) float64 { return ai * bi })
}

// Scale applies the factor given against every dimension.
func Scale(l Load, factor float64) Load {
	return nmap(l, func(_ Dimension, li float64) float64 { return li * factor })
}

// Set returns a new Load with every dimension equal to the value given.
func Set(val float64) Load {
	l := Vector{}
	return nmap(l, func(_ Dimension, li float64) float64 { return val })
}

func bimap(a, b Load, op func(ai, bi float64) float64) Load {
	mapped := Vector{}
	for dim := Dimension(0); dim < Dimension(nDimensions); dim++ {
		mapped[dim] = op(a.Dim(dim), b.Dim(dim))
	}
	return mapped
}

func nmap(l Load, op func(d Dimension, li float64) float64) Load {
	mapped := Vector{}
	for dim := Dimension(0); dim < Dimension(nDimensions); dim++ {
		mapped[dim] = op(dim, l.Dim(dim))
	}
	return mapped
}
