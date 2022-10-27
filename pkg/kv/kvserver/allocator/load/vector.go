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

import (
	"bytes"
	"fmt"
	"math"
)

// Vector is a static container which implements the Load interface.
type Vector [nDimensions]float64

// Dim returns the value of the Dimension given.
func (s Vector) Dim(dim Dimension) float64 {
	if int(dim) > len(s) || dim < 0 {
		panic("Unkown load dimension tried to be accessed")
	}
	return s[dim]
}

// Greater returns true if for every dim given the calling load is greater
// than the other, false otherwise.
func (s Vector) Greater(other Load, dims ...Dimension) bool {
	for _, dim := range dims {
		if s.Dim(dim) <= other.Dim(dim) {
			return false
		}
	}
	return true
}

// Less returns true if for every dim given the calling load is less than
// the other, false otherwise.
func (s Vector) Less(other Load, dims ...Dimension) bool {
	for _, dim := range dims {
		if s.Dim(dim) >= other.Dim(dim) {
			return false
		}
	}
	return true
}

// Sub takes the pairwise subtraction of the calling load with the other
// and returns the result.
func (s Vector) Sub(other Load) Load {
	return s.bimap(other, func(a, b float64) float64 { return a - b })
}

// Add takes the pairwise addition of each dimension and returns the
// result.
func (s Vector) Add(other Load) Load {
	return s.bimap(other, func(a, b float64) float64 { return a + b })
}

// Max takes the pairwise maximum of each dimension and returns the result.
func (s Vector) Max(other Load) Load {
	return s.bimap(other, func(a, b float64) float64 { return math.Max(a, b) })
}

// Min takes the pairwise minimum of each dimension and returns the result.
func (s Vector) Min(other Load) Load {
	return s.bimap(other, func(a, b float64) float64 { return math.Min(a, b) })
}

// Mul multiplies the calling Load with other and returns the result.
func (s Vector) Mul(other Load) Load {
	return s.bimap(other, func(a, b float64) float64 { return a * b })
}

func (s Vector) bimap(other Load, op func(a, b float64) float64) Load {
	mapped := Vector{}
	for i := range s {
		mapped[i] = op(s.Dim(Dimension(i)), other.Dim(Dimension(i)))
	}
	return mapped
}

// String returns a string representation of Load.
func (s Vector) String() string {
	var buf bytes.Buffer
	fmt.Fprint(&buf, "(")
	n := len(s)
	for i, val := range s {
		fmt.Fprintf(&buf, "%s=%.2f", DimensionNames[Dimension(i)], val)
		if i < n-1 {
			fmt.Fprint(&buf, " ")
		}
	}
	fmt.Fprint(&buf, ")")
	return buf.String()
}
