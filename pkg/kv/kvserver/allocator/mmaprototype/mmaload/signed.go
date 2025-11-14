// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaload

import "github.com/cockroachdb/redact"

// SignedLoadValue is a load value that could become negative. It intentionally
// does not expose its integer value directly to reduce the likelihood of
// sign-unaware computation.
type SignedLoadValue struct {
	n int64
}

// Subtract returns a SignedLoadValue representing the difference between
// the receiver and the argument.
func (s SignedLoadValue) Subtract(lv LoadValue) SignedLoadValue {
	return SignedLoadValue{n: s.n - int64(lv)}
}

func (s SignedLoadValue) Abs() int64 {
	return max(s.n, -s.n)
}

// Nonnegative returns the wrapped value as a LoadValue if it is non-negative,
// and zero otherwise.
func (s SignedLoadValue) Nonnegative() LoadValue {
	if s.n > 0 {
		return LoadValue(s.n)
	}
	return 0
}

func (s SignedLoadValue) String() string {
	return redact.StringWithoutMarkers(s)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (s SignedLoadValue) SafeFormat(w redact.SafePrinter, _ rune) {
	w.SafeInt(redact.SafeInt(s.n))
}

// SignedLoadVector is a load vector that could have negative values.
type SignedLoadVector [NumLoadDimensions]SignedLoadValue

// Add updates the vector by adding the given LoadVector to it.
func (v *SignedLoadVector) Add(o LoadVector) {
	for i := 0; i < int(NumLoadDimensions); i++ {
		(*v)[i].n += int64(o[i])
	}
}

// Subtract updates the vector by subtracting the given LoadVector from it.
func (v *SignedLoadVector) Subtract(o LoadVector) {
	for i := 0; i < int(NumLoadDimensions); i++ {
		(*v)[i].n -= int64(o[i])
	}
}

// NonnegativeAt returns the load value at the given dimension if it is
// non-negative, and zero otherwise. The boolean indicates whether the returned
// load is strictly positive.
func (v *SignedLoadVector) NonnegativeAt(dim LoadDimension) (LoadValue, bool) {
	val := (*v)[int(dim)].n
	return LoadValue(max(0, val)), val > 0
}

// UnwrapAt returns the integer value for the given dimension. This should be
// used sparingly. The boolean indicates whether the returned load is strictly
// positive.
func (v *SignedLoadVector) UnwrapAt(dim LoadDimension) (_ int64, positive bool) {
	val := (*v)[int(dim)].n
	return val, val > 0
}

// Nonnegative returns a LoadVector which has zeroes for any dimension for which
// the signed load is negative.
func (v *SignedLoadVector) Nonnegative() LoadVector {
	var lv LoadVector
	for i := 0; i < int(NumLoadDimensions); i++ {
		if val := (*v)[i].n; val > 0 {
			lv[i] = LoadValue(val)
		}
	}
	return lv
}

func (v SignedLoadVector) String() string {
	return redact.StringWithoutMarkers(v)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (v SignedLoadVector) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("[cpu:%d, write-bandwidth:%d, byte-size:%d]", v[CPURate], v[WriteBandwidth], v[ByteSize])
}
