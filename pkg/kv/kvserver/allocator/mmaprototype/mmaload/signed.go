// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaload

import "github.com/cockroachdb/redact"

type SignedLoadValue struct {
	n int64
}

func (s SignedLoadValue) Subtract(lv LoadValue) SignedLoadValue {
	return SignedLoadValue{n: s.n - int64(lv)}
}

func (s SignedLoadValue) Abs() int64 {
	return max(s.n, -s.n)
}

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

type SignedLoadVector [NumLoadDimensions]SignedLoadValue

func (v *SignedLoadVector) Add(o LoadVector) {
	for i := 0; i < int(NumLoadDimensions); i++ {
		(*v)[i].n += int64(o[i])
	}
}

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
