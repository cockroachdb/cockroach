// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaload

import "github.com/cockroachdb/redact"

// LoadDimension is an enum of load dimensions corresponding to "real"
// resources that can be subject to rebalancing. This type is also used
// to define capacities.
type LoadDimension uint8

const (
	// CPURate is in nanos per second.
	CPURate LoadDimension = iota
	// WriteBandwidth is the writes in bytes/s.
	WriteBandwidth
	// ByteSize is the size in bytes.
	ByteSize
	NumLoadDimensions
)

func (dim LoadDimension) SafeFormat(w redact.SafePrinter, _ rune) {
	switch dim {
	case CPURate:
		w.Printf("CPURate")
	case WriteBandwidth:
		w.Print("WriteBandwidth")
	case ByteSize:
		w.Printf("ByteSize")
	default:
		panic("unknown LoadDimension")
	}
}

func (dim LoadDimension) String() string {
	return redact.StringWithoutMarkers(dim)
}

// LoadValue is the load on a resource, represented as a *nonnegative* integer.
type LoadValue int64

func (lv LoadValue) Signed() SignedLoadValue {
	return SignedLoadValue{n: int64(lv)}
}

// LoadVector represents a vector of loads, with one element for each resource
// dimension.
type LoadVector [NumLoadDimensions]LoadValue

func (lv LoadVector) String() string {
	return redact.StringWithoutMarkers(lv)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (lv LoadVector) SafeFormat(w redact.SafePrinter, r rune) {
	var v SignedLoadVector
	v.Add(lv)
	v.SafeFormat(w, r)
}

func (lv *LoadVector) Add(other LoadVector) {
	for i := range other {
		(*lv)[i] += other[i]
	}
}

// TODO(tbg): Subtract should be removed; LoadVector must be positive.
func (lv *LoadVector) Subtract(other LoadVector) {
	for i := range other {
		(*lv)[i] -= other[i]
	}
}
