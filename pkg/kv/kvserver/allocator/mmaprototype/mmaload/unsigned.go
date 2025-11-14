// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaload

import "github.com/cockroachdb/redact"

// LoadDimension is an enum of load dimensions corresponding to "real"
// resources. Such resources can sometimes have a capacity. It is generally
// important to rebalance based on these. The code in this package should be
// structured that adding additional resource dimensions is easy.
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

// LoadValue is the load on a resource.
type LoadValue int64

// LoadVector represents a vector of loads, with one element for each resource
// dimension.
type LoadVector [NumLoadDimensions]LoadValue

func (lv LoadVector) String() string {
	return redact.StringWithoutMarkers(lv)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (lv LoadVector) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("[cpu:%d, write-bandwidth:%d, byte-size:%d]", lv[CPURate], lv[WriteBandwidth], lv[ByteSize])
}

func (lv *LoadVector) Add(other LoadVector) {
	for i := range other {
		(*lv)[i] += other[i]
	}
}

func (lv *LoadVector) Subtract(other LoadVector) {
	for i := range other {
		(*lv)[i] -= other[i]
	}
}
