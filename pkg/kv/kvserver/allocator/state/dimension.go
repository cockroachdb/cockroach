// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package state

import (
	"bytes"
	"fmt"
	"math"
)

// LoadDimension is a singe dimension of load that a component may track.
type LoadDimension int

const (
	// RangeCountDimension refers to the number of ranges.
	RangeCountDimension LoadDimension = iota
	// LeaseCountDimension refers to the number of leases.
	LeaseCountDimension
	// QueriesDimension refers to the number of queries.
	QueriesDimension
	// WriteKeysDimension refers to the number of keys written.
	WriteKeysDimension
	// StorageDimension refers to the number of bytes stored persistently.
	StorageDimension
	// CPUTimeDimension refers to the amount of cpu time used.
	CPUTimeDimension
)

// LoadDimensionNames contains a mapping of a load dimension, to a human
// readable string.
var LoadDimensionNames = map[LoadDimension]string{
	RangeCountDimension: "range-count",
	LeaseCountDimension: "lease-count",
	QueriesDimension:    "queries-per-second",
	WriteKeysDimension:  "write-keys-per-second",
	StorageDimension:    "disk-storage",
	CPUTimeDimension:    "cpu-time",
}

// Load represents a named collection of load dimensions. It is used for
// performing arithmetic and comparison between comparable objects which have
// load.
type Load interface {
	// Dim returns the value of the LoadDimension given.
	Dim(dim LoadDimension) float64
	// Greater returns true if for every dim given the calling load is greater
	// than the other, false otherwise.
	Greater(other Load, dims ...LoadDimension) bool
	// Less returns true if for every dim given the calling load is less than
	// the other, false otherwise.
	Less(other Load, dims ...LoadDimension) bool
	// Sub takes the pairwise subtraction of the calling load with the other
	// and returns the result.
	Sub(other Load) Load
	// Add takes the pairwise addition of each dimension and returns the
	// result.
	Add(other Load) Load
	// Max takes the pairwise maximum of each dimension and returns the result.
	Max(other Load) Load
	// Min takes the pairwise minimum of each dimension and returns the result.
	Min(other Load) Load
	// Mul multiplies the calling Load with other and returns the result.
	Mul(other Load) Load
	// String returns a string representation of Load.
	String() string
}

// StaticDimensionContainer is a static container which implements the Load
// interface.
type StaticDimensionContainer [6]float64

// Dim returns the value of the LoadDimension given.
func (s StaticDimensionContainer) Dim(dim LoadDimension) float64 {
	if int(dim) > len(s) || dim < 0 {
		panic("Unkown load dimension tried to be accessed")
	}
	return s[dim]
}

// Greater returns true if for every dim given the calling load is greater
// than the other, false otherwise.
func (s StaticDimensionContainer) Greater(other Load, dims ...LoadDimension) bool {
	for _, dim := range dims {
		if s.Dim(dim) <= other.Dim(dim) {
			return false
		}
	}
	return true
}

// Less returns true if for every dim given the calling load is less than
// the other, false otherwise.
func (s StaticDimensionContainer) Less(other Load, dims ...LoadDimension) bool {
	for _, dim := range dims {
		if s.Dim(dim) >= other.Dim(dim) {
			return false
		}
	}
	return true
}

// Sub takes the pairwise subtraction of the calling load with the other
// and returns the result.
func (s StaticDimensionContainer) Sub(other Load) Load {
	return s.bimap(other, func(a, b float64) float64 { return a - b })
}

// Add takes the pairwise addition of each dimension and returns the
// result.
func (s StaticDimensionContainer) Add(other Load) Load {
	return s.bimap(other, func(a, b float64) float64 { return a + b })
}

// Max takes the pairwise maximum of each dimension and returns the result.
func (s StaticDimensionContainer) Max(other Load) Load {
	return s.bimap(other, func(a, b float64) float64 { return math.Max(a, b) })
}

// Min takes the pairwise minimum of each dimension and returns the result.
func (s StaticDimensionContainer) Min(other Load) Load {
	return s.bimap(other, func(a, b float64) float64 { return math.Min(a, b) })
}

// Mul multiplies the calling Load with other and returns the result.
func (s StaticDimensionContainer) Mul(other Load) Load {
	return s.bimap(other, func(a, b float64) float64 { return a * b })
}

func (s StaticDimensionContainer) bimap(other Load, op func(a, b float64) float64) Load {
	mapped := StaticDimensionContainer{}
	for i := range s {
		mapped[i] = op(s.Dim(LoadDimension(i)), other.Dim(LoadDimension(i)))
	}
	return mapped
}

// String returns a string representation of Load.
func (s StaticDimensionContainer) String() string {
	var buf bytes.Buffer
	fmt.Fprint(&buf, "(")
	for i, val := range s {
		fmt.Fprintf(&buf, "%s=%.2f ", LoadDimensionNames[LoadDimension(i)], val)
	}
	fmt.Fprint(&buf, ")")
	return buf.String()
}
