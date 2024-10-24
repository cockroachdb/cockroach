// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vector

import (
	"slices"

	"github.com/cockroachdb/errors"
)

// MakeSet constructs a new empty vector set with the given number of
// dimensions. New vectors can be added via the Add or AddSet methods.
func MakeSet(dims int) Set {
	return Set{Dims: dims}
}

// MakeSetFromRawData constructs a new vector set from a raw slice of vectors.
// The vectors in the slice have the given number of dimensions and are laid out
// contiguously in row-wise order.
// NB: The data slice is directly used rather than copied; any outside changes
// to it will be reflected in the vector set.
func MakeSetFromRawData(data []float32, dims int) Set {
	if len(data)%dims != 0 {
		panic(errors.AssertionFailedf(
			"data length %d is not a multiple of %d dimensions", len(data), dims))
	}
	return Set{
		Dims:  dims,
		Count: len(data) / dims,
		Data:  data,
	}
}

// At returns the vector at the given offset in the set.
func (vs *Set) At(offset int) T {
	start := offset * vs.Dims
	end := start + vs.Dims
	return vs.Data[start:end:end]
}

// SplitAt divides the vector set into two subsets at the given offset. This
// vector set is updated to contain only the vectors before the split point, and
// the returned set contains only the vectors on or after the split point.
func (vs *Set) SplitAt(offset int) Set {
	if offset > vs.Count {
		panic(errors.AssertionFailedf(
			"split point %d cannot be greater than set size %d", offset, vs.Count))
	}

	other := Set{
		Dims:  vs.Dims,
		Count: vs.Count - offset,
		Data:  vs.Data[offset:],
	}

	// Specify capacity of the slice so that it's safe to add vectors to this
	// set without impacting the returned set.
	vs.Data = vs.Data[:offset:offset]
	return other
}

// Add appends a new vector to the set.
func (vs *Set) Add(v T) {
	if vs.Dims != len(v) {
		panic(errors.AssertionFailedf(
			"cannot add vector with %d dimensions to a set with %d dimensions", len(v), vs.Dims))
	}
	vs.Count++
	vs.Data = append(vs.Data, v...)
}

// AddSet appends all vectors from the given set to this set.
func (vs *Set) AddSet(vectors *Set) {
	if vs.Dims != vectors.Dims {
		panic(errors.AssertionFailedf(
			"cannot add vector set with %d dimensions to a set with %d dimensions",
			vectors.Dims, vs.Dims))
	}
	vs.Count += vectors.Count
	vs.Data = append(vs.Data, vectors.Data...)
}

// AddZero adds the given count of zero vectors to this set.
func (vs *Set) AddZero(count int) {
	vs.Count += count
	vs.Data = slices.Grow(vs.Data, count*vs.Dims)
	vs.Data = vs.Data[:vs.Count*vs.Dims]
}

// Remove removes the vector at the given offset from the set. This is an O(1)
// operation.
// NB: This operation changes the ordering of vectors in the set, with the last
// vector moved to the removed vector's offset.
func (vs *Set) Remove(offset int) {
	targetStart := offset * vs.Dims
	sourceEnd := len(vs.Data)
	copy(vs.Data[targetStart:targetStart+vs.Dims], vs.Data[sourceEnd-vs.Dims:sourceEnd])
	vs.Data = vs.Data[:sourceEnd-vs.Dims]
	vs.Count--
}

// EnsureCapacity grows the underlying data slice if needed to ensure the
// requested capacity. This is useful to prevent unnecessary resizing when it's
// known up-front how big the vector set will need to get.
func (vs *Set) EnsureCapacity(capacity int) {
	if vs.Count < capacity {
		vs.Data = slices.Grow(vs.Data, (capacity-vs.Count)*vs.Dims)
	}
}
