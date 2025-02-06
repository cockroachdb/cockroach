// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vector

import (
	"slices"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
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
// NB: The data slice is directly used rather than copied; do not use it outside
// the context of this vector set after this point.
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

// Clone performs a deep copy of the vector set. Changes to the original or
// clone will not affect the other.
func (vs *Set) Clone() Set {
	return Set{
		Dims:  vs.Dims,
		Count: vs.Count,
		Data:  slices.Clone(vs.Data),
	}
}

// AsMatrix returns this set as a matrix, to be used with the num32 package.
// NB: The underlying float32 slice is shared by the resulting matrix, so any
// changes will affect both the vector set and matrix.
func (vs *Set) AsMatrix() num32.Matrix {
	return num32.Matrix{
		Rows:   vs.Count,
		Cols:   vs.Dims,
		Stride: vs.Dims,
		Data:   vs.Data,
	}
}

// At returns the vector at the given offset in the set. The returned vector is
// intended for transient use, since mutations to the vector set can invalidate
// the reference.
//
//gcassert:inline
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

	split := offset * vs.Dims
	other := Set{
		Dims:  vs.Dims,
		Count: vs.Count - offset,
		Data:  vs.Data[split:],
	}

	// Specify capacity of the slice so that it's safe to add vectors to this
	// set without impacting the returned set.
	vs.Data = vs.Data[:split:split]
	vs.Count = offset
	return other
}

// Add appends a new vector to the set.
func (vs *Set) Add(v T) {
	if vs.Dims != len(v) {
		panic(errors.AssertionFailedf(
			"cannot add vector with %d dimensions to a set with %d dimensions", len(v), vs.Dims))
	}
	vs.Data = append(vs.Data, v...)
	vs.Count++
}

// AddSet appends all vectors from the given set to this set.
func (vs *Set) AddSet(vectors Set) {
	if vs.Dims != vectors.Dims {
		panic(errors.AssertionFailedf(
			"cannot add vector set with %d dimensions to a set with %d dimensions",
			vectors.Dims, vs.Dims))
	}
	vs.Data = append(vs.Data, vectors.Data...)
	vs.Count += vectors.Count
}

// AddUndefined adds the given number of vectors to this set. The vectors should
// be set to defined values before use.
func (vs *Set) AddUndefined(count int) {
	vs.Data = slices.Grow(vs.Data, count*vs.Dims)
	vs.Count += count
	vs.Data = vs.Data[:vs.Count*vs.Dims]
	if buildutil.CrdbTestBuild {
		// Write non-zero values to undefined memory.
		for i := len(vs.Data) - count*vs.Dims; i < len(vs.Data); i++ {
			vs.Data[i] = 0xBADF00D
		}
	}
}

// Clear empties the set so that it has zero vectors.
func (vs *Set) Clear() {
	if buildutil.CrdbTestBuild {
		// Write non-zero values to cleared memory.
		for i := 0; i < len(vs.Data); i++ {
			vs.Data[i] = 0xBADF00D
		}
	}
	vs.Data = vs.Data[:0]
	vs.Count = 0
}

// ReplaceWithLast removes the vector at the given offset from the set,
// replacing it with the last vector in the set. The modified set has one less
// element and the last vector's position changes.
func (vs *Set) ReplaceWithLast(offset int) {
	targetStart := offset * vs.Dims
	sourceEnd := len(vs.Data)
	copy(vs.Data[targetStart:targetStart+vs.Dims], vs.Data[sourceEnd-vs.Dims:sourceEnd])
	if buildutil.CrdbTestBuild {
		// Write non-zero values to undefined memory.
		for i := sourceEnd - vs.Dims; i < sourceEnd; i++ {
			vs.Data[i] = 0xBADF00D
		}
	}
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

// Centroid calculates the mean of each dimension across all vectors in the set
// and writes the resulting averages to the provided centroid slice. The slice
// must be pre-allocated by the caller, with its length set to the number of
// dimensions (Dims field) of the vectors in the set.
func (vs *Set) Centroid(centroid T) T {
	if len(centroid) != vs.Dims {
		panic(errors.AssertionFailedf(
			"centroid dims %d cannot differ from vector set dims %d", len(centroid), vs.Dims))
	}

	if vs.Count == 0 {
		// Return vector of zeros.
		num32.Zero(centroid)
		return centroid
	}

	data := vs.Data
	copy(centroid, data)
	data = data[vs.Dims:]
	for len(data) > 0 {
		num32.Add(centroid, data[:vs.Dims])
		data = data[vs.Dims:]
	}
	num32.Scale(1/float32(vs.Count), centroid)
	return centroid
}

// Equal returns true if this set is equal to the other set. Two sets are equal
// if they have the same number of dimensions, the same number of vectors, and
// the same values in the same order.
func (vs *Set) Equal(other *Set) bool {
	if vs.Dims != other.Dims || vs.Count != other.Count {
		return false
	}
	return slices.Equal(vs.Data, other.Data)
}
