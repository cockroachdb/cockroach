// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

import (
	"fmt"
	"strings"
)

// Bytes is a wrapper type for a two-dimensional byte slice ([][]byte).
type Bytes struct {
	// data is the slice of all bytes
	data []byte
	// offsets contains the offsets for each []byte slice in data. The offsets
	// may be out of order (e.g. offsets = []int{3, 0}), which is reflected in
	// outOfOrder. If the offsets are in order, fast paths can be used in various
	// situations,
	offsets []int32
	// lengths[i] contains the length of the []byte value beginning at offsets[i].
	lengths []int32

	// outOfOrder specifies whether the offsets slice is out of order.
	outOfOrder bool
	// maxSetIndex specifies the last index set by the user of this struct. This
	// enables us to set outOfOrder if an element is set prior to maxSetIndex.
	maxSetIndex int
}

// NewBytes returns a Bytes struct with enough capacity for n zero-length
// []byte values. It is legal to call Set on the returned Bytes at this point,
// but Get is undefined until at least one element is Set.
func NewBytes(n int) *Bytes {
	return &Bytes{
		// Given that the []byte slices are of variable length, we multiply the
		// number of elements by some constant factor.
		data:    make([]byte, 0, n*64),
		offsets: make([]int32, n),
		lengths: make([]int32, n),
	}
}

// Get returns the ith []byte in Bytes.
func (b Bytes) Get(i int) []byte {
	return b.data[b.offsets[i] : b.offsets[i]+b.lengths[i]]
}

// Set sets the ith []byte in Bytes.
func (b *Bytes) Set(i int, v []byte) {
	b.offsets[i] = int32(len(b.data))
	b.lengths[i] = int32(len(v))
	// Append to b.data since these Sets could be random and we want to minimize
	// data movement.
	b.data = append(b.data, v...)
	b.outOfOrder = b.outOfOrder || i < b.maxSetIndex
	if i > b.maxSetIndex {
		b.maxSetIndex = i
	}
}

// Swap swaps the ith []byte with the jth []byte and vice-versa.
func (b *Bytes) Swap(i int, j int) {
	// Swaps are unsupported because we currently do not use physical swaps in the
	// vectorized execution engine. The abstraction of having a flat byte slice
	// support out of order sets is supported but could lead to some unintended
	// performance consequences. The execution engine will slowly move towards not
	// doing these either, but we currently can only disallow Swaps.
	panic("physical flat Bytes Swaps are unsupported")
}

// Slice modifies and returns the receiver Bytes struct sliced according to
// start and end. Returning the result is not necessary but is done for
// compatibility with the usage of other data representations in vectorized
// execution.
func (b *Bytes) Slice(start, end int) *Bytes {
	if start == 0 && end == len(b.offsets) {
		return b
	}
	b.offsets = b.offsets[start:end]
	b.lengths = b.lengths[start:end]
	b.maxSetIndex = end - start - 1
	return b
}

// CopySlice copies srcStartIdx inclusive and srcEndIdx exclusive []byte values
// from src into the receiver starting at destIdx. Similar to the copy builtin,
// min(dest.Len(), src.Len()) values will be copied. Note that due to the
// possibility of unordered offsets, data that is overwritten might still be
// kept around.
func (b *Bytes) CopySlice(src *Bytes, destIdx, srcStartIdx, srcEndIdx int) {
	if destIdx < 0 || destIdx > b.Len() {
		panic(fmt.Sprintf("dest index %d out of range (len=%d)", destIdx, b.Len()))
	} else if srcStartIdx < 0 || srcStartIdx >= src.Len() || srcEndIdx > src.Len() || srcStartIdx > srcEndIdx {
		panic(
			fmt.Sprintf("source index start %d or end %d invalid (len=%d)", srcStartIdx, srcEndIdx, src.Len()),
		)
	}
	toCopy := srcEndIdx - srcStartIdx
	if toCopy == 0 || len(b.offsets) == 0 || destIdx == len(b.offsets) {
		return
	}

	newMaxIdx := destIdx + (toCopy - 1)
	if newMaxIdx > b.maxSetIndex {
		b.maxSetIndex = newMaxIdx
	}
	if destIdx == 0 && toCopy >= len(b.offsets) {
		// Fast path; this fully overwrites the destination data.
		if cap(b.data) < len(src.data) {
			// Ensure we have the correct capacity for a full copy. Note that the
			// capacity of b.data has no correlation with the number of values.
			b.data = make([]byte, len(src.data))
		}
		b.data = b.data[:cap(b.data)]
		copy(b.data, src.data)
		copy(b.offsets, src.offsets[srcStartIdx:srcEndIdx])
		copy(b.lengths, src.lengths[srcStartIdx:srcEndIdx])
		b.outOfOrder = src.outOfOrder
		return
	}

	if destIdx+toCopy < len(b.offsets) {
		// There will still be elements left over after the last element to copy,
		// therefore the destination will be out of order (since we only append).
		b.outOfOrder = true
	}

	// TODO(asubiotto): There are several performance improvements we can do here
	//  if !b.outOfOrder and !src.outOfOrder to use the builtin copy. The only
	//  thing to watch out for is whether there is a need to shift elements past
	//  the end of the last element copied. I'm wary of introducing more special
	//  casing in the first iteration of this so leaving this for the future.

	srcOffsets := src.offsets
	srcLengths := src.lengths
	if src == b {
		// If we're copying from the same Bytes struct, we don't want to
		// invalidate source offsets as we're setting the destination offsets. We
		// have to create a copy of the source offsets and lengths.
		srcOffsets = make([]int32, toCopy)
		srcLengths = make([]int32, toCopy)
		copy(srcOffsets, src.offsets[srcStartIdx:srcEndIdx])
		copy(srcLengths, src.lengths[srcStartIdx:srcEndIdx])
		srcStartIdx = 0
		srcEndIdx = len(srcOffsets)
	}

	// If the copy is not a full overwrite of b, we iterate element over element.
	// Note that since the offsets could be out of order, b.data is appended to.
	for i, j := destIdx, srcStartIdx; i < len(b.offsets) && j < srcEndIdx; i, j = i+1, j+1 {
		v := src.data[srcOffsets[j] : srcOffsets[j]+srcLengths[j]]
		b.offsets[i] = int32(len(b.data))
		b.lengths[i] = int32(len(v))
		b.data = append(b.data, v...)
	}
}

// AppendSlice appends srcStartIdx inclusive and srcEndIdx exclusive []byte
// values from src into the receiver starting at destIdx.
func (b *Bytes) AppendSlice(src *Bytes, destIdx, srcStartIdx, srcEndIdx int) {
	if destIdx < 0 || destIdx > b.Len() {
		panic(fmt.Sprintf("dest index %d out of range (len=%d)", destIdx, b.Len()))
	} else if srcStartIdx < 0 || srcStartIdx >= src.Len() || srcEndIdx > src.Len() || srcStartIdx > srcEndIdx {
		panic(
			fmt.Sprintf("source index start %d or end %d invalid (len=%d)", srcStartIdx, srcEndIdx, src.Len()),
		)
	}
	toAppend := srcEndIdx - srcStartIdx
	b.maxSetIndex = destIdx + (toAppend - 1)
	if toAppend == 0 {
		if destIdx == len(b.offsets) {
			return
		}
		// This is a truncation.
		if !b.outOfOrder {
			b.data = b.data[:b.offsets[destIdx]]
		}
		b.offsets = b.offsets[:destIdx]
		b.lengths = b.lengths[:destIdx]
		return
	}
	if destIdx == 0 {
		// Fast path, this append is overwriting the receiver (append truncates past
		// the end of the elements that are appended).
		b.data = append(b.data[:0], src.data...)
		b.offsets = append(b.offsets[:0], src.offsets[srcStartIdx:srcEndIdx]...)
		b.lengths = append(b.lengths[:0], src.lengths[srcStartIdx:srcEndIdx]...)
		b.outOfOrder = src.outOfOrder
		return
	}

	if !b.outOfOrder && !src.outOfOrder {
		destDataIdx := b.offsets[destIdx-1] + b.lengths[destIdx-1]
		b.data = append(
			b.data[:destDataIdx],
			src.data[src.offsets[srcStartIdx]:src.offsets[srcEndIdx-1]+src.lengths[srcEndIdx-1]]...,
		)
		translateBy := int32(destDataIdx - src.offsets[srcStartIdx])
		b.offsets = append(b.offsets[:destIdx], src.offsets[srcStartIdx:srcEndIdx]...)
		b.lengths = append(b.lengths[:destIdx], src.lengths[srcStartIdx:srcEndIdx]...)
		for i := destIdx; i < destIdx+toAppend; i++ {
			b.offsets[i] += translateBy
		}
		return
	}

	b.outOfOrder = b.outOfOrder || src.outOfOrder

	srcOffsets := src.offsets
	srcLengths := src.lengths
	if src == b {
		// If we're appending from the same Bytes struct, we don't want to
		// invalidate source offsets as we're appending to the destination offsets.
		// Since we can't use the builtin append as above, we have to create a copy
		// of the source offsets and lengths.
		srcOffsets = make([]int32, toAppend)
		srcLengths = make([]int32, toAppend)
		copy(srcOffsets, src.offsets[srcStartIdx:srcEndIdx])
		copy(srcLengths, src.lengths[srcStartIdx:srcEndIdx])
		srcStartIdx = 0
		srcEndIdx = len(srcOffsets)
	}

	b.offsets = b.offsets[:destIdx]
	b.lengths = b.lengths[:destIdx]
	for i := srcStartIdx; i < srcEndIdx; i++ {
		v := src.data[srcOffsets[i] : srcOffsets[i]+srcLengths[i]]
		b.offsets = append(b.offsets, int32(len(b.data)))
		b.lengths = append(b.lengths, int32(len(v)))
		b.data = append(b.data, v...)
	}
}

// AppendVal appends the given []byte value to the end of the receiver.
func (b *Bytes) AppendVal(v []byte) {
	b.maxSetIndex = len(b.offsets)
	b.offsets = append(b.offsets, int32(len(b.data)))
	b.lengths = append(b.lengths, int32(len(v)))
	b.data = append(b.data, v...)
}

// Len returns how many []byte values the receiver contains.
func (b Bytes) Len() int {
	return len(b.offsets)
}

var (
	zeroBytesSlice = make([]byte, BatchSize)
	zeroInt32Slice = make([]int32, BatchSize)
)

// Zero zeroes out the underlying bytes.
func (b Bytes) Zero() {
	b.data = b.data[:0]
	for n := 0; n < len(b.offsets); n += copy(b.offsets[n:], zeroInt32Slice) {
	}
	for n := 0; n < len(b.lengths); n += copy(b.lengths[n:], zeroInt32Slice) {
	}
	b.outOfOrder = false
	b.maxSetIndex = 0
}

// String is used for debugging purposes.
func (b Bytes) String() string {
	var builder strings.Builder
	for i := range b.offsets {
		builder.WriteString(
			fmt.Sprintf("%d: %v\n", i, b.data[b.offsets[i]:b.offsets[i]+b.lengths[i]]),
		)
	}
	return builder.String()
}

// PrimitiveRepr is a temprorary migration tool.
// TODO(asubiotto): Remove this.
func (b Bytes) PrimitiveRepr() [][]byte {
	bytes := make([][]byte, b.Len())
	for i := 0; i < b.Len(); i++ {
		bytes[i] = b.Get(i)
	}
	return bytes
}

// BytesFromArrowSerializationFormat takes an Arrow byte slice and accompanying
// offsets and populates b.
func BytesFromArrowSerializationFormat(b *Bytes, data []byte, offsets []int32) {
	if cap(b.lengths) < len(offsets)-1 {
		b.lengths = b.lengths[:cap(b.lengths)]
		b.lengths = append(b.lengths, make([]int32, len(offsets)-1)...)
	}
	b.lengths = b.lengths[:len(offsets)-1]
	for i, offset := range offsets[1:] {
		// Starting from the second index, the length for each element is the
		// current offset minus the previous offset.
		b.lengths[i] = offset - offsets[i]
	}
	// Trim the last offset, which is just the length of the data slice.
	offsets = offsets[:len(offsets)-1]
	b.data = data
	b.offsets = offsets
	b.outOfOrder = false
	b.maxSetIndex = len(offsets) - 1
}

// ToArrowSerializationFormat returns a bytes slice and offsets that are
// Arrow-compatible.
func (b Bytes) ToArrowSerializationFormat() ([]byte, []int32) {
	data := b.data
	offsets := b.offsets
	if b.outOfOrder {
		// Reorder for Arrow compatibility. Not doing this would probably be a
		// performance optimization but keeping the serialization format compatible
		// gives us the option of improving this flat data representation in the
		// future without needing to worry about multiversion node interactions.
		data = make([]byte, 0, len(b.data))
		offsets = make([]int32, 0, len(b.offsets)+1)
		offsets = append(offsets, 0)
		for i := range b.offsets {
			data = append(data, b.data[b.offsets[i]:b.offsets[i]+b.lengths[i]]...)
			offsets = append(offsets, int32(len(data)))
		}
	} else {
		offsets = append(offsets, int32(len(data)))
	}
	return data, offsets
}
