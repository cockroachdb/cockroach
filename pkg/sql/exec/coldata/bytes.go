// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// http://www.apache.org/licenses/LICENSE-2.0

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
func (b *Bytes) Set(i int, new []byte) {
	b.offsets[i] = int32(len(b.data))
	b.lengths[i] = int32(len(new))
	// Append to b.data since these Sets could be random and we want to minimize
	// data movement.
	b.data = append(b.data, new...)
	b.outOfOrder = b.outOfOrder || i < b.maxSetIndex
	if i > b.maxSetIndex {
		b.maxSetIndex = i
	}
}

// Swap swaps the ith []byte with the jth []byte and vice-versa.
func (b *Bytes) Swap(i int, j int) {
	b.offsets[i], b.offsets[j] = b.offsets[j], b.offsets[i]
	b.lengths[i], b.lengths[j] = b.lengths[j], b.lengths[i]
	b.outOfOrder = true
}

// Slice returns a new Bytes struct with its internal data sliced according to
// start and end.
func (b *Bytes) Slice(start, end int) *Bytes {
	if start == 0 && end == len(b.offsets) {
		return b
	}
	return &Bytes{
		data:        b.data,
		offsets:     b.offsets[start:end],
		lengths:     b.lengths[start:end],
		outOfOrder:  b.outOfOrder,
		maxSetIndex: end - start - 1,
	}
}

// CopySlice copies srcStartIdx inclusive and srcEndIdx inclusive []byte values
// from src into the receiver starting at destIdx. Similar to the copy builtin,
// min(dest.Len(), src.Len()) values will be copied. Note that due to the
// possibility of unordered offsets, data that is overwritten might still be
// kept around.
func (b *Bytes) CopySlice(src *Bytes, destIdx, srcStartIdx, srcEndIdx int) {
	if destIdx > b.Len() {
		panic(fmt.Sprintf("dest index %d out of range (len=%d)", destIdx, b.Len()))
	} else if srcStartIdx >= src.Len() || srcEndIdx > src.Len() {
		panic(
			fmt.Sprintf("source index start %d or end %d out of range (len=%d)", srcStartIdx, srcEndIdx, src.Len()),
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

	// If the copy is not a full overwrite of b, we iterate element over element.
	// Note that since the offsets could be out of order, b.data is appended to.
	for i, j := destIdx, srcStartIdx; i < len(b.offsets) && j < srcEndIdx; i, j = i+1, j+1 {
		v := src.data[src.offsets[j] : src.offsets[j]+src.lengths[j]]
		b.offsets[i] = int32(len(b.data))
		b.lengths[i] = int32(len(v))
		b.data = append(b.data, v...)
	}
}

// AppendSlice appends srcStartIdx inclusive and srcEndIdx inclusive []byte
// values from src into the receiver starting at destIdx.
func (b *Bytes) AppendSlice(src *Bytes, destIdx, srcStartIdx, srcEndIdx int) {
	if destIdx > b.Len() {
		panic(fmt.Sprintf("dest index %d out of range (len=%d)", destIdx, b.Len()))
	} else if srcStartIdx >= src.Len() || srcEndIdx > src.Len() {
		panic(
			fmt.Sprintf("source index start %d or end %d out of range (len=%d)", srcStartIdx, srcEndIdx, src.Len()),
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
		destEnd := b.offsets[destIdx-1] + b.lengths[destIdx-1]
		b.data = append(
			b.data[:destEnd],
			src.data[src.offsets[srcStartIdx]:src.offsets[srcEndIdx-1]+src.lengths[srcEndIdx-1]]...,
		)
		translateBy := int32(destEnd - src.offsets[srcStartIdx])
		b.offsets = b.offsets[:destIdx]
		b.lengths = b.lengths[:destIdx]
		for i := srcStartIdx; i < srcEndIdx; i++ {
			b.offsets = append(b.offsets, src.offsets[i]+translateBy)
			b.lengths = append(b.lengths, src.lengths[i])
		}
		return
	}

	if src.outOfOrder {
		b.outOfOrder = src.outOfOrder
	}

	b.offsets = b.offsets[:destIdx]
	b.lengths = b.lengths[:destIdx]
	for i := srcStartIdx; i < srcEndIdx; i++ {
		v := src.data[src.offsets[i] : src.offsets[i]+src.lengths[i]]
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
	lengths := make([]int32, len(offsets))
	for i, offset := range offsets[1:] {
		// Starting from the second index, the length for each element is the
		// current offset minus the previous offset.
		lengths[i] = offset - offsets[i]
	}
	// Trim the last offset, which is just the length of the data slice.
	offsets = offsets[:len(offsets)-1]
	b.data = data
	b.offsets = offsets
	b.lengths = lengths
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
