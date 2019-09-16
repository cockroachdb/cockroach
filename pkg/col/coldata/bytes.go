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

// Bytes is a wrapper type for a two-dimensional byte slice.
type Bytes struct {
	data [][]byte
}

// NewBytes returns a Bytes struct with enough capacity for n elements.
func NewBytes(n int) *Bytes {
	return &Bytes{
		data: make([][]byte, n),
	}
}

// Get returns the ith []byte in Bytes.
func (b *Bytes) Get(i int) []byte {
	return b.data[i]
}

// Set sets the ith []byte in Bytes.
func (b *Bytes) Set(i int, v []byte) {
	b.data[i] = v
}

// Slice returns a new Bytes struct with its internal data sliced according to
// start and end.
func (b *Bytes) Slice(start, end int) *Bytes {
	return &Bytes{data: b.data[start:end]}
}

// CopySlice copies srcStartIdx inclusive and srcEndIdx inclusive []byte values
// from src into the receiver starting at destIdx.
func (b *Bytes) CopySlice(src *Bytes, destIdx, srcStartIdx, srcEndIdx int) {
	copy(b.data[destIdx:], src.data[srcStartIdx:srcEndIdx])
}

// AppendSlice appends srcStartIdx inclusive and srcEndIdx inclusive []byte
// values from src into the receiver starting at destIdx.
func (b *Bytes) AppendSlice(src *Bytes, destIdx, srcStartIdx, srcEndIdx int) {
	b.data = append(b.data[:destIdx], src.data[srcStartIdx:srcEndIdx]...)
}

// AppendVal appends the given []byte value to the end of the receiver.
func (b *Bytes) AppendVal(v []byte) {
	b.data = append(b.data, v)
}

// Len returns how many []byte values the receiver contains.
func (b *Bytes) Len() int {
	return len(b.data)
}

var zeroBytesColumn = make([][]byte, BatchSize())

// Zero zeroes out the underlying bytes.
func (b *Bytes) Zero() {
	for n := 0; n < len(b.data); n += copy(b.data[n:], zeroBytesColumn) {
	}
}

// PrimitiveRepr is a temprorary migration tool.
// TODO(asubiotto): Remove this.
func (b *Bytes) PrimitiveRepr() [][]byte {
	return b.data
}

// Reset here is a stub that is added so that the calls that used to be to
// flatBytes.Reset() don't need to be removed.
func (b *Bytes) Reset() {}

// TODO(yuzefovich): fix flatBytes implementation and use that instead of
// Bytes.

// Remove the unused warnings.
var _ *flatBytes = newFlatBytes(0)

// flatBytes is a wrapper type for a two-dimensional byte slice ([][]byte).
type flatBytes struct {
	// data is the slice of all bytes.
	data []byte
	// offsets contains the offsets for each []byte slice in data.
	offsets []int32
	// lengths[i] contains the length of the []byte value beginning at offsets[i].
	// TODO(asubiotto): lengths are unnecessary since we can use
	//  offsets[i+1]-offsets[i] to determine the length of the element at
	//  offsets[i].
	lengths []int32

	// maxSetIndex specifies the last index set by the user of this struct. This
	// enables us to disallow unordered sets (which would require data movement).
	maxSetIndex int
}

// newFlatBytes returns a flatBytes struct with enough capacity for n zero-length
// []byte values. It is legal to call Set on the returned flatBytes at this point,
// but Get is undefined until at least one element is Set.
func newFlatBytes(n int) *flatBytes {
	return &flatBytes{
		// Given that the []byte slices are of variable length, we multiply the
		// number of elements by some constant factor.
		// TODO(asubiotto): Make this tunable.
		data:    make([]byte, 0, n*64),
		offsets: make([]int32, n),
		lengths: make([]int32, n),
	}
}

// Get returns the ith []byte in flatBytes. Note that the returned byte slice is
// unsafe for reuse if any write operation happens.
func (b *flatBytes) Get(i int) []byte {
	return b.data[b.offsets[i] : b.offsets[i]+b.lengths[i]]
}

// Set sets the ith []byte in flatBytes. Overwriting a value that is not at the end
// of the flatBytes is not allowed since it complicates memory movement to make/take
// away necessary space in the flat buffer. Note that a nil value will be
// "converted" into an empty byte slice.
func (b *flatBytes) Set(i int, v []byte) {
	if i < b.maxSetIndex {
		panic(
			fmt.Sprintf(
				"cannot overwrite value on flat Bytes: maxSetIndex=%d, setIndex=%d, consider using Reset",
				b.maxSetIndex,
				i,
			),
		)
	}
	if i == b.maxSetIndex && b.offsets[i]+b.lengths[i] > 0 {
		// We are overwriting a non-zero element at the end of b.data, truncate so
		// we can append in every path.
		b.data = b.data[:b.offsets[i]]
	}
	b.offsets[i] = int32(len(b.data))
	b.lengths[i] = int32(len(v))
	b.data = append(b.data, v...)
	b.maxSetIndex = i
}

// Slice modifies and returns the receiver flatBytes struct sliced according to
// start and end. Returning the result is not necessary but is done for
// compatibility with the usage of other data representations in vectorized
// execution. Note that Slicing can be expensive since it incurs a step to
// translate offsets.
func (b *flatBytes) Slice(start, end int) *flatBytes {
	if start == 0 && end == len(b.offsets) {
		return b
	}
	if start == end {
		b.offsets = b.offsets[:0]
		b.lengths = b.lengths[:0]
		b.data = b.data[:0]
		b.maxSetIndex = 0
		return b
	}
	translateBy := b.offsets[start]
	b.data = b.data[b.offsets[start] : b.offsets[end-1]+b.lengths[end-1]]
	b.offsets = b.offsets[start:end]
	for i := range b.offsets {
		b.offsets[i] -= translateBy
	}
	b.lengths = b.lengths[start:end]
	b.maxSetIndex = end - start - 1
	return b
}

// CopySlice copies srcStartIdx inclusive and srcEndIdx exclusive []byte values
// from src into the receiver starting at destIdx. Similar to the copy builtin,
// min(dest.Len(), src.Len()) values will be copied. Note that if the length of
// the receiver is greater than the length of the source, bytes will have to be
// physically moved. Consider the following example:
// dest flatBytes: "helloworld", offsets: []int32{0, 5}, lengths: []int32{5, 5}
// src flatBytes: "a", offsets: []int32{0}, lengths: []int32{1}
// If we copy src into the beginning of dest, we will have to move "world" so
// that the result is:
// result flatBytes: "aworld", offsets: []int32{0, 1}, lengths: []int32{1, 5}
// Similarly, if "a", is instead "alongerstring", "world" would have to be
// shifted right.
func (b *flatBytes) CopySlice(src *flatBytes, destIdx, srcStartIdx, srcEndIdx int) {
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

	srcDataToCopy := src.data[src.offsets[srcStartIdx] : src.offsets[srcEndIdx-1]+src.lengths[srcEndIdx-1]]
	if destIdx+toCopy > len(b.offsets) {
		// Reduce the number of elements to copy to what can fit into the
		// destination.
		toCopy = len(b.offsets) - destIdx
		srcDataToCopy = src.data[src.offsets[srcStartIdx]:src.offsets[srcStartIdx+toCopy]]
	}

	newMaxIdx := destIdx + (toCopy - 1)
	if newMaxIdx > b.maxSetIndex {
		b.maxSetIndex = newMaxIdx
	}

	var leftoverDestBytes []byte
	if destIdx+toCopy < len(b.offsets) {
		// There will still be elements left over after the last element to copy. We
		// copy those elements into leftoverDestBytes to append after all elements
		// have been copied.
		leftoverDestBytesToCopy := b.data[b.offsets[destIdx+toCopy]:]
		leftoverDestBytes = make([]byte, len(leftoverDestBytesToCopy))
		copy(leftoverDestBytes, leftoverDestBytesToCopy)
	}

	destDataIdx := int32(0)
	if destIdx != 0 {
		destDataIdx = b.offsets[destIdx-1] + b.lengths[destIdx-1]
	}
	translateBy := destDataIdx - src.offsets[srcStartIdx]

	// Append the actual bytes, we use an append instead of a copy to ensure that
	// b.data has enough capacity. Note that the "capacity" was checked
	// beforehand, but the number of elements to copy does not correlate with the
	// number of bytes.
	b.data = append(b.data[:b.offsets[destIdx]], srcDataToCopy...)
	copy(b.lengths[destIdx:], src.lengths[srcStartIdx:srcEndIdx])
	copy(b.offsets[destIdx:], src.offsets[srcStartIdx:srcEndIdx])

	if translateBy != 0 {
		destOffsets := b.offsets[destIdx : destIdx+toCopy]
		for i := range destOffsets {
			destOffsets[i] += translateBy
		}
	}

	if leftoverDestBytes != nil {
		dataToAppendTo := b.data[:b.offsets[destIdx+toCopy-1]+b.lengths[destIdx+toCopy-1]]
		// Append (to make sure we have the capacity for) the leftoverDestBytes to
		// the end of the data we just copied.
		b.data = append(dataToAppendTo, leftoverDestBytes...)
		// The lengths for these elements are correct (they weren't overwritten),
		// but the offsets need updating.
		destOffsets := b.offsets[destIdx+toCopy:]
		translateBy := int32(len(dataToAppendTo)) - destOffsets[0]
		for i := range destOffsets {
			destOffsets[i] += translateBy
		}
	}

	// "Trim" unnecessary bytes. It's important we do this because we don't want
	// to have unreferenced bytes. This could lead to issues since we assume that
	// all bytes are "live" in order to use the builtin copy/append functions.
	dataLen := len(b.offsets)
	b.data = b.data[:b.offsets[dataLen-1]+b.lengths[dataLen-1]]
}

// AppendSlice appends srcStartIdx inclusive and srcEndIdx exclusive []byte
// values from src into the receiver starting at destIdx.
func (b *flatBytes) AppendSlice(src *flatBytes, destIdx, srcStartIdx, srcEndIdx int) {
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
		b.data = b.data[:b.offsets[destIdx]]
		b.offsets = b.offsets[:destIdx]
		b.lengths = b.lengths[:destIdx]
		return
	}

	srcDataToAppend := src.data[src.offsets[srcStartIdx] : src.offsets[srcEndIdx-1]+src.lengths[srcEndIdx-1]]

	destDataIdx := int32(0)
	if destIdx != 0 {
		destDataIdx = b.offsets[destIdx-1] + b.lengths[destIdx-1]
	}

	// Calculate the amount to translate offsets by before modifying any
	// destination offsets since we might be appending from the same flatBytes (and
	// might be overwriting information).
	translateBy := destDataIdx - src.offsets[srcStartIdx]
	b.data = append(b.data[:destDataIdx], srcDataToAppend...)
	b.offsets = append(b.offsets[:destIdx], src.offsets[srcStartIdx:srcEndIdx]...)
	b.lengths = append(b.lengths[:destIdx], src.lengths[srcStartIdx:srcEndIdx]...)
	if translateBy == 0 {
		// No offset translation needed.
		return
	}
	// destOffsets is used to avoid having to recompute the target index on every
	// iteration.
	destOffsets := b.offsets[destIdx:]
	for i := range destOffsets {
		destOffsets[i] += translateBy
	}
}

// AppendVal appends the given []byte value to the end of the receiver. A nil
// value will be "converted" into an empty byte slice.
func (b *flatBytes) AppendVal(v []byte) {
	b.maxSetIndex = len(b.offsets)
	b.offsets = append(b.offsets, int32(len(b.data)))
	b.lengths = append(b.lengths, int32(len(v)))
	b.data = append(b.data, v...)
}

// Len returns how many []byte values the receiver contains.
func (b *flatBytes) Len() int {
	return len(b.offsets)
}

var zeroInt32Slice = make([]int32, BatchSize())

// Zero zeroes out the underlying bytes. Note that this doesn't change the
// length. Use this instead of Reset if you need to be able to Get zeroed byte
// slices.
func (b *flatBytes) Zero() {
	b.data = b.data[:0]
	for n := 0; n < len(b.offsets); n += copy(b.offsets[n:], zeroInt32Slice) {
	}
	for n := 0; n < len(b.lengths); n += copy(b.lengths[n:], zeroInt32Slice) {
	}
	b.maxSetIndex = 0
}

// Reset resets the underlying flatBytes for reuse.
// This is useful when the caller is going to overwrite the underlying bytes and
// needs a quick way to Reset. Note that this doesn't change the length either.
// TODO(asubiotto): Move towards removing Set in favor of AppendVal. At that
//  point we can reset the length to 0.
func (b *flatBytes) Reset() {
	b.data = b.data[:0]
	b.maxSetIndex = 0
}

// String is used for debugging purposes.
func (b *flatBytes) String() string {
	var builder strings.Builder
	for i := range b.offsets {
		builder.WriteString(
			fmt.Sprintf("%d: %v\n", i, b.data[b.offsets[i]:b.offsets[i]+b.lengths[i]]),
		)
	}
	return builder.String()
}

/*
// BytesFromArrowSerializationFormat takes an Arrow byte slice and accompanying
// offsets and populates b.
func BytesFromArrowSerializationFormat(b *Bytes, data []byte, offsets []int32) {
	if cap(b.lengths) < len(offsets)-1 {
		b.lengths = b.lengths[:cap(b.lengths)]
		b.lengths = append(b.lengths, make([]int32, len(offsets)-1-len(b.lengths))...)
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
	b.maxSetIndex = len(offsets) - 1
}


// ToArrowSerializationFormat returns a bytes slice and offsets that are
// Arrow-compatible. n is the number of elements to serialize.
func (b *Bytes) ToArrowSerializationFormat(n int) ([]byte, []int32) {
	if n == 0 {
		return []byte{}, []int32{0}
	}
	serializeLength := b.offsets[n-1] + b.lengths[n-1]
	data := b.data[:serializeLength]
	offsets := append(b.offsets[:n], serializeLength)
	return data, offsets
}
*/
