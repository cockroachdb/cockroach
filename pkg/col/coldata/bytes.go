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
	"unsafe"

	"github.com/cockroachdb/errors"
)

// Bytes is a wrapper type for a two-dimensional byte slice ([][]byte).
type Bytes struct {
	// data is the slice of all bytes.
	data []byte
	// offsets contains the offsets for each []byte slice in data. Note that the
	// last offset (similarly to Arrow format) will contain the full length of
	// data. Note that we assume that offsets are non-decreasing, and with every
	// access of this Bytes we will try to maintain this assumption.
	offsets []int32

	// maxSetIndex specifies the last index set by the user of this struct. This
	// enables us to disallow unordered sets (which would require data movement).
	// Also, this helps us maintain the assumption of non-decreasing offsets.
	maxSetIndex int

	// isWindow indicates whether this Bytes is a "window" into another Bytes.
	// If it is, no modifications are allowed (all of them will panic).
	isWindow bool
}

// BytesInitialAllocationFactor is an estimate of how many bytes each []byte
// slice takes up. It is used during the initialization of Bytes.
const BytesInitialAllocationFactor = 64

// NewBytes returns a Bytes struct with enough capacity for n zero-length
// []byte values. It is legal to call Set on the returned Bytes at this point,
// but Get is undefined until at least one element is Set.
func NewBytes(n int) *Bytes {
	return &Bytes{
		// Given that the []byte slices are of variable length, we multiply the
		// number of elements by some constant factor.
		// TODO(asubiotto): Make this tunable.
		data:    make([]byte, 0, n*BytesInitialAllocationFactor),
		offsets: make([]int32, n+1),
	}
}

// AssertOffsetsAreNonDecreasing asserts that all b.offsets[:n+1] are
// non-decreasing.
func (b *Bytes) AssertOffsetsAreNonDecreasing(n int) {
	for j := 1; j <= n; j++ {
		if b.offsets[j] < b.offsets[j-1] {
			panic(errors.AssertionFailedf("unexpectedly found decreasing offsets: %v", b.offsets))
		}
	}
}

// UpdateOffsetsToBeNonDecreasing makes sure that b.offsets[:n+1] are
// non-decreasing which is an invariant that we need to maintain. It must be
// called by the colexec.Operator that is modifying this Bytes before
// returning it as an output. A convenient place for this is Batch.SetLength()
// method - we assume that *always*, before returning a batch, the length is
// set on it.
func (b *Bytes) UpdateOffsetsToBeNonDecreasing(n int) {
	// Note that we're not checking whether this Bytes is a window because
	// although this function might modify the "window" Bytes, it maintains the
	// invariant that we need to have.
	prev := b.offsets[0]
	for j := 1; j <= n; j++ {
		if b.offsets[j] > prev {
			prev = b.offsets[j]
		} else {
			b.offsets[j] = prev
		}
	}
}

// maybeBackfillOffsets is an optimized version of
// UpdateOffsetsToBeNonDecreasing that assumes that all offsets up to
// b.maxSetIndex+1 are non-decreasing. Note that this method is a noop when
// i <= b.maxSetIndex+1.
func (b *Bytes) maybeBackfillOffsets(i int) {
	// Note that we're not checking whether this Bytes is a window because
	// although this function might modify the "window" Bytes, it maintains the
	// invariant that we need to have.
	for j := b.maxSetIndex + 2; j <= i; j++ {
		b.offsets[j] = b.offsets[b.maxSetIndex+1]
	}
	if i > b.maxSetIndex {
		b.maxSetIndex = i - 1
	}
}

// Get returns the ith []byte in Bytes. Note that the returned byte slice is
// unsafe for reuse if any write operation happens.
// NOTE: if ith element was never set in any way, the behavior of Get is
// undefined.
func (b *Bytes) Get(i int) []byte {
	return b.data[b.offsets[i]:b.offsets[i+1]]
}

// Set sets the ith []byte in Bytes. Overwriting a value that is not at the end
// of the Bytes is not allowed since it complicates memory movement to make/take
// away necessary space in the flat buffer. Note that a nil value will be
// "converted" into an empty byte slice.
func (b *Bytes) Set(i int, v []byte) {
	if b.isWindow {
		panic("Set is called on a window into Bytes")
	}
	if i < b.maxSetIndex {
		panic(
			fmt.Sprintf(
				"cannot overwrite value on flat Bytes: maxSetIndex=%d, setIndex=%d, consider using Reset",
				b.maxSetIndex,
				i,
			),
		)
	}
	// We're maybe setting an element not right after the last already present
	// element (i.e. there might be gaps in b.offsets). This is probably due to
	// NULL values that are stored separately. In order to maintain the
	// assumption of non-decreasing offsets, we need to backfill them.
	b.maybeBackfillOffsets(i)
	b.data = b.data[:b.offsets[i]]
	b.offsets[i] = int32(len(b.data))
	b.data = append(b.data, v...)
	b.offsets[i+1] = int32(len(b.data))
	b.maxSetIndex = i
}

// Window creates a "window" into the receiver. It behaves similarly to
// Golang's slice, but the returned object is *not* allowed to be modified - it
// is read-only. Window is a lightweight operation that doesn't involve copying
// the underlying data.
func (b *Bytes) Window(start, end int) *Bytes {
	if start < 0 || start > end || end > b.Len() {
		panic(
			fmt.Sprintf(
				"invalid window arguments: start=%d end=%d when Bytes.Len()=%d",
				start, end, b.Len(),
			),
		)
	}
	b.maybeBackfillOffsets(end)
	data := b.data[:b.offsets[end]]
	if end == 0 {
		data = b.data[:0]
	}
	return &Bytes{
		data: data,
		// We use 'end+1' because of the extra offset to know the length of the
		// last element of the newly created window.
		offsets: b.offsets[start : end+1],
		// maxSetIndex is set only for pretty printing the Bytes.
		maxSetIndex: (end - start) - 1,
		isWindow:    true,
	}
}

// CopySlice copies srcStartIdx inclusive and srcEndIdx exclusive []byte values
// from src into the receiver starting at destIdx. Similar to the copy builtin,
// min(dest.Len(), src.Len()) values will be copied. Note that if the length of
// the receiver is greater than the length of the source, bytes will have to be
// physically moved. Consider the following example:
// dest Bytes: "helloworld", offsets: []int32{0, 5}, lengths: []int32{5, 5}
// src Bytes: "a", offsets: []int32{0}, lengths: []int32{1}
// If we copy src into the beginning of dest, we will have to move "world" so
// that the result is:
// result Bytes: "aworld", offsets: []int32{0, 1}, lengths: []int32{1, 5}
// Similarly, if "a", is instead "alongerstring", "world" would have to be
// shifted right.
func (b *Bytes) CopySlice(src *Bytes, destIdx, srcStartIdx, srcEndIdx int) {
	if b.isWindow {
		panic("CopySlice is called on a window into Bytes")
	}
	if destIdx < 0 || destIdx > b.Len() {
		panic(
			fmt.Sprintf(
				"dest index %d out of range (len=%d)", destIdx, b.Len(),
			),
		)
	} else if srcStartIdx < 0 || srcStartIdx > src.Len() ||
		srcEndIdx > src.Len() || srcStartIdx > srcEndIdx {
		panic(
			fmt.Sprintf(
				"source index start %d or end %d invalid (len=%d)",
				srcStartIdx, srcEndIdx, src.Len(),
			),
		)
	}
	toCopy := srcEndIdx - srcStartIdx
	if toCopy == 0 || b.Len() == 0 || destIdx == b.Len() {
		return
	}
	b.maybeBackfillOffsets(destIdx)

	if destIdx+toCopy > b.Len() {
		// Reduce the number of elements to copy to what can fit into the
		// destination.
		toCopy = b.Len() - destIdx
		srcEndIdx = srcStartIdx + toCopy
	}

	// It is possible that the last couple of elements to be copied from the
	// source are actually NULL values in which case the corresponding offsets
	// might remain zeroes. We want to be on the safe side, so we backfill the
	// source offsets as well.
	src.maybeBackfillOffsets(srcEndIdx)
	srcDataToCopy := src.data[src.offsets[srcStartIdx]:src.offsets[srcEndIdx]]

	var leftoverDestBytes []byte
	if destIdx+toCopy <= b.maxSetIndex {
		// There will still be elements left over after the last element to copy. We
		// copy those elements into leftoverDestBytes to append after all elements
		// have been copied.
		leftoverDestBytesToCopy := b.data[b.offsets[destIdx+toCopy]:]
		leftoverDestBytes = make([]byte, len(leftoverDestBytesToCopy))
		copy(leftoverDestBytes, leftoverDestBytesToCopy)
	}

	newMaxIdx := destIdx + (toCopy - 1)
	if newMaxIdx > b.maxSetIndex {
		b.maxSetIndex = newMaxIdx
	}

	destDataIdx := int32(0)
	if destIdx != 0 {
		// Note that due to our implementation of slicing, b.offsets[0] is not
		// always 0, so we need this 'if'.
		destDataIdx = b.offsets[destIdx]
	}
	translateBy := destDataIdx - src.offsets[srcStartIdx]

	// Append the actual bytes, we use an append instead of a copy to ensure that
	// b.data has enough capacity. Note that the "capacity" was checked
	// beforehand, but the number of elements to copy does not correlate with the
	// number of bytes.
	if destIdx != 0 {
		b.data = append(b.data[:b.offsets[destIdx]], srcDataToCopy...)
	} else {
		b.data = append(b.data[:0], srcDataToCopy...)
	}
	copy(b.offsets[destIdx:], src.offsets[srcStartIdx:srcEndIdx])

	if translateBy != 0 {
		destOffsets := b.offsets[destIdx : destIdx+toCopy]
		for i := range destOffsets {
			destOffsets[i] += translateBy
		}
	}

	oldPrefixAndCopiedDataLen := b.offsets[destIdx] + int32(len(srcDataToCopy))
	if leftoverDestBytes != nil {
		dataToAppendTo := b.data[:oldPrefixAndCopiedDataLen]
		// Append (to make sure we have the capacity for) the leftoverDestBytes to
		// the end of the data we just copied.
		b.data = append(dataToAppendTo, leftoverDestBytes...)
		// The lengths for these elements are correct (they weren't overwritten),
		// but the offsets need updating.
		destOffsets := b.offsets[destIdx+toCopy+1:]
		translateBy := int32(len(dataToAppendTo)) - b.offsets[destIdx+toCopy]
		for i := range destOffsets {
			destOffsets[i] += translateBy
		}
	}
	b.offsets[destIdx+toCopy] = oldPrefixAndCopiedDataLen
}

// AppendSlice appends srcStartIdx inclusive and srcEndIdx exclusive []byte
// values from src into the receiver starting at destIdx.
func (b *Bytes) AppendSlice(src *Bytes, destIdx, srcStartIdx, srcEndIdx int) {
	if b.isWindow {
		panic("AppendSlice is called on a window into Bytes")
	}
	if destIdx < 0 || destIdx > b.Len() {
		panic(
			fmt.Sprintf(
				"dest index %d out of range (len=%d)", destIdx, b.Len(),
			),
		)
	} else if srcStartIdx < 0 || srcStartIdx > src.Len() ||
		srcEndIdx > src.Len() || srcStartIdx > srcEndIdx {
		panic(
			fmt.Sprintf(
				"source index start %d or end %d invalid (len=%d)",
				srcStartIdx, srcEndIdx, src.Len(),
			),
		)
	}
	// NOTE: it is important that we do not update b.maxSetIndex before we do the
	// backfill. Also, since it is possible to have a self referencing source, we
	// can update b.maxSetIndex only after backfilling the source below.
	b.maybeBackfillOffsets(destIdx)
	toAppend := srcEndIdx - srcStartIdx
	if toAppend == 0 {
		b.maxSetIndex = destIdx + (toAppend - 1)
		if destIdx == b.Len() {
			return
		}
		b.data = b.data[:b.offsets[destIdx]]
		b.offsets = b.offsets[:destIdx+1]
		return
	}

	// It is possible that the last couple of elements to be copied from the
	// source are actually NULL values in which case the corresponding offsets
	// might remain zeroes. We want to be on the safe side, so we backfill the
	// source offsets as well.
	src.maybeBackfillOffsets(srcEndIdx)
	srcDataToAppend := src.data[src.offsets[srcStartIdx]:src.offsets[srcEndIdx]]
	destDataIdx := b.offsets[destIdx]
	b.maxSetIndex = destIdx + (toAppend - 1)

	// Calculate the amount to translate offsets by before modifying any
	// destination offsets since we might be appending from the same Bytes (and
	// might be overwriting information).
	translateBy := destDataIdx - src.offsets[srcStartIdx]
	b.data = append(b.data[:destDataIdx], srcDataToAppend...)
	b.offsets = append(b.offsets[:destIdx], src.offsets[srcStartIdx:srcEndIdx+1]...)
	if translateBy == 0 {
		// No offset translation needed.
		return
	}
	// destOffsets is used to avoid having to recompute the target index on every
	// iteration.
	destOffsets := b.offsets[destIdx:]
	// The lengths for these elements are correct, but the offsets need updating.
	for i := range destOffsets {
		destOffsets[i] += translateBy
	}
}

// AppendVal appends the given []byte value to the end of the receiver. A nil
// value will be "converted" into an empty byte slice.
func (b *Bytes) AppendVal(v []byte) {
	if b.isWindow {
		panic("AppendVal is called on a window into Bytes")
	}
	b.maybeBackfillOffsets(b.Len())
	b.maxSetIndex = b.Len()
	b.offsets[b.Len()] = int32(len(b.data))
	b.data = append(b.data, v...)
	b.offsets = append(b.offsets, int32(len(b.data)))
}

// SetLength sets the length of this Bytes. Note that it will panic if there is
// not enough capacity.
func (b *Bytes) SetLength(l int) {
	if b.isWindow {
		panic("SetLength is called on a window into Bytes")
	}
	// We need +1 for an extra offset at the end.
	b.offsets = b.offsets[:l+1]
}

// Len returns how many []byte values the receiver contains.
func (b *Bytes) Len() int {
	return len(b.offsets) - 1
}

// FlatBytesOverhead is the overhead of Bytes in bytes.
const FlatBytesOverhead = unsafe.Sizeof(Bytes{})
const sizeOfInt32 = unsafe.Sizeof(int32(0))

// Size returns the total size of the receiver in bytes.
func (b *Bytes) Size() uintptr {
	return FlatBytesOverhead +
		uintptr(cap(b.data)) +
		uintptr(cap(b.offsets))*sizeOfInt32
}

// ProportionalSize returns the size of the receiver in bytes that is
// attributed to only first n out of Len() elements.
func (b *Bytes) ProportionalSize(n int64) uintptr {
	return FlatBytesOverhead + uintptr(len(b.data[:b.offsets[n]])) + uintptr(n)*sizeOfInt32
}

var zeroInt32Slice = make([]int32, BatchSize())

// Reset resets the underlying Bytes for reuse. Note that this zeroes out the
// underlying bytes but doesn't change the length (see #42054 for the
// discussion on why simply truncating b.data and setting b.maxSetIndex to 0 is
// not sufficient).
// TODO(asubiotto): Move towards removing Set in favor of AppendVal. At that
// point we can reset the length to 0.
func (b *Bytes) Reset() {
	if b.isWindow {
		panic("Reset is called on a window into Bytes")
	}
	b.data = b.data[:0]
	for n := 0; n < len(b.offsets); n += copy(b.offsets[n:], zeroInt32Slice) {
	}
	b.maxSetIndex = 0
}

// String is used for debugging purposes.
func (b *Bytes) String() string {
	var builder strings.Builder
	for i := range b.offsets[:b.maxSetIndex+1] {
		builder.WriteString(
			fmt.Sprintf("%d: %v\n", i, b.data[b.offsets[i]:b.offsets[i+1]]),
		)
	}
	return builder.String()
}

// BytesFromArrowSerializationFormat takes an Arrow byte slice and accompanying
// offsets and populates b.
func BytesFromArrowSerializationFormat(b *Bytes, data []byte, offsets []int32) {
	b.data = data
	b.offsets = offsets
	b.maxSetIndex = len(offsets) - 2
}

// ToArrowSerializationFormat returns a bytes slice and offsets that are
// Arrow-compatible. n is the number of elements to serialize.
func (b *Bytes) ToArrowSerializationFormat(n int) ([]byte, []int32) {
	if n == 0 {
		return []byte{}, []int32{0}
	}
	serializeLength := b.offsets[n]
	data := b.data[:serializeLength]
	offsets := b.offsets[:n+1]
	return data, offsets
}
