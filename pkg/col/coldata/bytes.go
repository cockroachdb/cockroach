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
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// element describes a single []byte value. If a value doesn't exceed
// BytesMaxInlineLength, then the whole value is stored within the element;
// otherwise, the element points at a region in Bytes.buffer where the
// non-inlined value is stored.
//
// If the value is inlined, then the layout of element is used as follows (SH is
// the short for reflect.SliceHeader):
//
//            SH.Data   SH.Len SH.Cap padding
//   element: xxxxxxxx | ................... | false | length
//   Bytes.buffer: N/A
//
// where 8 x's represent reflect.SliceHeader.Data that is skipped and remains
// nil, 19 dots describe the inlinable space followed by a single byte (boolean
// 'false') indicating an inlined value followed by 4 bytes for the length. The
// Data field of the slice header is not used in order to not confuse Golang's
// GC.
//
// If the value is non-inlined, then the layout of element is used as follows:
//
//                    padding
//   element: []byte |  xxx  | true | offset
//   Bytes.buffer: xxxxxxxx | start .... | xxxxxxxx
//
// where first 24 bytes contain a valid byte slice with its Data field pointing
// at start in Bytes.buffer. Then we have 3 bytes of unused padding followed by
// a single byte (boolean 'true') indicating a non-inlined value followed by 4
// bytes for the offset. The offset determines the position of 'start' within
// Bytes.buffer.
type element struct {
	data []byte
	_    [paddingLength]byte
	// We use 'nonInlined' rather than 'inlined' here so that zero value of
	// element was valid (namely, it'll contain a non-inlined value with an
	// empty []byte).
	nonInlined     bool
	lengthOrOffset int32
}

// ElementSize is the size of element object. It is expected to be 32 on a 64
// bit system.
const ElementSize = int64(unsafe.Sizeof(element{}))

// paddingLength is the number of bytes that are not assigned to any particular
// field in element and are such that ElementSize is exactly 32 on a 64 bit
// system.
const paddingLength = 3

// lenOffsetInSliceHeader is the offset of Len field in reflect.SliceHeader
// relative to the start of the struct.
const lenOffsetInSliceHeader = unsafe.Sizeof(uintptr(0))

// BytesMaxInlineLength is the maximum length of a []byte that can be inlined
// within element.
const BytesMaxInlineLength = int(2*unsafe.Sizeof(int(0))) + paddingLength

// Assert that the layout of the slice header hasn't changed - we expect the
// layout as
//   type SliceHeader struct {
//	   Data uintptr
//	   Len  int
//	   Cap  int
//   }
func init() {
	sliceHeaderSize := unsafe.Sizeof([]byte{})
	expectedSliceHeaderSize := unsafe.Sizeof(uintptr(0)) + 2*unsafe.Sizeof(0)
	if sliceHeaderSize != expectedSliceHeaderSize {
		panic(errors.AssertionFailedf(
			"unexpectedly the size of the slice header changed: expected %d, actual %d",
			expectedSliceHeaderSize, sliceHeaderSize,
		))
	}
	var sh reflect.SliceHeader
	actualDataOffset := uintptr(unsafe.Pointer(&sh.Data)) - uintptr(unsafe.Pointer(&sh))
	if actualDataOffset != 0 {
		panic(errors.AssertionFailedf("unexpectedly Data field is not the first in reflect.SliceHeader"))
	}
	actualLenOffset := uintptr(unsafe.Pointer(&sh.Len)) - uintptr(unsafe.Pointer(&sh))
	if actualLenOffset != lenOffsetInSliceHeader {
		panic(errors.AssertionFailedf(
			"unexpectedly Len field is %d bytes after Data, not %d bytes",
			actualLenOffset, lenOffsetInSliceHeader,
		))
	}
}

// inlinedSlice returns 19 bytes of space within e that can be used for storing
// a value inlined, as a slice.
//gcassert:inline
func (e *element) inlinedSlice() []byte {
	// Take the pointer to the data, skip first 8 bytes (past
	// reflect.SliceHeader.Data), convert the following 19 bytes into a pointer
	// to [19]byte, and then make a slice out of it.
	return (*(*[BytesMaxInlineLength]byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&e.data)) + lenOffsetInSliceHeader)))[:]
}

//gcassert:inline
func (e *element) get() []byte {
	if !e.nonInlined {
		return e.inlinedSlice()[:e.lengthOrOffset:BytesMaxInlineLength]
	}
	return e.data
}

//gcassert:inline
func (e *element) len() int {
	if !e.nonInlined {
		return int(e.lengthOrOffset)
	}
	return len(e.data)
}

func (e *element) set(v []byte, b *Bytes) {
	if len(v) <= BytesMaxInlineLength {
		*e = element{lengthOrOffset: int32(len(v))}
		copy(e.inlinedSlice(), v)
	} else {
		e.setNonInlined(v, b)
	}
}

func (e *element) setNonInlined(v []byte, b *Bytes) {
	// Check if we there was an old non-inlined value we can overwrite.
	if e.nonInlined && cap(e.data) >= len(v) {
		e.data = e.data[:len(v)]
		copy(e.data, v)
	} else {
		oldBuffer := b.buffer
		b.buffer = append(b.buffer, v...)
		if cap(oldBuffer) != cap(b.buffer) && cap(oldBuffer) > 0 {
			// The buffer has been reallocated, so we have to update old
			// non-inlined elements. Set nonInlined to false so that e is
			// skipped in the loop.
			e.nonInlined = false
			for i := 0; i < b.firstUnsetIdx; i++ {
				if e := b.elements[i]; e.nonInlined {
					e.data = b.buffer[e.lengthOrOffset : e.lengthOrOffset+int32(len(e.data)) : e.lengthOrOffset+int32(cap(e.data))]
				}
			}
		}
		e.data = b.buffer[len(oldBuffer) : len(oldBuffer)+len(v) : len(oldBuffer)+len(v)]
		e.lengthOrOffset = int32(len(oldBuffer))
		e.nonInlined = true
	}
}

// Bytes is a vector that stores []byte values.
type Bytes struct {
	// elements contains all values stored in this Bytes vector.
	elements []element
	// buffer contains all values that couldn't be inlined within the
	// corresponding elements.
	buffer []byte
	// firstUnsetIdx tracks the element with the smallest index that has not
	// been set yet. In other words all elements starting with this index are
	// expected to contain a zero value.
	firstUnsetIdx int
	// isWindow indicates whether this Bytes is a "window" into another Bytes.
	// If it is, no modifications are allowed (all of them will panic).
	isWindow bool
}

// NewBytes returns a Bytes struct with enough capacity to store n []byte
// values.
func NewBytes(n int) *Bytes {
	return &Bytes{elements: make([]element, n)}
}

// Get returns the ith []byte in Bytes. Note that the returned byte slice is
// unsafe for reuse if any write operation happens.
//
// Note this function call is mostly inlined except in a handful of very large
// generated functions, so we can't add the gcassert directive for it.
func (b *Bytes) Get(i int) []byte {
	return b.elements[i].get()
}

// Set sets the ith []byte in Bytes.
//
// Note this function call is mostly inlined except in a handful of very large
// generated functions, so we can't add the gcassert directive for it.
func (b *Bytes) Set(i int, v []byte) {
	if buildutil.CrdbTestBuild {
		if b.isWindow {
			panic("Set is called on a window into Bytes")
		}
	}
	b.elements[i].set(v, b)
	if i+1 > b.firstUnsetIdx {
		b.firstUnsetIdx = i + 1
	}
}

// Window creates a "window" into the receiver. It behaves similarly to Golang's
// slice, but the returned object is *not* allowed to be modified - it is
// read-only. If b is modified, then the returned object becomes invalid.
//
// Window is a lightweight operation that doesn't involve copying the underlying
// data.
// TODO(yuzefovich): consider renaming it to Slice and allowing modification of
// "sliced" Bytes since we can now support semantics similar to Golang's slices.
func (b *Bytes) Window(start, end int) *Bytes {
	if start < 0 || start > end || end > b.Len() {
		panic(
			fmt.Sprintf(
				"invalid window arguments: start=%d end=%d when Bytes.Len()=%d",
				start, end, b.Len(),
			),
		)
	}
	return &Bytes{
		elements: b.elements[start:end],
		isWindow: true,
	}
}

// CopySlice copies srcStartIdx inclusive and srcEndIdx exclusive []byte values
// from src into the receiver starting at destIdx. Similar to the copy builtin,
// min(dest.Len(), src.Len()) values will be copied.
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

	if destIdx+toCopy > b.Len() {
		// Reduce the number of elements to copy to what can fit into the
		// destination.
		toCopy = b.Len() - destIdx
	}

	if b == src && destIdx == srcStartIdx {
		// No need to do anything.
		return
	}

	// If we're copying from the vector into itself and from the earlier part
	// into the later part, we need to reverse the order (otherwise we would
	// overwrite the later values before copying them, thus corrupting the
	// vector).
	if b == src && destIdx > srcStartIdx {
		for i := toCopy - 1; i >= 0; i-- {
			b.Set(destIdx+i, src.Get(srcStartIdx+i))
		}
	} else {
		for i := 0; i < toCopy; i++ {
			b.Set(destIdx+i, src.Get(srcStartIdx+i))
		}
	}
	if destIdx+toCopy > b.firstUnsetIdx {
		b.firstUnsetIdx = destIdx + toCopy
	}
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
	srcElementsToCopy := src.elements[srcStartIdx:srcEndIdx]
	newLen := destIdx + srcEndIdx - srcStartIdx
	// If the elements slice doesn't have enough capacity, then it's clearly
	// must be reallocated.
	//
	// Additionally, when we're appending values from the vector into itself and
	// there is an overlap between the source and the destination values such
	// that the destination is in the middle of the source, we have to
	// reallocate as well in order to not corrupt values we want to copy.
	//
	// Consider the following example:
	//   src = b = {a b c}
	//   destIdx = 1, srcStartIdx = 0, srcEndIdx = 2
	// then we want the result as
	//   b = {a a b}.
	// However, if we naively set the destination's first element to the
	// source's zeroth element, we will have
	//   src = b = {a a c}
	// at which point the value 'b' is lost forever, so the end result would be
	//   b = {a a a}.
	mustReallocate := cap(b.elements) < newLen || (b == src && srcEndIdx > destIdx)
	if mustReallocate {
		oldElements := b.elements
		// Figure out the new capacity of the elements slice. We will be
		// doubling the existing capacity until newLen elements can fit, unless
		// currently elements slice is nil, in which case we use newLen as the
		// new capacity.
		newCap := cap(b.elements)
		if newCap > 0 {
			for newCap < newLen {
				newCap *= 2
			}
		} else {
			newCap = newLen
		}
		b.elements = make([]element, newLen, newCap)
		copy(b.elements[:destIdx], oldElements)
	} else {
		// We don't have to reallocate, so we can just reuse the old elements.
		// First we need to zero out all elements past the new length and then
		// we can set the new length accordingly (the elements will be set
		// below).
		b.zero(newLen)
		b.elements = b.elements[:newLen]
	}
	b.firstUnsetIdx = destIdx
	for i := 0; i < len(srcElementsToCopy); i++ {
		b.elements[b.firstUnsetIdx].set(srcElementsToCopy[i].get(), b)
		b.firstUnsetIdx++
	}
}

// AppendVal appends the given []byte value to the end of the receiver. A nil
// value will be "converted" into an empty byte slice.
func (b *Bytes) AppendVal(v []byte) {
	if b.isWindow {
		panic("AppendVal is called on a window into Bytes")
	}
	b.elements = append(b.elements, element{})
	b.elements[len(b.elements)-1].set(v, b)
	b.firstUnsetIdx = len(b.elements)
}

// Len returns how many []byte values the receiver contains.
//gcassert:inline
func (b *Bytes) Len() int {
	return len(b.elements)
}

// FlatBytesOverhead is the overhead of Bytes in bytes.
const FlatBytesOverhead = int64(unsafe.Sizeof(Bytes{}))

// Size returns the total size of the receiver in bytes.
func (b *Bytes) Size() int64 {
	if b == nil {
		return 0
	}
	return FlatBytesOverhead + int64(cap(b.elements))*ElementSize + int64(cap(b.buffer))
}

// ProportionalSize returns the size of the receiver in bytes that is attributed
// to only first n out of Len() elements.
func (b *Bytes) ProportionalSize(n int64) int64 {
	if n == 0 {
		return 0
	}
	s := FlatBytesOverhead
	for i := 0; i < int(n); i++ {
		s += b.ElemSize(i)
	}
	return s
}

// ElemSize returns the size in bytes of the []byte elem at the given index.
// Panics if passed an invalid element.
//gcassert:inline
func (b *Bytes) ElemSize(idx int) int64 {
	if !b.elements[idx].nonInlined {
		return ElementSize
	}
	return ElementSize + int64(cap(b.elements[idx].data))
}

// Abbreviated returns a uint64 slice where each uint64 represents the first
// eight bytes of each []byte. It is used for byte comparison fast paths.
//
// Given Bytes b, and abbr = b.Abbreviated():
//
//   - abbr[i] > abbr[j] iff b.Get(i) > b.Get(j)
//   - abbr[i] < abbr[j] iff b.Get(i) < b.Get(j)
//   - If abbr[i] == abbr[j], it is unknown if b.Get(i) is greater than, less
//     than, or equal to b.Get(j). A full comparison of all bytes in each is
//     required.
//
func (b *Bytes) Abbreviated() []uint64 {
	r := make([]uint64, b.Len())
	for i := range r {
		bs := b.Get(i)
		r[i] = abbreviate(bs)
	}
	return r
}

// abbreviate interprets up to the first 8 bytes of the slice as a big-endian
// uint64. If the slice has less than 8 bytes, the value returned is the same as
// if the slice was filled to 8 bytes with zero value bytes. For example:
//
//   abbreviate([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01})
//     => 1
//
//   abbreviate([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00})
//     => 256
//
//   abbreviate([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01})
//     => 256
//
func abbreviate(bs []byte) uint64 {
	if len(bs) >= 8 {
		return binary.BigEndian.Uint64(bs)
	}
	var v uint64
	for _, b := range bs {
		v <<= 8
		v |= uint64(b)
	}
	return v << uint(8*(8-len(bs)))
}

var zeroElements = make([]element, MaxBatchSize)

// zero zeroes out all elements in b starting from position startIdx until the
// first unset element.
//gcassert:inline
func (b *Bytes) zero(startIdx int) {
	for n := startIdx; n < b.firstUnsetIdx; {
		n += copy(b.elements[n:b.firstUnsetIdx], zeroElements)
	}
}

// Reset resets the underlying Bytes for reuse. It is a noop if b is a window
// into another Bytes.
//
// Calling it is not required for correctness and is only an optimization.
// Namely, this allows us to remove all "holes" (unused space) in b.buffer which
// can occur when an old non-inlined element is overwritten by a new element
// that is either fully-inlined or non-inlined but larger.
func (b *Bytes) Reset() {
	if b.isWindow {
		return
	}
	b.zero(0 /* startIdx */)
	b.buffer = b.buffer[:0]
	b.firstUnsetIdx = 0
}

// String is used for debugging purposes.
func (b *Bytes) String() string {
	var builder strings.Builder
	for i := range b.elements {
		builder.WriteString(
			fmt.Sprintf("%d: %v\n", i, b.Get(i)),
		)
	}
	return builder.String()
}

// BytesFromArrowSerializationFormat takes an Arrow byte slice and accompanying
// offsets and populates b.
func BytesFromArrowSerializationFormat(b *Bytes, data []byte, offsets []int32) {
	numElements := len(offsets) - 1
	if cap(b.elements) < numElements {
		b.elements = make([]element, numElements)
		b.buffer = b.buffer[:0]
		b.firstUnsetIdx = 0
	} else {
		b.Reset()
		b.elements = b.elements[:numElements]
	}
	for i := 0; i < numElements; i++ {
		b.Set(i, data[offsets[i]:offsets[i+1]])
	}
}

// ToArrowSerializationFormat returns a bytes slice and offsets that are
// Arrow-compatible. n is the number of elements to serialize.
func (b *Bytes) ToArrowSerializationFormat(n int) ([]byte, []int32) {
	// Calculate the size of the flat byte slice that will contain all elements.
	var dataSize int
	for _, e := range b.elements[:n] {
		dataSize += e.len()
	}
	data := make([]byte, 0, dataSize)
	offsets := make([]int32, 1, n+1)
	for _, e := range b.elements[:n] {
		data = append(data, e.get()...)
		offsets = append(offsets, int32(len(data)))
	}
	return data, offsets
}

// ProportionalSize calls the method of the same name on bytes-like vectors,
// panicking if not bytes-like.
func ProportionalSize(v Vec, length int64) int64 {
	family := v.CanonicalTypeFamily()
	switch family {
	case types.BytesFamily:
		return v.Bytes().ProportionalSize(length)
	case types.JsonFamily:
		return v.JSON().ProportionalSize(length)
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unsupported type %s", family))
	}
	return 0
}

// ResetIfBytesLike calls Reset on v if it is bytes-like, noop otherwise.
func ResetIfBytesLike(v Vec) {
	switch v.CanonicalTypeFamily() {
	case types.BytesFamily:
		v.Bytes().Reset()
	case types.JsonFamily:
		v.JSON().Reset()
	}
}
