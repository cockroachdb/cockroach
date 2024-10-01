// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
// BytesMaxInlineLength (30 bytes on 64 bit systems), then the whole value is
// stored within the element; otherwise, the element "points" at a region in
// Bytes.buffer where the non-inlined value is stored.
//
// If the value is inlined, then the layout of element is used as follows:
//
//	         24-byte header | 6-byte padding
//	element: .............................. | length | true
//	Bytes.buffer: N/A
//
// where 30 dots describe the inlinable space followed by a single byte for the
// length followed by a boolean 'true' indicating an inlined value.
//
// If the value is non-inlined, then the layout of element is used as follows:
//
//	                                          padding
//	element: .offset. | ..len... | ..cap... | xxxxxx | x | false
//	Bytes.buffer: xxxxxxxx | offset .... | xxxxxxxx
//
// where first 24 bytes contain our custom "header" of a byte slice that is
// backed by Bytes.buffer. The following 7 bytes (the padding and the
// inlinedLength byte) are not used, and then we have a boolean 'false'
// indicating a non-inlined value.
type element struct {
	header sliceHeader
	_      [6]byte
	// inlinedLength contains the length of the []byte value if it is inlined.
	// Since the length of that value is at most BytesMaxInlineLength (30), it
	// can definitely fit into a single byte.
	inlinedLength byte
	// inlined indicates whether the byte value is inlined in the element.
	inlined bool
}

// sliceHeader describes a non-inlined byte value that is backed by
// Bytes.buffer.
//
// We do not just use []byte for two reasons:
// 1. we can easily overwrite the whole struct when a value is inlined without
// causing any issues with the GC (because our sliceHeader doesn't contain an
// actual slice, GC doesn't have to track whether the backing array is live or
// not);
// 2. our slice header remains valid even when Bytes.buffer is reallocated. If
// we were to use []byte, then whenever that reallocation occurred, we would
// have to iterate over all non-inlined values and update the byte slices to
// point to the new Bytes.buffer.
type sliceHeader struct {
	// bufferOffset stores the offset into Bytes.buffer where the corresponding
	// bytes value begins.
	bufferOffset int
	len          int
	cap          int
}

// ElementSize is the size of element object. It is expected to be 32 on a 64
// bit system.
const ElementSize = int64(unsafe.Sizeof(element{}))

// BytesMaxInlineLength is the maximum length of a []byte that can be inlined
// within element.
const BytesMaxInlineLength = int(unsafe.Offsetof(element{}.inlinedLength))

// inlinedSlice returns 30 bytes of space within e that can be used for storing
// a value inlined, as a slice.
//
//gcassert:inline
func (e *element) inlinedSlice() []byte {
	return (*(*[BytesMaxInlineLength]byte)(unsafe.Pointer(&e.header)))[:]
}

//gcassert:inline
func (e *element) get(b *Bytes) []byte {
	if e.inlined {
		return e.inlinedSlice()[:e.inlinedLength:BytesMaxInlineLength]
	}
	return e.getNonInlined(b)
}

//gcassert:inline
func (e *element) getNonInlined(b *Bytes) []byte {
	return b.buffer[e.header.bufferOffset : e.header.bufferOffset+e.header.len : e.header.bufferOffset+e.header.cap]
}

func (e *element) set(v []byte, b *Bytes) {
	if len(v) <= BytesMaxInlineLength {
		*e = element{inlinedLength: byte(len(v)), inlined: true}
		copy(e.inlinedSlice(), v)
	} else {
		e.setNonInlined(v, b)
	}
}

// copy copies the value contained in other into the receiver.
func (e *element) copy(other element, dest, src *Bytes) {
	if other.inlined {
		*e = other
		return
	}
	e.setNonInlined(other.getNonInlined(src), dest)
}

func (e *element) setNonInlined(v []byte, b *Bytes) {
	// Check if we there was an old non-inlined value we can overwrite.
	if !e.inlined && e.header.cap >= len(v) {
		e.header.len = len(v)
		copy(e.getNonInlined(b), v)
	} else {
		*e = element{
			header: sliceHeader{
				bufferOffset: len(b.buffer),
				len:          len(v),
				cap:          len(v),
			},
			inlined: false,
		}
		// Use a custom append to grow the buffer faster than go does by default.
		if rem := cap(b.buffer) - len(b.buffer); rem < len(v) {
			increment := cap(b.buffer)                  // at least double the buffer
			if need := len(v) - rem; increment < need { // grow enough to fit v
				increment = need
			}
			const initialBufferSize = 256 // don't go smaller than this
			if increment < initialBufferSize {
				increment = initialBufferSize
			}
			realloc := make([]byte, len(b.buffer), cap(b.buffer)+increment)
			copy(realloc, b.buffer)
			b.buffer = realloc
		}
		b.buffer = append(b.buffer, v...)
	}
}

// Bytes is a vector that stores []byte values.
type Bytes struct {
	// elements contains all values stored in this Bytes vector.
	elements []element
	// buffer contains all values that couldn't be inlined within the
	// corresponding elements.
	buffer []byte
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
// If the returned value is then Set() into another Bytes, then use Bytes.Copy
// instead.
//
// Note this function call is mostly inlined except in a handful of very large
// generated functions, so we can't add the gcassert directive for it.
func (b *Bytes) Get(i int) []byte {
	return b.elements[i].get(b)
}

// Set sets the ith []byte in Bytes.
//
// If the provided value is obtained via Get() from another Bytes, then use
// Bytes.Copy instead.
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
}

// Window creates a "window" into the receiver. It behaves similarly to Golang's
// slice, but the returned object is *not* allowed to be modified - it is
// read-only. If b is modified, then the returned object becomes invalid.
//
// Window is a lightweight operation that doesn't involve copying the underlying
// data.
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
		buffer:   b.buffer,
		isWindow: true,
	}
}

// Copy copies a single value from src at position srcIdx into position destIdx
// of the receiver. It is faster than b.Set(destIdx, src.Get(srcIdx)).
func (b *Bytes) Copy(src *Bytes, destIdx, srcIdx int) {
	if buildutil.CrdbTestBuild {
		if b.isWindow {
			panic("copy is called on a window into Bytes")
		}
	}
	b.elements[destIdx].copy(src.elements[srcIdx], b, src)
}

// copyElements copies the provided elements into the receiver starting at
// position destIdx. The method assumes that there are enough elements in the
// receiver in [destIdx:] range to fit all the source elements.
func (b *Bytes) copyElements(srcElementsToCopy []element, src *Bytes, destIdx int) {
	if len(srcElementsToCopy) == 0 {
		return
	}
	destElements := b.elements[destIdx:]
	if buildutil.CrdbTestBuild {
		if len(destElements) < len(srcElementsToCopy) {
			panic(errors.AssertionFailedf("unexpectedly not enough destination elements"))
		}
	}
	// Optimize copying of the elements by copying all of them directly into the
	// destination. This way all inlined values become correctly set, and we
	// only need to set the non-inlined values separately.
	//
	// Note that this behavior results in losing the references to the old
	// non-inlined values, even if they could be reused. If Bytes is not Reset,
	// then that unused space in Bytes.buffer can accumulate. However, checking
	// whether there are old non-inlined values with non-zero capacity leads to
	// performance regressions, and in the production code we do reset the Bytes
	// in all cases, so we accept this poor behavior in such a hypothetical /
	// test-only scenario. See #78703 for more details.
	copy(destElements, srcElementsToCopy)
	// Early bounds checks.
	_ = destElements[len(srcElementsToCopy)-1]
	for i := 0; i < len(srcElementsToCopy); i++ {
		//gcassert:bce
		if e := &destElements[i]; !e.inlined {
			// This value is non-inlined, so at the moment it is pointing
			// into the buffer of the source - we have to explicitly set it
			// in order to copy the actual value's data into the buffer of
			// b.
			//
			// First, unset the element so that we don't try to reuse the old
			// non-inlined value to write the new value into - we do want to
			// append the new value to b.buffer.
			*e = element{}
			//gcassert:bce
			srcElement := srcElementsToCopy[i]
			e.setNonInlined(srcElement.getNonInlined(src), b)
		}
	}
}

// TODO(yuzefovich): unexport some of the methods (like CopySlice and
// AppendSlice).

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

	if b != src {
		srcElementsToCopy := src.elements[srcStartIdx : srcStartIdx+toCopy]
		b.copyElements(srcElementsToCopy, src, destIdx)
	} else if destIdx > srcStartIdx {
		// If we're copying from the vector into itself and from the earlier
		// part into the later part, we need to reverse the order (otherwise we
		// would overwrite the later values before copying them, thus corrupting
		// the vector).
		for i := toCopy - 1; i >= 0; i-- {
			b.elements[destIdx+i].copy(src.elements[srcStartIdx+i], b, src)
		}
	} else {
		for i := 0; i < toCopy; i++ {
			b.elements[destIdx+i].copy(src.elements[srcStartIdx+i], b, src)
		}
	}
}

// ensureLengthForAppend makes sure that b has enough elements to support newLen
// as the new length of the vector for an append operation.
func (b *Bytes) ensureLengthForAppend(destIdx, newLen int) {
	if cap(b.elements) < newLen {
		// If the elements slice doesn't have enough capacity, then it must be
		// reallocated.
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
		b.elements = b.elements[:newLen]
	}
}

// AppendSlice appends srcStartIdx inclusive and srcEndIdx exclusive []byte
// values from src into the receiver starting at destIdx.
func (b *Bytes) AppendSlice(src *Bytes, destIdx, srcStartIdx, srcEndIdx int) {
	if b == src {
		panic("AppendSlice when b == src is not supported")
	}
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
	b.ensureLengthForAppend(destIdx, newLen)
	b.copyElements(srcElementsToCopy, src, destIdx)
}

// appendSliceWithSel appends all values specified in sel from the source into
// the receiver starting at position destIdx.
func (b *Bytes) appendSliceWithSel(src *Bytes, destIdx int, sel []int) {
	if b == src {
		panic("appendSliceWithSel when b == src is not supported")
	}
	if b.isWindow {
		panic("appendSliceWithSel is called on a window into Bytes")
	}
	if destIdx < 0 || destIdx > b.Len() {
		panic(
			fmt.Sprintf(
				"dest index %d out of range (len=%d)", destIdx, b.Len(),
			),
		)
	}
	newLen := destIdx + len(sel)
	b.ensureLengthForAppend(destIdx, newLen)
	for i, srcIdx := range sel {
		b.elements[destIdx+i].copy(src.elements[srcIdx], b, src)
	}
}

// Len returns how many []byte values the receiver contains.
//
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
//
//gcassert:inline
func (b *Bytes) ElemSize(idx int) int64 {
	if b.elements[idx].inlined {
		return ElementSize
	}
	return ElementSize + int64(b.elements[idx].header.cap)
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
//	abbreviate([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01})
//	  => 1
//
//	abbreviate([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00})
//	  => 256
//
//	abbreviate([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01})
//	  => 256
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

// Reset resets the underlying Bytes for reuse. It is a noop if b is a window
// into another Bytes.
//
// Calling it is not required for correctness and is only an optimization.
// Namely, this allows us to remove all "holes" (unused space) in b.buffer which
// can occur when an old non-inlined element is overwritten by a new element
// that is either fully-inlined or non-inlined but larger.
//
//gcassert:inline
func (b *Bytes) Reset() {
	if b.isWindow {
		return
	}
	// Zero out all elements, up to the capacity, and then restore the length of
	// the vector.
	l := len(b.elements)
	b.elements = b.elements[:cap(b.elements)]
	for n := 0; n < len(b.elements); {
		n += copy(b.elements[n:], zeroElements)
	}
	b.elements = b.elements[:l]
	b.buffer = b.buffer[:0]
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

// elementsAsBytes unsafely casts b.elements[:n] to []byte.
func (b *Bytes) elementsAsBytes(n int) []byte {
	var bytes []byte
	//lint:ignore SA1019 SliceHeader is deprecated, but no clear replacement
	elementsHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b.elements))
	//lint:ignore SA1019 SliceHeader is deprecated, but no clear replacement
	bytesHeader := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))
	bytesHeader.Data = elementsHeader.Data
	bytesHeader.Len = int(ElementSize) * n
	bytesHeader.Cap = int(ElementSize) * n
	return bytes
}

var zeroInt32Slice []int32

func init() {
	zeroInt32Slice = make([]int32, MaxBatchSize)
}

// Serialize converts b into the "arrow-like" (which is arrow-compatible)
// format.
//
// We call this "arrow-like" because we're abusing the arrow format to get the
// best speed, possibly at the cost of increased allocations (when Bytes vector
// has been modified in-place many times via Sets at arbitrary positions with
// values of different lengths).
//
// In particular, the arrow format represents bytes values via two slices - the
// flat []byte buffer and the offsets where len(offsets) = n + 1 (where n is the
// number of elements). ith element is then buffer[offsets[i]:offsets[i+1].
// However, we squash b.elements (which is []element) and b.buffer to be stored
// in that flat byte slice, and we only need two positions in offsets to
// indicate the boundary between the two as well as the total data length. As a
// result, we have the following representation (which defeats the spirit of the
// arrow format but doesn't cause any issues anywhere):
//
//	 buffer = [<b.elements as []byte><b.buffer]
//	offsets = [0, 0, ..., 0, len(<b.elements as []byte>), len(<b.elements as []byte>) + len(buffer)]
//
// Note: it is assumed that n is not larger than MaxBatchSize.
func (b *Bytes) Serialize(n int, dataScratch []byte, offsetsScratch []int32) ([]byte, []int32) {
	if buildutil.CrdbTestBuild {
		if n > MaxBatchSize {
			colexecerror.InternalError(errors.AssertionFailedf(
				"too many bytes elements to serialize: %d vs MaxBatchSize of %d", n, MaxBatchSize,
			))
		}
	}
	data := dataScratch[:0]
	offsets := offsetsScratch[:0]

	// Handle the cases of 0 and 1 elements separately since then we cannot
	// store two offsets that we need.
	if n == 0 {
		offsets = append(offsets, 0)
		return data, offsets
	} else if n == 1 {
		data = append(data, b.Get(0)...)
		offsets = append(offsets, 0)
		offsets = append(offsets, int32(len(data)))
		return data, offsets
	}

	// Copy over b.elements treated as []byte as well as b.buffer into data.
	bytes := b.elementsAsBytes(n)
	if cap(data) < len(bytes)+len(b.buffer) {
		data = make([]byte, 0, len(bytes)+len(b.buffer))
	}
	data = append(data, bytes...)
	data = append(data, b.buffer...)

	// Now populate the offsets slice which conforms to the arrow format and has
	// the correct length.
	offsets = append(offsets, zeroInt32Slice[:n-1]...)
	offsets = append(offsets, int32(len(bytes)))
	offsets = append(offsets, int32(len(data)))
	return data, offsets
}

// Deserialize updates b according to the "arrow-like" format that was produced
// by Serialize.
func (b *Bytes) Deserialize(data []byte, offsets []int32) {
	n := len(offsets) - 1
	if cap(b.elements) < n {
		b.elements = make([]element, n)
	} else {
		b.elements = b.elements[:n]
	}
	b.buffer = b.buffer[:0]
	if n == 0 {
		return
	} else if n == 1 {
		b.elements[0] = element{}
		b.Set(0, data)
		return
	}
	bytes := b.elementsAsBytes(n)
	copy(bytes, data)
	b.buffer = append(b.buffer, data[len(bytes):]...)
}

// ProportionalSize calls the method of the same name on bytes-like vectors,
// panicking if not bytes-like.
func ProportionalSize(v *Vec, length int64) int64 {
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
func ResetIfBytesLike(v *Vec) {
	switch v.CanonicalTypeFamily() {
	case types.BytesFamily:
		v.Bytes().Reset()
	case types.JsonFamily:
		v.JSON().Reset()
	}
}
