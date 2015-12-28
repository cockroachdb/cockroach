// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package encoding

import (
	"bytes"
	"io"
	"math"
	"reflect"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/util"
)

const (
	encodedNull = 0x00
	// A marker greater than NULL but lower than any other value.
	// This value is not actually ever present in a stored key, but
	// it's used in keys used as span boundaries for index scans.
	encodedNotNull = 0x01

	floatNaN              = encodedNotNull + 1
	floatNegativeInfinity = floatNaN + 1
	floatNegLarge         = floatNegativeInfinity + 1
	floatNegMedium        = floatNegLarge + 11
	floatNegSmall         = floatNegMedium + 1
	floatZero             = floatNegSmall + 1
	floatPosSmall         = floatZero + 1
	floatPosMedium        = floatPosSmall + 1
	floatPosLarge         = floatPosMedium + 11
	floatInfinity         = floatPosLarge + 1
	floatTerminator       = 0x00

	bytesMarker byte = floatInfinity + 1
	timeMarker  byte = bytesMarker + 1

	// IntMin is chosen such that the range of int tags does not overlap the
	// ascii character set that is frequently used in testing.
	IntMin   = 0x80
	intZero  = IntMin + 8
	intSmall = IntMax - intZero - 8 // 111
	// IntMax is the maximum int tag value.
	IntMax = 0xff
)

// Direction for ordering results.
type Direction int

// Direction values.
const (
	DefaultDirection Direction = iota
	Ascending
	Descending
)

type Reader interface {
	// Read returns a slice of up to `n` bytes.
	// If `n` bytes are not available, a io.EOF will be returned as the error.
	// Attention: the returned slice may be using the same storage as the Reader's,
	// so it should generally be treated as read-only.
	Read(n int) ([]byte, error)
	// !!! document that r is pointer so we can pass nil to signify no user buffer.
	// !!! also document that we always return the new slice, although we also modify
	// the pointer if passed.
	ReadInto(n int, r *[]byte) ([]byte, error)
	ReadByte() (byte, error)
	PeekByte() (byte, error)
	RawBytesRemaining() []byte
	EOF() bool
}

type CachingReader struct {
	s        []byte
	i        int
	dir      Direction // !!! this needs to be an array and we need an advance function
	inverted []byte
}

func NewCachingReader(s []byte) *CachingReader {
	reader := CachingReader{s: s, i: 0, dir: Ascending}
	reader.inverted = append([]byte(nil), s...)
	onesComplement(reader.inverted)
	return &reader
}

func (r *CachingReader) EOF() bool {
	return r.i >= len(r.s)
}

func (r *CachingReader) Read(n int) ([]byte, error) {
	if n > len(r.s) {
		return nil, io.EOF
	}
	var res []byte
	if r.dir != Ascending {
		res = r.inverted[r.i : r.i+n]
	} else {
		res = r.s[r.i : r.i+n]
	}
	r.i += n
	return res, nil
}

func (r *CachingReader) ReadByte() (byte, error) {
	if r.EOF() {
		return 0, io.EOF
	}
	b := r.s[r.i]
	r.i++
	if r.dir != Descending {
		return b, nil
	} else {
		return ^b, nil
	}
}

func (r *CachingReader) PeekByte() (byte, error) {
	if r.EOF() {
		return 0, io.EOF
	}
	b := r.s[r.i]
	if r.dir != Descending {
		return b, nil
	} else {
		return ^b, nil
	}
}

func (r *CachingReader) Reset(s []byte) {
	r.s = s
	r.i = 0
	r.dir = Ascending
	//r.inverted = append([]byte(nil), s...)
	//onesComplement(r.inverted)
}

// !!!
type RReader struct {
	s     []byte
	i     int
	dir   Direction // !!! this needs to be an array and we need an advance function
	slice []byte
}

func NewRReader(s []byte) *RReader {
	return &RReader{s: s, i: 0, dir: Ascending, slice: nil}
}

func (r *RReader) Reset(s []byte) {
	r.s = s
	r.i = 0
	r.slice = nil
}

func (r *RReader) RawBytesRemaining() []byte {
	return r.s[r.i:]
}

func (r *RReader) EOF() bool {
	return r.i >= len(r.s)
}

type UnsafeArray [0x7fffffff]byte
type UnsafeWordArray [0x7fffffff]uintptr

func onesComplementXXX(b *UnsafeArray, n int) {
	w := n / wordSize
	if w > 0 {
		bw := (*UnsafeWordArray)(unsafe.Pointer(b))
		for i := 0; i < w; i++ {
			bw[i] = ^bw[i]
		}
	}

	for i := w * wordSize; i < n; i++ {
		b[i] = ^b[i]
	}
}

func (r *RReader) ReadAtMost(n int) (*UnsafeArray, int) {
	if n > len(r.s) {
		n = len(r.s)
	}
	var ptr *UnsafeArray
	ptr = (*UnsafeArray)(unsafe.Pointer(&r.s[r.i]))
	if r.dir == Descending {
		onesComplementXXX(ptr, n)
		// !!! try an unsafe copy here by casting to uintptr, or just a for loop
		//r.slice = r.slice[:0]
		//r.slice = append(r.slice, ptr[:n]...)
		//onesComplement(r.slice)
		//ptr = (*UnsafeArray)(unsafe.Pointer(&r.slice[0]))
	}
	r.i += n
	return ptr, n
}

func (r *RReader) Read(n int) (*UnsafeArray, error) {
	if n > len(r.s) {
		return nil, io.EOF
	}
	var ptr *UnsafeArray
	ptr = (*UnsafeArray)(unsafe.Pointer(&r.s[r.i]))
	if r.dir == Descending {
		onesComplementXXX(ptr, n)
		//r.slice = r.slice[:0]
		//r.slice = append(r.slice, ptr[:n]...)
		//onesComplement(r.slice)
		//ptr = (*UnsafeArray)(unsafe.Pointer(&r.slice[0]))
	}
	r.i += n
	return ptr, nil
}

func (r *RReader) ReadByte() (byte, error) {
	//if r.EOF() {
	//  return 0, io.EOF
	//}
	b := r.s[r.i]
	r.i++
	return b, nil
	//if r.dir != Descending {
	//  return b, nil
	//} else {
	//  return ^b, nil
	//}
}

//func (r *RReader) ReadByte() (byte, error) {
//  if r.EOF() {
//    return 0, io.EOF
//  }
//  b := r.s[r.i]
//  r.i++
//  if r.dir != Descending {
//    return b, nil
//  } else {
//    return ^b, nil
//  }
//}

func (r *RReader) PeekByte() (byte, error) {
	if r.EOF() {
		return 0, io.EOF
	}
	b := r.s[r.i]
	if r.dir != Descending {
		return b, nil
	} else {
		return ^b, nil
	}
}

// BufferReader is a reader that consumes a []byte.
type BufferReader struct {
	// The buffer that we're reading from. The slice is always resliced to
	// represent unread bytes.
	s []byte
}

func NewBufferReader(s []byte) *BufferReader {
	return &BufferReader{s: s}
}

func (r *BufferReader) RawBytesRemaining() []byte {
	return r.s
}

/* !!!
func (r *BufferReader) CompareToKey() int {
	return bytes.Compare(r.RawBytesRemaining())
}
*/

func (r *BufferReader) EOF() bool {
	return len(r.s) == 0
}

// !!!
var aa = []byte{1, 2, 3, 4}

func (r *BufferReader) Read(n int) ([]byte, error) {
	//return r.ReadInto(n, nil)

	//if n > len(r.s) {
	//  n = len(r.s)
	//  if n <= 0 {
	//    return []byte{}, io.EOF
	//  }
	//}

	//return []byte{1, 2, 3, 4}, nil

	slice := r.s[:n]
	//_ = r.s[n:]
	aa = r.s[n:]
	//r.s = r.s[n:]
	return slice, nil
}

func (r *BufferReader) ReadInto(n int, buf *[]byte) ([]byte, error) {
	var err error
	if n > len(r.s) {
		err = io.EOF
		n = len(r.s)
		if n <= 0 {
			return []byte{}, io.EOF
		}
	}
	var slice []byte
	if buf == nil {
		slice = r.s[:n]
	} else {
		slice = append(*buf, r.s[:n]...)
		*buf = slice
	}
	r.s = r.s[n:]
	return slice, err
}

func (r *BufferReader) ReadByte() (byte, error) {
	if r.EOF() {
		return 0, io.EOF
	}
	b := r.s[0]
	r.s = r.s[1:]
	return b, nil
}

func (r *BufferReader) PeekByte() (byte, error) {
	if r.EOF() {
		return 0, io.EOF
	}
	return r.s[0], nil
}

// Reader on top of a SQL encoded key.
// This reader is aware of the encoding directions of the
// column constituents.
type KeyReader struct {
	// The buffer that we're reading from. The slice is always resliced to
	// represent unread bytes.
	s []byte
	// The direction with which the bytes corresponding to the
	// various columns have been written.
	dirs []Direction
	// The index of the direction of the column `i`
	// is positioned on.
	dirIdx int       // !!! get rid of this and just reslice dirs
	dir    Direction // dirs[dirIdx]
}

func NewKeyReader(s []byte, dirs []Direction) *KeyReader {
	return &KeyReader{s: s, dirs: dirs, dirIdx: 0, dir: dirs[0]}
}

func (r *KeyReader) AdvanceColumn() error {
	r.dirIdx++
	if r.dirIdx == len(r.dirs) {
		return io.EOF
	}
	if r.dirIdx > len(r.dirs) {
		return util.Errorf("No more columns to read.")
	}
	r.dir = r.dirs[r.dirIdx]
	return nil
}

func (r *KeyReader) RawBytesRemaining() []byte {
	return r.s
}

func (r *KeyReader) EOF() bool {
	return len(r.s) == 0
}

/* !!!
func (r *KeyReader) CompareToKey() int {
	if r.dir == Descending {
		cpy := make([]byte, len(res))
		copy(cpy, res)
		onesComplement(cpy)

	} else {
		return bytes.Compare(r.RawBytesRemaining())
	}
}
*/

func (r *KeyReader) Read(n int) ([]byte, error) {
	return r.ReadInto(n, nil)
}

func (r *KeyReader) ReadInto(n int, buf *[]byte) ([]byte, error) {
	var err error
	if n > len(r.s) {
		err = io.EOF
		n = len(r.s)
		if n == 0 {
			return []byte{}, io.EOF
		}
	}
	tmp := r.s[:n]
	r.s = r.s[n:]
	// tmp has the new bytes we want to return. We might want to copy them to
	// buf, we might want to invert them.
	if buf != nil {
		*buf = append(*buf, tmp...)
		tmp = (*buf)[len(*buf)-n:]
	}
	if r.dir == Descending {
		// If we were using r's storage, we need to allocate a new slice.
		var complementSlice []byte
		if buf != nil {
			complementSlice = tmp
		} else {
			complementSlice = make([]byte, n)
			copy(complementSlice, tmp)
		}
		onesComplement(complementSlice)
		if buf != nil {
			return *buf, err
		} else {
			return complementSlice, err
		}
	} else {
		if buf != nil {
			return *buf, err
		} else {
			return tmp, err
		}
	}

	/* !!!!
	// `slice` will point to a slice
	var slice *[]byte
	if buf != nil {
		slice = buf
		fmt.Printf("!!! ReadInto. about to read into chars: %v\n", r.s[:n])
		*buf = append(*slice, r.s[:n]...)
	} else {
		tmp := r.s[:n]
		slice = &tmp
	}
	r.s = r.s[n:]
	fmt.Printf("!!! ReadInto. after ReadInto the remaining slice is: %v\n", r.s)
	if r.dir == Descending {
		// If we were using r's storage, we need to allocate a new slice.
		var complementSlice []byte
		if buf == nil {
			complementSlice = make([]byte, n)
			copy(complementSlice, *slice)
		} else {
			complementSlice = *slice
		}
		onesComplement(complementSlice)
		return complementSlice, err
	}
	return *slice, err
	*/
}

func (r *KeyReader) ReadByte() (byte, error) {
	if r.EOF() {
		return 0, io.EOF
	}
	b := r.s[0]
	if r.dir == Descending {
		b = ^b
	}
	r.s = r.s[1:]
	return b, nil
}

func (r *KeyReader) PeekByte() (byte, error) {
	b, err := r.PeekByteAbsolute()
	if r.dir == Descending {
		b = ^b
	}
	return b, err
}

// !!! is this needed?
// PeekByteAbsolute reads one byte ignoring the KeyReader's direction.
// Useful for marker bytes that are direction-agnostic.
func (r *KeyReader) PeekByteAbsolute() (byte, error) {
	if r.EOF() {
		return 0, io.EOF
	}
	return r.s[0], nil
}

// EncodeUint32 encodes the uint32 value using a big-endian 8 byte
// representation. The bytes are appended to the supplied buffer and
// the final buffer is returned.
func EncodeUint32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// EncodeUint32Decreasing encodes the uint32 value so that it sorts in
// reverse order, from largest to smallest.
func EncodeUint32Decreasing(b []byte, v uint32) []byte {
	l := len(b)
	b = EncodeUint32(b, v)
	onesComplement(b[l:])
	return b
}

// DecodeUint32 decodes a uint32 from the input buffer, treating
// the input as a big-endian 4 byte uint32 representation.
func DecodeUint32(r Reader) (uint32, error) {
	var b []byte
	var err error
	if b, err = r.Read(4); err != nil {
		return 0, util.Errorf("insufficient bytes to decode uint32 int value")
	}
	v := (uint32(b[0]) << 24) | (uint32(b[1]) << 16) |
		(uint32(b[2]) << 8) | uint32(b[3])
	return v, nil
}

// EncodeUint64 encodes the uint64 value using a big-endian 8 byte
// representation. The bytes are appended to the supplied buffer and
// the final buffer is returned.
func EncodeUint64(b []byte, v uint64) []byte {
	return append(b,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// EncodeUint64Decreasing encodes the uint64 value so that it sorts in
// reverse order, from largest to smallest.
func EncodeUint64Decreasing(b []byte, v uint64) []byte {
	l := len(b)
	b = EncodeUint64(b, v)
	onesComplement(b[l:])
	return b
}

// DecodeUint64 decodes a uint64 from the input buffer, treating
// the input as a big-endian 8 byte uint64 representation.
func DecodeUint64(r Reader) (uint64, error) {
	var b []byte
	var err error
	if b, err = r.Read(8); err != nil {
		return 0, util.Errorf("insufficient bytes to decode uint64 int value")
	}
	v := (uint64(b[0]) << 56) | (uint64(b[1]) << 48) |
		(uint64(b[2]) << 40) | (uint64(b[3]) << 32) |
		(uint64(b[4]) << 24) | (uint64(b[5]) << 16) |
		(uint64(b[6]) << 8) | uint64(b[7])
	return v, nil
}

// !!!
func EncodeVarintWithDir(b []byte, v int64, dir Direction) []byte {
	if dir == Ascending {
		return EncodeVarint(b, v)
	} else {
		return EncodeVarintDecreasing(b, v)
	}
}

// EncodeVarint encodes the int64 value using a variable length
// (length-prefixed) representation. The length is encoded as a single
// byte. If the value to be encoded is negative the length is encoded
// as 8-numBytes. If the value is positive it is encoded as
// 8+numBytes. The encoded bytes are appended to the supplied buffer
// and the final buffer is returned.
func EncodeVarint(b []byte, v int64) []byte {
	if v < 0 {
		switch {
		case v >= -0xff:
			return append(b, IntMin+7, byte(v))
		case v >= -0xffff:
			return append(b, IntMin+6, byte(v>>8), byte(v))
		case v >= -0xffffff:
			return append(b, IntMin+5, byte(v>>16), byte(v>>8), byte(v))
		case v >= -0xffffffff:
			return append(b, IntMin+4, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		case v >= -0xffffffffff:
			return append(b, IntMin+3, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8),
				byte(v))
		case v >= -0xffffffffffff:
			return append(b, IntMin+2, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),
				byte(v>>8), byte(v))
		case v >= -0xffffffffffffff:
			return append(b, IntMin+1, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24),
				byte(v>>16), byte(v>>8), byte(v))
		default:
			return append(b, IntMin, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
				byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		}
	}

	return EncodeUvarint(b, uint64(v))
}

// EncodeVarintDecreasing encodes the int64 value so that it sorts in reverse
// order, from largest to smallest.
func EncodeVarintDecreasing(b []byte, v int64) []byte {
	l := len(b)
	b = EncodeVarint(b, v)
	onesComplement(b[l:])
	return b
}

// DecodeVarint decodes a varint encoded int64 from the input
// key.
func DecodeVarint(r Reader) (int64, error) {
	var b byte
	var length int
	var err error
	if b, err = r.PeekByte(); err != nil {
		return 0, err
	}
	length = int(b) - intZero
	if length < 0 {
		length = -length
		r.ReadByte()
		var remB []byte
		remB, err = r.Read(length)
		if err == io.EOF {
			return 0, util.Errorf("insufficient bytes to decode var uint64 int value: %s", remB)
		}
		var v int64
		// Use the ones-complement of each encoded byte in order to build
		// up a positive number, then take the ones-complement again to
		// arrive at our negative value.
		for _, t := range remB {
			v = (v << 8) | int64(^t)
		}
		return ^v, nil
	}

	v, err := DecodeUvarint(r)
	if err != nil {
		return 0, err
	}
	if v > math.MaxInt64 {
		return 0, util.Errorf("varint %d overflows int64", v)
	}
	return int64(v), nil
}

// EncodeUvarint encodes the uint64 value using a variable length
// (length-prefixed) representation. The length is encoded as a single
// byte indicating the number of encoded bytes (-8) to follow. See
// EncodeVarint for rationale. The encoded bytes are appended to the
// supplied buffer and the final buffer is returned.
func EncodeUvarint(b []byte, v uint64) []byte {
	switch {
	case v <= intSmall:
		return append(b, intZero+byte(v))
	case v <= 0xff:
		return append(b, IntMax-7, byte(v))
	case v <= 0xffff:
		return append(b, IntMax-6, byte(v>>8), byte(v))
	case v <= 0xffffff:
		return append(b, IntMax-5, byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffff:
		return append(b, IntMax-4, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffffff:
		return append(b, IntMax-3, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8),
			byte(v))
	case v <= 0xffffffffffff:
		return append(b, IntMax-2, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),
			byte(v>>8), byte(v))
	case v <= 0xffffffffffffff:
		return append(b, IntMax-1, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24),
			byte(v>>16), byte(v>>8), byte(v))
	default:
		return append(b, IntMax, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
}

// EncodeUvarintDecreasing encodes the uint64 value so that it sorts in
// reverse order, from largest to smallest.
func EncodeUvarintDecreasing(b []byte, v uint64) []byte {
	l := len(b)
	b = EncodeUvarint(b, v)
	onesComplement(b[l:])
	return b
}

// DecodeUvarint decodes a varint encoded uint64 from the input
// key.
func DecodeUvarint(r Reader) (uint64, error) {
	var b byte
	var length int
	var err error
	if b, err = r.ReadByte(); err != nil {
		return 0, err
	}
	length = int(b) - intZero
	if length <= intSmall {
		return uint64(length), nil
	}
	length -= intSmall
	var buf []byte
	if length < 0 || length > 8 {
		return 0, util.Errorf("invalid uvarint length of %d", length)
	} else {
		buf, err = r.Read(length)
		if err == io.EOF {
			return 0, util.Errorf("insufficient bytes to decode var uint64 int value: %v", buf)
		}
	}
	var v uint64
	// It is faster to range over the elements in a slice than to index
	// into the slice on each loop iteration.
	for _, t := range buf {
		v = (v << 8) | uint64(t)
	}
	return v, nil
}

const (
	// <term>     -> \x00\x01
	// \x00       -> \x00\xff

	// escape needs to be the smallest byte value so that
	// it doesn't change the sort order of strings of
	// different lengths.
	escape      byte = 0x00
	escapedTerm byte = 0x01
	escaped00   byte = 0xff
	escapedFF   byte = 0x00
)

type escapes struct {
	escape      byte
	escapedTerm byte
	escaped00   byte
	escapedFF   byte
	marker      byte
}

var (
	ascendingEscapes  = escapes{escape, escapedTerm, escaped00, escapedFF, bytesMarker}
	descendingEscapes = escapes{^escape, ^escapedTerm, ^escaped00, ^escapedFF, bytesMarker}
)

// EncodeBytes encodes the []byte value using an escape-based
// encoding. The encoded value is terminated with the sequence
// "\x00\x01" which is guaranteed to not occur elsewhere in the
// encoded value. This terminal also needs to sort before any
// valid sequence, to preserve the natural input ordering.
// The encoded bytes are append to the supplied buffer
// and the resulting buffer is returned.
func EncodeBytes(b []byte, data []byte) []byte {
	b = append(b, bytesMarker)
	for {
		// IndexByte is implemented by the go runtime in assembly and is
		// much faster than looping over the bytes in the slice.
		i := bytes.IndexByte(data, escape)
		if i == -1 {
			break
		}
		b = append(b, data[:i]...)
		b = append(b, escape, escaped00)
		data = data[i+1:]
	}
	b = append(b, data...)
	return append(b, escape, escapedTerm)
}

// EncodeBytesDecreasing encodes the []byte value using an
// escape-based encoding and then inverts (ones complement) the result
// so that it sorts in reverse order, from larger to smaller
// lexicographically.
func EncodeBytesDecreasing(b []byte, data []byte) []byte {
	n := len(b)
	b = EncodeBytes(b, data)
	onesComplement(b[n:])
	return b
}

// !!! make IndexByte a method on the reader. Stop taking escapes in?
// !!! figure out if buf should be *[]byte. That's what it is in ReadInto.
func decodeBytes(r Reader, buf []byte, e escapes) ([]byte, error) {
	marker, err := r.PeekByte()
	if (err != nil) || (marker != e.marker) {
		return nil, util.Errorf("did not find marker")
	}
	r.ReadByte()

	for {
		i := bytes.IndexByte(r.RawBytesRemaining(), e.escape)
		if i == -1 {
			return nil, util.Errorf("did not find terminator")
		}
		if i+1 >= len(r.RawBytesRemaining()) {
			return nil, util.Errorf("malformed escape")
		}

		v := r.RawBytesRemaining()[i+1]
		if v == e.escapedTerm {
			if buf == nil {
				buf, err = r.Read(i)
			} else {
				buf, err = r.ReadInto(i, &buf)
			}
			if err != nil {
				panic(err)
			}
			// Discard the escaped terminal.
			r.Read(2) // !!! make this more efficient - add a skip method
			return buf, nil
		}

		if v != e.escaped00 {
			return nil, util.Errorf("unknown escape")
		}
		if buf == nil {
			buf, err = r.Read(i)
		} else {
			buf, err = r.ReadInto(i, &buf)
		}
		if err != nil {
			panic(err)
		}
		buf = append(buf, 0x00)
		// Discard the escaped char.
		r.Read(2) // !!! make this more efficient - add a skip method
	}
}

// DecodeBytes decodes a []byte value from the input buffer which was
// encoded using EncodeBytes. The decoded bytes are appended to `buf`.
// `buf` can be nil, in which case a new buffer is allocated.
// The buffer to which the bytes have been appended is returned.
func DecodeBytes(r Reader, buf []byte) ([]byte, error) {
	return decodeBytes(r, buf, ascendingEscapes)
}

// DecodeBytesDecreasing decodes a []byte value from the input buffer
// which was encoded using EncodeBytesDecreasing. The decoded bytes
// are appended to r. The remainder of the input buffer and the
// decoded []byte are returned.
// !!! delete this function once we no longer use separate escapes for asc and desc
func DecodeBytesDecreasing(r Reader, buf []byte) ([]byte, error) {
	return decodeBytes(r, buf, descendingEscapes)
	// !!!
	// onesComplement(r)
	//return b, r, err
}

// EncodeString encodes the string value using an escape-based encoding. See
// EncodeBytes for details. The encoded bytes are append to the supplied buffer
// and the resulting buffer is returned.
func EncodeString(b []byte, s string) []byte {
	if len(s) == 0 {
		return EncodeBytes(b, nil)
	}
	// We unsafely convert the string to a []byte to avoid the
	// usual allocation when converting to a []byte. This is
	// kosher because we know that EncodeBytes{,Decreasing} does
	// not keep a reference to the value it encodes. The first
	// step is getting access to the string internals.
	hdr := (*reflect.StringHeader)(unsafe.Pointer(&s))
	// Next we treat the string data as a maximally sized array which we
	// slice. This usage is safe because the pointer value remains in the string.
	arg := (*[0x7fffffff]byte)(unsafe.Pointer(hdr.Data))[:len(s):len(s)]
	return EncodeBytes(b, arg)
}

// EncodeStringDecreasing encodes the string value using an escape-based
// encoding. See EncodeBytesDecreasing for details. The encoded bytes are
// append to the supplied buffer and the resulting buffer is returned.
func EncodeStringDecreasing(b []byte, s string) []byte {
	l := len(b)
	b = EncodeString(b, s)
	onesComplement(b[l:])
	return b
}

// DecodeString decodes a string value from the input buffer which was encoded
// using EncodeString or EncodeBytes. The r []byte is used as a temporary
// buffer in order to avoid memory allocations. The remainder of the input
// buffer and the decoded string are returned.
// !!! comment
func DecodeString(r Reader, buf []byte) (string, error) {
	buf = buf[:0] // Clear buf but keep the capacity.
	buf, err := decodeBytes(r, buf, ascendingEscapes)
	return string(buf), err
}

// DecodeStringDecreasing decodes a string value from the input buffer which
// was encoded using EncodeStringDecreasing or EncodeBytesDecreasing. The r
// []byte is used as a temporary buffer in order to avoid memory
// allocations. The remainder of the input buffer and the decoded string are
// returned.
// !!! comment
func DecodeStringDecreasing(r Reader, buf []byte) (string, error) {
	buf = buf[:0] // Clear buf but keep the capacity.
	// !!! get rid of escapes
	buf, err := decodeBytes(r, buf, descendingEscapes)
	return string(buf), err
	// !!! onesComplement(r)
	// return b, string(r), err
}

// EncodeNull encodes a NULL value. The encodes bytes are appended to the
// supplied buffer and the final buffer is returned. The encoded value for a
// NULL is guaranteed to not be a prefix for the EncodeVarint, EncodeFloat,
// EncodeBytes and EncodeString encodings.
func EncodeNull(b []byte) []byte {
	return append(b, encodedNull)
}

// EncodeNotNull encodes a value that is larger than the NULL marker encoded by
// EncodeNull but less than any encoded value returned by EncodeVarint,
// EncodeFloat, EncodeBytes or EncodeString.
func EncodeNotNull(b []byte) []byte {
	return append(b, encodedNotNull)
}

// !!! Add Encode/Decode{Not}NullDecreasing.

// DecodeIfNull decodes a NULL value from the input key. If the key contains a
// null at the start of the buffer then it is removed from the buffer and true
// is returned. Otherwise, the key is left unchanged and false is returned.
// Since the NULL value encoding is guaranteed to never occur as the prefix for
// the EncodeVarint, EncodeFloat, EncodeBytes and EncodeString encodings, it is
// safe to call DecodeIfNull on their encoded values.
func DecodeIfNull(r Reader) (bool, error) {
	var b byte
	var err error
	if b, err = r.PeekByte(); err != nil {
		return false, err
	}
	if b == encodedNull {
		r.ReadByte()
		return true, nil
	}
	return false, nil
}

// DecodeIfNotNull decodes a not-NULL value from the input key. If the key
// contains a not-NULL marker at the start of the buffer then it is removed
// from the buffer and true is returned. Otherwise, the key is left unchanged
// and false is returned.
// Note that the not-NULL marker is identical to the empty string encoding, so
// do not use this routine where it is necessary to distinguish not-NULL from
// the empty string.
func DecodeIfNotNull(r Reader) (bool, error) {
	var b byte
	var err error
	if b, err = r.PeekByte(); err != nil {
		return false, err
	}
	if b == encodedNotNull {
		r.ReadByte()
		return true, nil
	}
	return false, nil
}

// EncodeTime encodes a time value, appends it to the supplied buffer,
// and returns the final buffer. The encoding is guaranteed to be ordered
// Such that if t1.Before(t2) then after EncodeTime(b1, t1), and
// EncodeTime(b2, t1), Compare(b1, b2) < 0. The time zone offset not
// included in the encoding.
func EncodeTime(b []byte, t time.Time) []byte {
	// Read the unix absolute time. This is the absolute time and is
	// not time zone offset dependent.
	b = append(b, timeMarker)
	b = EncodeVarint(b, t.Unix())
	b = EncodeVarint(b, int64(t.Nanosecond()))
	return b
}

// DecodeTime decodes a time.Time value which was encoded using
// EncodeTime. The remainder of the input buffer and the decoded
// time.Time are returned.
func DecodeTime(r Reader) (time.Time, error) {
	if PeekType(r) != Time {
		return time.Time{}, util.Errorf("did not find marker")
	}
	r.ReadByte()
	sec, err := DecodeVarint(r)
	if err != nil {
		return time.Time{}, err
	}
	nsec, err := DecodeVarint(r)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(sec, nsec), nil
}

// Type represents the type of a value encoded by
// Encode{Null,NotNull,Varint,Uvarint,Float,Bytes}.
type Type int

// Type values.
const (
	Unknown Type = iota
	Null
	NotNull
	Int
	Float
	Bytes
	Time
)

// PeekType peeks at the type of the value encoded at the start of the input key.
func PeekType(r Reader) Type {
	var marker byte
	var err error
	if marker, err = r.PeekByte(); err != nil {
		return Unknown
	}
	switch {
	case marker == encodedNull:
		return Null
	case marker == encodedNotNull:
		return NotNull
	case marker == bytesMarker:
		return Bytes
	case marker == timeMarker:
		return Time
	case marker >= IntMin && marker <= IntMax:
		return Int
	case marker >= floatNaN && marker <= floatInfinity:
		return Float
	}
	return Unknown
}
