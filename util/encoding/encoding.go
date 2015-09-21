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
	"fmt"
	"hash"
	"hash/crc32"
	"math"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

var crc32Pool = sync.Pool{
	New: func() interface{} {
		return crc32.NewIEEE()
	},
}

// NewCRC32Checksum returns a CRC32 checksum computed from the input byte slice.
func NewCRC32Checksum(b []byte) hash.Hash32 {
	crc := crc32Pool.Get().(hash.Hash32)
	if _, err := crc.Write(b); err != nil {
		panic(err)
	}
	return crc
}

// ReleaseCRC32Checksum releases a CRC32 back to the allocation pool.
func ReleaseCRC32Checksum(crc hash.Hash32) {
	crc.Reset()
	crc32Pool.Put(crc)
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
	return EncodeUint32(b, ^v)
}

// DecodeUint32 decodes a uint32 from the input buffer, treating
// the input as a big-endian 8 byte uint32 representation. The remainder
// of the input buffer and the decoded uint32 are returned.
func DecodeUint32(b []byte) ([]byte, uint32) {
	if len(b) < 4 {
		panic("insufficient bytes to decode uint32 int value")
	}
	v := (uint32(b[0]) << 24) | (uint32(b[1]) << 16) |
		(uint32(b[2]) << 8) | uint32(b[3])
	return b[4:], v
}

// DecodeUint32Decreasing decodes a uint32 value which was encoded
// using EncodeUint32Decreasing.
func DecodeUint32Decreasing(b []byte) ([]byte, uint32) {
	leftover, v := DecodeUint32(b)
	return leftover, ^v
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
	return EncodeUint64(b, ^v)
}

// DecodeUint64 decodes a uint64 from the input buffer, treating
// the input as a big-endian 8 byte uint64 representation. The remainder
// of the input buffer and the decoded uint64 are returned.
func DecodeUint64(b []byte) ([]byte, uint64) {
	if len(b) < 8 {
		panic("insufficient bytes to decode uint64 int value")
	}
	v := (uint64(b[0]) << 56) | (uint64(b[1]) << 48) |
		(uint64(b[2]) << 40) | (uint64(b[3]) << 32) |
		(uint64(b[4]) << 24) | (uint64(b[5]) << 16) |
		(uint64(b[6]) << 8) | uint64(b[7])
	return b[8:], v
}

// DecodeUint64Decreasing decodes a uint64 value which was encoded
// using EncodeUint64Decreasing.
func DecodeUint64Decreasing(b []byte) ([]byte, uint64) {
	leftover, v := DecodeUint64(b)
	return leftover, ^v
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
			return append(b, 8, byte(v))
		case v >= -0xffff:
			return append(b, 7, byte(v>>8), byte(v))
		case v >= -0xffffff:
			return append(b, 6, byte(v>>16), byte(v>>8), byte(v))
		case v >= -0xffffffff:
			return append(b, 5, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		case v >= -0xffffffffff:
			return append(b, 4, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8),
				byte(v))
		case v >= -0xffffffffffff:
			return append(b, 3, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),
				byte(v>>8), byte(v))
		case v >= -0xffffffffffffff:
			return append(b, 2, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24),
				byte(v>>16), byte(v>>8), byte(v))
		default:
			return append(b, 1, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
				byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		}
	}

	return EncodeUvarint(b, uint64(v))
}

// EncodeVarintDecreasing encodes the uint64 value so that it sorts in
// reverse order, from largest to smallest.
func EncodeVarintDecreasing(b []byte, v int64) []byte {
	return EncodeVarint(b, ^v)
}

// DecodeVarint decodes a varint encoded int64 from the input
// buffer. The remainder of the input buffer and the decoded int64
// are returned.
func DecodeVarint(b []byte) ([]byte, int64) {
	if len(b) == 0 {
		panic("insufficient bytes to decode var uint64 int value")
	}
	length := int(b[0]) - 9
	if length < 0 {
		length = -length
		remB := b[1:]
		if len(remB) < length {
			panic(fmt.Sprintf("insufficient bytes to decode var uint64 int value: %s", remB))
		}
		var v int64
		// Use the ones-complement of each encoded byte in order to build
		// up a positive number, then take the ones-complement again to
		// arrive at our negative value.
		for _, t := range remB[:length] {
			v = (v << 8) | int64(^t)
		}
		return remB[length:], ^v
	}

	remB, v := DecodeUvarint(b)
	if v > math.MaxInt64 {
		panic(fmt.Sprintf("varint %d overflows int64", v))
	}
	return remB, int64(v)
}

// DecodeVarintDecreasing decodes a uint64 value which was encoded
// using EncodeVarintDecreasing.
func DecodeVarintDecreasing(b []byte) ([]byte, int64) {
	leftover, v := DecodeVarint(b)
	return leftover, ^v
}

// EncodeUvarint encodes the uint64 value using a variable length
// (length-prefixed) representation. The length is encoded as a single
// byte indicating the number of encoded bytes (-8) to follow. See
// EncodeVarint for rationale. The encoded bytes are appended to the
// supplied buffer and the final buffer is returned.
func EncodeUvarint(b []byte, v uint64) []byte {
	switch {
	case v == 0:
		return append(b, 9)
	case v <= 0xff:
		return append(b, 10, byte(v))
	case v <= 0xffff:
		return append(b, 11, byte(v>>8), byte(v))
	case v <= 0xffffff:
		return append(b, 12, byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffff:
		return append(b, 13, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffffff:
		return append(b, 14, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8),
			byte(v))
	case v <= 0xffffffffffff:
		return append(b, 15, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),
			byte(v>>8), byte(v))
	case v <= 0xffffffffffffff:
		return append(b, 16, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24),
			byte(v>>16), byte(v>>8), byte(v))
	default:
		return append(b, 17, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
}

// EncodeUvarintDecreasing encodes the uint64 value so that it sorts in
// reverse order, from largest to smallest.
func EncodeUvarintDecreasing(b []byte, v uint64) []byte {
	switch {
	case v == 0:
		return append(b, 9)
	case v <= 0xff:
		v = ^v
		return append(b, 8, byte(v))
	case v <= 0xffff:
		v = ^v
		return append(b, 7, byte(v>>8), byte(v))
	case v <= 0xffffff:
		v = ^v
		return append(b, 6, byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffff:
		v = ^v
		return append(b, 5, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffffff:
		v = ^v
		return append(b, 4, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8),
			byte(v))
	case v <= 0xffffffffffff:
		v = ^v
		return append(b, 3, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),
			byte(v>>8), byte(v))
	case v <= 0xffffffffffffff:
		v = ^v
		return append(b, 2, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24),
			byte(v>>16), byte(v>>8), byte(v))
	default:
		v = ^v
		return append(b, 1, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
}

// DecodeUvarint decodes a varint encoded uint64 from the input
// buffer. The remainder of the input buffer and the decoded uint64
// are returned.
func DecodeUvarint(b []byte) ([]byte, uint64) {
	if len(b) == 0 {
		panic("insufficient bytes to decode var uint64 int value")
	}
	length := int(b[0]) - 9
	b = b[1:] // skip length byte
	if length < 0 || length > 8 {
		panic(fmt.Sprintf("invalid uvarint length of %d", length))
	} else if len(b) < length {
		panic(fmt.Sprintf("insufficient bytes to decode var uint64 int value: %v", b))
	}
	var v uint64
	// It is faster to range over the elements in a slice than to index
	// into the slice on each loop iteration.
	for _, t := range b[:length] {
		v = (v << 8) | uint64(t)
	}
	return b[length:], v
}

// DecodeUvarintDecreasing decodes a uint64 value which was encoded
// using EncodeUvarintDecreasing.
func DecodeUvarintDecreasing(b []byte) ([]byte, uint64) {
	if len(b) == 0 {
		panic("insufficient bytes to decode var uint64 int value")
	}
	length := 9 - int(b[0])
	b = b[1:] // skip length byte
	if length < 0 || length > 8 {
		panic(fmt.Sprintf("invalid uvarint length of %d", length))
	} else if len(b) < length {
		panic(fmt.Sprintf("insufficient bytes to decode var uint64 int value: %v", b))
	}
	var x uint64
	for _, t := range b[:length] {
		x = (x << 8) | uint64(^t)
	}
	return b[length:], x
}

const (
	// <term>     -> \x00\x01
	// \x00       -> \x00\xff
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
}

var (
	ascendingEscapes  = escapes{escape, escapedTerm, escaped00, escapedFF}
	descendingEscapes = escapes{^escape, ^escapedTerm, ^escaped00, ^escapedFF}
)

// EncodeBytes encodes the []byte value using an escape-based
// encoding. The encoded value is terminated with the sequence
// "\x00\x01" which is guaranteed to not occur elsewhere in the
// encoded value. The encoded bytes are append to the supplied buffer
// and the resulting buffer is returned.
func EncodeBytes(b []byte, data []byte) []byte {
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

func decodeBytes(b []byte, r []byte, e escapes) ([]byte, []byte) {
	for {
		i := bytes.IndexByte(b, e.escape)
		if i == -1 {
			panic("did not find terminator")
		}
		if i+1 >= len(b) {
			panic("malformed escape")
		}

		v := b[i+1]
		if v == e.escapedTerm {
			if r == nil {
				r = b[:i]
			} else {
				r = append(r, b[:i]...)
			}
			return b[i+2:], r
		}

		if v == e.escaped00 {
			r = append(r, b[:i]...)
			r = append(r, e.escapedFF)
		} else {
			panic("unknown escape")
		}

		b = b[i+2:]
	}
}

// DecodeBytes decodes a []byte value from the input buffer which was
// encoded using EncodeBytes. The decoded bytes are appended to r. The
// remainder of the input buffer and the decoded []byte are returned.
func DecodeBytes(b []byte, r []byte) ([]byte, []byte) {
	return decodeBytes(b, r, ascendingEscapes)
}

// DecodeBytesDecreasing decodes a []byte value from the input buffer
// which was encoded using EncodeBytesDecreasing. The decoded bytes
// are appended to r. The remainder of the input buffer and the
// decoded []byte are returned.
func DecodeBytesDecreasing(b []byte, r []byte) ([]byte, []byte) {
	if r == nil {
		r = []byte{}
	}
	b, r = decodeBytes(b, r, descendingEscapes)
	onesComplement(r)
	return b, r
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
	if len(s) == 0 {
		return EncodeBytesDecreasing(b, nil)
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
	return EncodeBytesDecreasing(b, arg)
}

// DecodeString decodes a string value from the input buffer which was encoded
// using EncodeString or EncodeBytes. The r []byte is used as a temporary
// buffer in order to avoid memory allocations. The remainder of the input
// buffer and the decoded string are returned.
func DecodeString(b []byte, r []byte) ([]byte, string) {
	b, r = decodeBytes(b, r, ascendingEscapes)
	return b, string(r)
}

// DecodeStringDecreasing decodes a string value from the input buffer which
// was encoded using EncodeStringDecreasing or EncodeBytesDecreasing. The r
// []byte is used as a temporary buffer in order to avoid memory
// allocations. The remainder of the input buffer and the decoded string are
// returned.
func DecodeStringDecreasing(b []byte, r []byte) ([]byte, string) {
	// We need to pass in a non-nil "r" parameter here so that the output is
	// always copied to a new string instead of just returning the input when
	// when there are no embedded escapes.
	if r == nil {
		r = []byte{}
	}
	b, r = decodeBytes(b, r, descendingEscapes)
	onesComplement(r)
	return b, string(r)
}

var (
	encodedNull = []byte{0x00, 0x00}
)

// EncodeNull encodes a NULL value. The encodes bytes are appended to the
// supplied buffer and the final buffer is returned. The encoded value for a
// NULL is guaranteed to not be a prefix for the EncodeVarint, EncodeFloat,
// EncodeBytes and EncodeString encodings.
func EncodeNull(b []byte) []byte {
	return append(b, encodedNull...)
}

// DecodeIfNull decodes a NULL value from the input buffer. If the input buffer
// contains a null at the start of the buffer then it is removed from the
// buffer and true is returned for the second result. Otherwise, the buffer is
// returned unchanged and false is returned for the second result. Since the
// NULL value encoding is guaranteed to never occur as the prefix for the
// EncodeVarint, EncodeFloat, EncodeBytes and EncodeString encodings, it is
// safe to call DecodeIfNull on their encoded values.
func DecodeIfNull(b []byte) ([]byte, bool) {
	if len(b) >= 2 && b[0] == 0x00 && b[1] == 0x00 {
		return b[2:], true
	}
	return b, false
}

// EncodeTime encodes a time value, appends it to the supplied buffer,
// and returns the final buffer. The encoding is guaranteed to be ordered
// Such that if t1.Before(t2) then after EncodeTime(b1, t1), and
// EncodeTime(b2, t1), Compare(b1, b2) < 0. The time zone offset not
// included in the encoding.
func EncodeTime(b []byte, t time.Time) []byte {
	// Read the unix absolute time. This is the absolute time and is
	// not time zone offset dependent.
	sec := t.Unix()
	nsec := t.Nanosecond()
	b = EncodeVarint(b, sec)
	b = EncodeVarint(b, int64(nsec))
	return b
}

// DecodeTime converts an encoded time into a UTC time.Time type and
// returns the rest of the buffer.
func DecodeTime(b []byte) ([]byte, time.Time) {
	b, sec := DecodeVarint(b)
	b, nsec := DecodeVarint(b)
	return b, time.Unix(sec, nsec).UTC()
}
