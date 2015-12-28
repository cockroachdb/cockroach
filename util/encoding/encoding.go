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
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package encoding

import (
	"bytes"
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
	floatNaNDesc          = floatInfinity + 1 // NaN encoded descendingly
	floatTerminator       = 0x00

	bytesMarker     byte = floatNaNDesc + 1
	timeMarker      byte = bytesMarker + 1
	bytesDescMarker byte = timeMarker + 1
	timeDescMarker  byte = bytesDescMarker + 1

	// IntMin is chosen such that the range of int tags does not overlap the
	// ascii character set that is frequently used in testing.
	IntMin   = 0x80
	intZero  = IntMin + 8
	intSmall = IntMax - intZero - 8 // 109
	// IntMax is the maximum int tag value.
	IntMax = 0xfd

	// Nulls come last when encoded descendingly.
	encodedNotNullDesc = 0xfe
	encodedNullDesc    = 0xff
)

// Direction for ordering results.
type Direction int

// Direction values.
const (
	Ascending = iota
	Descending
)

// EncodeUint32 encodes the uint32 value using a big-endian 8 byte
// representation. The bytes are appended to the supplied buffer and
// the final buffer is returned.
func EncodeUint32(b []byte, v uint32, dir Direction) []byte {
	if dir == Ascending {
		return encodeUint32(b, v)
	}
	return encodeUint32Decreasing(b, v)
}

func encodeUint32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// EncodeUint32Decreasing encodes the uint32 value so that it sorts in
// reverse order, from largest to smallest.
func encodeUint32Decreasing(b []byte, v uint32) []byte {
	return encodeUint32(b, ^v)
}

// DecodeUint32 decodes a uint32 from the input buffer, treating
// the input as a big-endian 4 byte uint32 representation. The remainder
// of the input buffer and the decoded uint32 are returned.
func DecodeUint32(b []byte, dir Direction) ([]byte, uint32, error) {
	if dir == Ascending {
		return decodeUint32(b)
	}
	return decodeUint32Decreasing(b)
}

func decodeUint32(b []byte) ([]byte, uint32, error) {
	if len(b) < 4 {
		return nil, 0, util.Errorf("insufficient bytes to decode uint32 int value")
	}
	v := (uint32(b[0]) << 24) | (uint32(b[1]) << 16) |
		(uint32(b[2]) << 8) | uint32(b[3])
	return b[4:], v, nil
}

// DecodeUint32Decreasing decodes a uint32 value which was encoded
// using EncodeUint32Decreasing.
func decodeUint32Decreasing(b []byte) ([]byte, uint32, error) {
	leftover, v, err := decodeUint32(b)
	return leftover, ^v, err
}

// EncodeUint64 encodes the uint64 value using a big-endian 8 byte
// representation. The bytes are appended to the supplied buffer and
// the final buffer is returned.
func EncodeUint64(b []byte, v uint64, dir Direction) []byte {
	if dir == Ascending {
		return encodeUint64(b, v)
	}
	return encodeUint64Decreasing(b, v)
}

func encodeUint64(b []byte, v uint64) []byte {
	return append(b,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// EncodeUint64Decreasing encodes the uint64 value so that it sorts in
// reverse order, from largest to smallest.
func encodeUint64Decreasing(b []byte, v uint64) []byte {
	return encodeUint64(b, ^v)
}

// DecodeUint64 decodes a uint64 from the input buffer, treating
// the input as a big-endian 8 byte uint64 representation. The remainder
// of the input buffer and the decoded uint64 are returned.
func DecodeUint64(b []byte, dir Direction) ([]byte, uint64, error) {
	if dir == Ascending {
		return decodeUint64(b)
	}
	return decodeUint64Decreasing(b)
}

func decodeUint64(b []byte) ([]byte, uint64, error) {
	if len(b) < 8 {
		return nil, 0, util.Errorf("insufficient bytes to decode uint64 int value")
	}
	v := (uint64(b[0]) << 56) | (uint64(b[1]) << 48) |
		(uint64(b[2]) << 40) | (uint64(b[3]) << 32) |
		(uint64(b[4]) << 24) | (uint64(b[5]) << 16) |
		(uint64(b[6]) << 8) | uint64(b[7])
	return b[8:], v, nil
}

// DecodeUint64Decreasing decodes a uint64 value which was encoded
// using EncodeUint64Decreasing.
func decodeUint64Decreasing(b []byte) ([]byte, uint64, error) {
	leftover, v, err := decodeUint64(b)
	return leftover, ^v, err
}

// EncodeVarint encodes the int64 value using a variable length
// (length-prefixed) representation. The length is encoded as a single
// byte. If the value to be encoded is negative the length is encoded
// as 8-numBytes. If the value is positive it is encoded as
// 8+numBytes. The encoded bytes are appended to the supplied buffer
// and the final buffer is returned.
func EncodeVarint(b []byte, v int64, dir Direction) []byte {
	if dir == Ascending {
		return encodeVarint(b, v)
	}
	return encodeVarintDecreasing(b, v)
}

func encodeVarint(b []byte, v int64) []byte {
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

	return encodeUvarint(b, uint64(v))
}

// EncodeVarintDecreasing encodes the int64 value so that it sorts in reverse
// order, from largest to smallest.
func encodeVarintDecreasing(b []byte, v int64) []byte {
	return encodeVarint(b, ^v)
}

// DecodeVarint decodes a varint encoded int64 from the input
// buffer. The remainder of the input buffer and the decoded int64
// are returned.
func DecodeVarint(b []byte, dir Direction) ([]byte, int64, error) {
	if dir == Ascending {
		return decodeVarint(b)
	}
	return decodeVarintDecreasing(b)
}

func decodeVarint(b []byte) ([]byte, int64, error) {
	if len(b) == 0 {
		return nil, 0, util.Errorf("insufficient bytes to decode var uint64 int value")
	}
	length := int(b[0]) - intZero
	if length < 0 {
		length = -length
		remB := b[1:]
		if len(remB) < length {
			return nil, 0, util.Errorf("insufficient bytes to decode var uint64 int value: %s", remB)
		}
		var v int64
		// Use the ones-complement of each encoded byte in order to build
		// up a positive number, then take the ones-complement again to
		// arrive at our negative value.
		for _, t := range remB[:length] {
			v = (v << 8) | int64(^t)
		}
		return remB[length:], ^v, nil
	}

	remB, v, err := decodeUvarint(b)
	if err != nil {
		return remB, 0, err
	}
	if v > math.MaxInt64 {
		return nil, 0, util.Errorf("varint %d overflows int64", v)
	}
	return remB, int64(v), nil
}

// DecodeVarintDecreasing decodes a uint64 value which was encoded
// using EncodeVarintDecreasing.
func decodeVarintDecreasing(b []byte) ([]byte, int64, error) {
	leftover, v, err := decodeVarint(b)
	return leftover, ^v, err
}

// EncodeUvarint encodes the uint64 value using a variable length
// (length-prefixed) representation. The length is encoded as a single
// byte indicating the number of encoded bytes (-8) to follow. See
// EncodeVarint for rationale. The encoded bytes are appended to the
// supplied buffer and the final buffer is returned.
func EncodeUvarint(b []byte, v uint64, dir Direction) []byte {
	if dir == Ascending {
		return encodeUvarint(b, v)
	}
	return encodeUvarintDecreasing(b, v)
}

func encodeUvarint(b []byte, v uint64) []byte {
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
func encodeUvarintDecreasing(b []byte, v uint64) []byte {
	switch {
	case v == 0:
		return append(b, IntMin+8)
	case v <= 0xff:
		v = ^v
		return append(b, IntMin+7, byte(v))
	case v <= 0xffff:
		v = ^v
		return append(b, IntMin+6, byte(v>>8), byte(v))
	case v <= 0xffffff:
		v = ^v
		return append(b, IntMin+5, byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffff:
		v = ^v
		return append(b, IntMin+4, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffffff:
		v = ^v
		return append(b, IntMin+3, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8),
			byte(v))
	case v <= 0xffffffffffff:
		v = ^v
		return append(b, IntMin+2, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),
			byte(v>>8), byte(v))
	case v <= 0xffffffffffffff:
		v = ^v
		return append(b, IntMin+1, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24),
			byte(v>>16), byte(v>>8), byte(v))
	default:
		v = ^v
		return append(b, IntMin, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
}

// DecodeUvarint decodes a varint encoded uint64 from the input
// buffer. The remainder of the input buffer and the decoded uint64
// are returned.
func DecodeUvarint(b []byte, dir Direction) ([]byte, uint64, error) {
	if dir == Ascending {
		return decodeUvarint(b)
	}
	return decodeUvarintDecreasing(b)
}

func decodeUvarint(b []byte) ([]byte, uint64, error) {
	if len(b) == 0 {
		return nil, 0, util.Errorf("insufficient bytes to decode var uint64 int value")
	}
	length := int(b[0]) - intZero
	b = b[1:] // skip length byte
	if length <= intSmall {
		return b, uint64(length), nil
	}
	length -= intSmall
	if length < 0 || length > 8 {
		return nil, 0, util.Errorf("invalid uvarint length of %d", length)
	} else if len(b) < length {
		return nil, 0, util.Errorf("insufficient bytes to decode var uint64 int value: %v", b)
	}
	var v uint64
	// It is faster to range over the elements in a slice than to index
	// into the slice on each loop iteration.
	for _, t := range b[:length] {
		v = (v << 8) | uint64(t)
	}
	return b[length:], v, nil
}

// decodeUvarintDecreasing decodes a uint64 value which was encoded
// using EncodeUvarintDecreasing.
func decodeUvarintDecreasing(b []byte) ([]byte, uint64, error) {
	if len(b) == 0 {
		return nil, 0, util.Errorf("insufficient bytes to decode var uint64 int value")
	}
	length := intZero - int(b[0])
	b = b[1:] // skip length byte
	if length < 0 || length > 8 {
		return nil, 0, util.Errorf("invalid uvarint length of %d", length)
	} else if len(b) < length {
		return nil, 0, util.Errorf("insufficient bytes to decode var uint64 int value: %v", b)
	}
	var x uint64
	for _, t := range b[:length] {
		x = (x << 8) | uint64(^t)
	}
	return b[length:], x, nil
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
	marker      byte
}

var (
	ascendingEscapes  = escapes{escape, escapedTerm, escaped00, escapedFF, bytesMarker}
	descendingEscapes = escapes{^escape, ^escapedTerm, ^escaped00, ^escapedFF, bytesMarker}
)

// EncodeBytes encodes the []byte value using an escape-based encoding with a
// terminal marker.
func EncodeBytes(b []byte, data []byte, dir Direction) []byte {
	if dir == Ascending {
		return encodeBytes(b, data)
	}
	return encodeBytesDecreasing(b, data)
}

// EncodeBytes encodes the []byte value using an escape-based
// encoding. The encoded value is terminated with the sequence
// "\x00\x01" which is guaranteed to not occur elsewhere in the
// encoded value. The encoded bytes are append to the supplied buffer
// and the resulting buffer is returned.
func encodeBytes(b []byte, data []byte) []byte {
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
func encodeBytesDecreasing(b []byte, data []byte) []byte {
	n := len(b)
	b = encodeBytes(b, data)
	onesComplement(b[n+1:])
	return b
}

// DecodeBytes decodes a []byte value from the input buffer which was
// encoded using EncodeBytes. The decoded bytes are appended to r. The
// remainder of the input buffer and the decoded []byte are returned.
func DecodeBytes(b []byte, r []byte, dir Direction) ([]byte, []byte, error) {
	if dir == Ascending {
		return decodeBytes(b, r)
	}
	return decodeBytesDecreasing(b, r)
}

func decodeBytes(b []byte, r []byte) ([]byte, []byte, error) {
	return decodeBytesInternal(b, r, ascendingEscapes)
}

// DecodeBytesDecreasing decodes a []byte value from the input buffer
// which was encoded using EncodeBytesDecreasing. The decoded bytes
// are appended to r. The remainder of the input buffer and the
// decoded []byte are returned.
func decodeBytesDecreasing(b []byte, r []byte) ([]byte, []byte, error) {
	if r == nil {
		r = []byte{}
	}
	b, r, err := decodeBytesInternal(b, r, descendingEscapes)
	onesComplement(r)
	return b, r, err
}

func decodeBytesInternal(b []byte, r []byte, e escapes) ([]byte, []byte, error) {
	if len(b) == 0 || b[0] != e.marker {
		return nil, nil, util.Errorf("did not find marker")
	}
	b = b[1:]

	for {
		i := bytes.IndexByte(b, e.escape)
		if i == -1 {
			return nil, nil, util.Errorf("did not find terminator")
		}
		if i+1 >= len(b) {
			return nil, nil, util.Errorf("malformed escape")
		}

		v := b[i+1]
		if v == e.escapedTerm {
			if r == nil {
				r = b[:i]
			} else {
				r = append(r, b[:i]...)
			}
			return b[i+2:], r, nil
		}

		if v == e.escaped00 {
			r = append(r, b[:i]...)
			r = append(r, e.escapedFF)
		} else {
			return nil, nil, util.Errorf("unknown escape")
		}

		b = b[i+2:]
	}
}

// EncodeString encodes the string value using an escape-based encoding. See
// EncodeBytes for details. The encoded bytes are append to the supplied buffer
// and the resulting buffer is returned.
func EncodeString(b []byte, s string, dir Direction) []byte {
	if len(s) == 0 {
		return EncodeBytes(b, nil, dir)
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
	return EncodeBytes(b, arg, dir)
}

// DecodeString decodes a string value from the input buffer which was encoded
// using EncodeString or EncodeBytes. The r []byte is used as a temporary
// buffer in order to avoid memory allocations. The remainder of the input
// buffer and the decoded string are returned.
func DecodeString(b []byte, r []byte, dir Direction) ([]byte, string, error) {
	if dir == Ascending {
		return decodeString(b, r)
	}
	return decodeStringDecreasing(b, r)
}

func decodeString(b []byte, r []byte) ([]byte, string, error) {
	b, r, err := DecodeBytes(b, r, Ascending)
	return b, string(r), err
}

// DecodeStringDecreasing decodes a string value from the input buffer which
// was encoded using EncodeStringDecreasing or EncodeBytesDecreasing. The r
// []byte is used as a temporary buffer in order to avoid memory
// allocations. The remainder of the input buffer and the decoded string are
// returned.
func decodeStringDecreasing(b []byte, r []byte) ([]byte, string, error) {
	b, r, err := DecodeBytes(b, r, Descending)
	return b, string(r), err
}

// EncodeNull encodes a NULL value. The encodes bytes are appended to the
// supplied buffer and the final buffer is returned. The encoded value for a
// NULL is guaranteed to not be a prefix for the EncodeVarint, EncodeFloat,
// EncodeBytes and EncodeString encodings.
func EncodeNull(b []byte, dir Direction) []byte {
	if dir == Ascending {
		return encodeNull(b)
	}
	return encodeNullDecreasing(b)
}

func encodeNull(b []byte) []byte {
	return append(b, encodedNull)
}

func encodeNullDecreasing(b []byte) []byte {
	return append(b, encodedNullDesc)
}

// EncodeNotNull encodes a value that is larger than the NULL marker encoded by
// EncodeNull but less than any encoded value returned by EncodeVarint,
// EncodeFloat, EncodeBytes or EncodeString.
func EncodeNotNull(b []byte, dir Direction) []byte {
	if dir == Ascending {
		return encodeNotNull(b)
	}
	return encodeNotNullDecreasing(b)
}

func encodeNotNull(b []byte) []byte {
	return append(b, encodedNotNull)
}

func encodeNotNullDecreasing(b []byte) []byte {
	return append(b, encodedNotNullDesc)
}

// DecodeIfNull decodes a NULL value from the input buffer. If the input buffer
// contains a null at the start of the buffer then it is removed from the
// buffer and true is returned for the second result. Otherwise, the buffer is
// returned unchanged and false is returned for the second result. Since the
// NULL value encoding is guaranteed to never occur as the prefix for the
// EncodeVarint, EncodeFloat, EncodeBytes and EncodeString encodings, it is
// safe to call DecodeIfNull on their encoded values.
// This function handles both ascendingly and descendingly encoded NULLs.
func DecodeIfNull(b []byte) ([]byte, bool) {
	if PeekType(b) == Null {
		return b[1:], true
	}
	return b, false
}

// DecodeIfNotNull decodes a not-NULL value from the input buffer. If the input
// buffer contains a not-NULL marker at the start of the buffer then it is
// removed from the buffer and true is returned for the second
// result. Otherwise, the buffer is returned unchanged and false is returned
// for the second result. Note that the not-NULL marker is identical to the
// empty string encoding, so do not use this routine where it is necessary to
// distinguish not-NULL from the empty string.
// This function handles both ascendingly and descendingly encoded NULLs.
func DecodeIfNotNull(b []byte) ([]byte, bool) {
	if PeekType(b) == NotNull {
		return b[1:], true
	}
	return b, false
}

// EncodeTime encodes a time value, appends it to the supplied buffer,
// and returns the final buffer. The encoding is guaranteed to be ordered
// Such that if t1.Before(t2) then after EncodeTime(b1, t1), and
// EncodeTime(b2, t1), Compare(b1, b2) < 0. The time zone offset not
// included in the encoding.
func EncodeTime(b []byte, t time.Time, dir Direction) []byte {
	if dir == Ascending {
		return encodeTime(b, t)
	}
	return encodeTimeDecreasing(b, t)
}

func encodeTime(b []byte, t time.Time) []byte {
	// Read the unix absolute time. This is the absolute time and is
	// not time zone offset dependent.
	b = append(b, timeMarker)
	b = EncodeVarint(b, t.Unix(), Ascending)
	b = EncodeVarint(b, int64(t.Nanosecond()), Ascending)
	return b
}

func encodeTimeDecreasing(b []byte, t time.Time) []byte {
	// Read the unix absolute time. This is the absolute time and is
	// not time zone offset dependent.
	b = append(b, timeDescMarker)
	b = EncodeVarint(b, t.Unix(), Descending)
	b = EncodeVarint(b, int64(t.Nanosecond()), Descending)
	return b
}

// DecodeTime decodes a time.Time value which was encoded using
// EncodeTime. The remainder of the input buffer and the decoded
// time.Time are returned.
func DecodeTime(b []byte, dir Direction) ([]byte, time.Time, error) {
	if dir == Ascending {
		return decodeTime(b)
	}
	return decodeTimeDecreasing(b)
}

func decodeTime(b []byte) ([]byte, time.Time, error) {
	if PeekType(b) != Time {
		return nil, time.Time{}, util.Errorf("did not find marker")
	}
	b = b[1:]
	b, sec, err := DecodeVarint(b, Ascending)
	if err != nil {
		return b, time.Time{}, err
	}
	b, nsec, err := DecodeVarint(b, Ascending)
	if err != nil {
		return b, time.Time{}, err
	}
	return b, time.Unix(sec, nsec), nil
}

func decodeTimeDecreasing(b []byte) ([]byte, time.Time, error) {
	if PeekType(b) != TimeDesc {
		return nil, time.Time{}, util.Errorf("did not find marker")
	}
	b = b[1:]
	b, sec, err := DecodeVarint(b, Descending)
	if err != nil {
		return b, time.Time{}, err
	}
	b, nsec, err := DecodeVarint(b, Descending)
	if err != nil {
		return b, time.Time{}, err
	}
	return b, time.Unix(sec, nsec), nil
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
	BytesDesc // Bytes encoded descendingly
	Time
	TimeDesc // Time encoded descendingly
)

// PeekType peeks at the type of the value encoded at the start of b.
func PeekType(b []byte) Type {
	if len(b) >= 1 {
		m := b[0]
		switch {
		case m == encodedNull || m == encodedNullDesc:
			return Null
		case m == encodedNotNull || m == encodedNotNullDesc:
			return NotNull
		case m == bytesMarker:
			return Bytes
		case m == bytesDescMarker:
			return BytesDesc
		case m == timeMarker:
			return Time
		case m == timeDescMarker:
			return TimeDesc
		case m >= IntMin && m <= IntMax:
			return Int
		case m >= floatNaN && m <= floatNaNDesc:
			return Float
		}
	}
	return Unknown
}
