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
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/duration"
)

const (
	encodedNull = 0x00
	// A marker greater than NULL but lower than any other value.
	// This value is not actually ever present in a stored key, but
	// it's used in keys used as span boundaries for index scans.
	encodedNotNull = 0x01

	floatNaN     = encodedNotNull + 1
	floatNeg     = floatNaN + 1
	floatZero    = floatNeg + 1
	floatPos     = floatZero + 1
	floatNaNDesc = floatPos + 1 // NaN encoded descendingly

	// The gap between floatNaNDesc and bytesMarker was left for
	// compatibility reasons.
	bytesMarker          byte = 0x12
	bytesDescMarker      byte = bytesMarker + 1
	timeMarker           byte = bytesDescMarker + 1
	durationBigNegMarker byte = timeMarker + 1 // Only used for durations < MinInt64 nanos.
	durationMarker       byte = durationBigNegMarker + 1
	durationBigPosMarker byte = durationMarker + 1 // Only used for durations > MaxInt64 nanos.

	decimalNaN              = durationBigPosMarker + 1
	decimalNegativeInfinity = decimalNaN + 1
	decimalNegLarge         = decimalNegativeInfinity + 1
	decimalNegMedium        = decimalNegLarge + 11
	decimalNegSmall         = decimalNegMedium + 1
	decimalZero             = decimalNegSmall + 1
	decimalPosSmall         = decimalZero + 1
	decimalPosMedium        = decimalPosSmall + 1
	decimalPosLarge         = decimalPosMedium + 11
	decimalInfinity         = decimalPosLarge + 1
	decimalNaNDesc          = decimalInfinity + 1 // NaN encoded descendingly
	decimalTerminator       = 0x00

	// IntMin is chosen such that the range of int tags does not overlap the
	// ascii character set that is frequently used in testing.
	IntMin      = 0x80
	intMaxWidth = 8
	intZero     = IntMin + intMaxWidth
	intSmall    = IntMax - intZero - intMaxWidth // 109
	// IntMax is the maximum int tag value.
	IntMax = 0xfd

	// Nulls come last when encoded descendingly.
	encodedNotNullDesc = 0xfe
	encodedNullDesc    = 0xff
)

const (
	// EncodedDurationMaxLen is the largest number of bytes used when encoding a
	// Duration.
	EncodedDurationMaxLen = 1 + 3*binary.MaxVarintLen64 // 3 varints are encoded.
	// BytesDescMarker is exported for testing.
	BytesDescMarker = bytesDescMarker
)

// Direction for ordering results.
type Direction int

// Direction values.
const (
	_ Direction = iota
	Ascending
	Descending
)

// Reverse returns the opposite direction.
func (d Direction) Reverse() Direction {
	switch d {
	case Ascending:
		return Descending
	case Descending:
		return Ascending
	default:
		panic(fmt.Sprintf("Invalid direction %d", d))
	}
}

// EncodeUint32Ascending encodes the uint32 value using a big-endian 8 byte
// representation. The bytes are appended to the supplied buffer and
// the final buffer is returned.
func EncodeUint32Ascending(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// EncodeUint32Descending encodes the uint32 value so that it sorts in
// reverse order, from largest to smallest.
func EncodeUint32Descending(b []byte, v uint32) []byte {
	return EncodeUint32Ascending(b, ^v)
}

// DecodeUint32Ascending decodes a uint32 from the input buffer, treating
// the input as a big-endian 4 byte uint32 representation. The remainder
// of the input buffer and the decoded uint32 are returned.
func DecodeUint32Ascending(b []byte) ([]byte, uint32, error) {
	if len(b) < 4 {
		return nil, 0, util.Errorf("insufficient bytes to decode uint32 int value")
	}
	v := (uint32(b[0]) << 24) | (uint32(b[1]) << 16) |
		(uint32(b[2]) << 8) | uint32(b[3])
	return b[4:], v, nil
}

// DecodeUint32Descending decodes a uint32 value which was encoded
// using EncodeUint32Descending.
func DecodeUint32Descending(b []byte) ([]byte, uint32, error) {
	leftover, v, err := DecodeUint32Ascending(b)
	return leftover, ^v, err
}

// EncodeUint64Ascending encodes the uint64 value using a big-endian 8 byte
// representation. The bytes are appended to the supplied buffer and
// the final buffer is returned.
func EncodeUint64Ascending(b []byte, v uint64) []byte {
	return append(b,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// EncodeUint64Descending encodes the uint64 value so that it sorts in
// reverse order, from largest to smallest.
func EncodeUint64Descending(b []byte, v uint64) []byte {
	return EncodeUint64Ascending(b, ^v)
}

// DecodeUint64Ascending decodes a uint64 from the input buffer, treating
// the input as a big-endian 8 byte uint64 representation. The remainder
// of the input buffer and the decoded uint64 are returned.
func DecodeUint64Ascending(b []byte) ([]byte, uint64, error) {
	if len(b) < 8 {
		return nil, 0, util.Errorf("insufficient bytes to decode uint64 int value")
	}
	v := (uint64(b[0]) << 56) | (uint64(b[1]) << 48) |
		(uint64(b[2]) << 40) | (uint64(b[3]) << 32) |
		(uint64(b[4]) << 24) | (uint64(b[5]) << 16) |
		(uint64(b[6]) << 8) | uint64(b[7])
	return b[8:], v, nil
}

// DecodeUint64Descending decodes a uint64 value which was encoded
// using EncodeUint64Descending.
func DecodeUint64Descending(b []byte) ([]byte, uint64, error) {
	leftover, v, err := DecodeUint64Ascending(b)
	return leftover, ^v, err
}

const maxVarintSize = 9

// EncodeVarintAscending encodes the int64 value using a variable length
// (length-prefixed) representation. The length is encoded as a single
// byte. If the value to be encoded is negative the length is encoded
// as 8-numBytes. If the value is positive it is encoded as
// 8+numBytes. The encoded bytes are appended to the supplied buffer
// and the final buffer is returned.
func EncodeVarintAscending(b []byte, v int64) []byte {
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
	return EncodeUvarintAscending(b, uint64(v))
}

// EncodeVarintDescending encodes the int64 value so that it sorts in reverse
// order, from largest to smallest.
func EncodeVarintDescending(b []byte, v int64) []byte {
	return EncodeVarintAscending(b, ^v)
}

// DecodeVarintAscending decodes a value encode by EncodeVaringAscending.
func DecodeVarintAscending(b []byte) ([]byte, int64, error) {
	if len(b) == 0 {
		return nil, 0, util.Errorf("insufficient bytes to decode uvarint value")
	}
	length := int(b[0]) - intZero
	if length < 0 {
		length = -length
		remB := b[1:]
		if len(remB) < length {
			return nil, 0, util.Errorf("insufficient bytes to decode uvarint value: %s", remB)
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

	remB, v, err := DecodeUvarintAscending(b)
	if err != nil {
		return remB, 0, err
	}
	if v > math.MaxInt64 {
		return nil, 0, util.Errorf("varint %d overflows int64", v)
	}
	return remB, int64(v), nil
}

// DecodeVarintDescending decodes a uint64 value which was encoded
// using EncodeVarintDescending.
func DecodeVarintDescending(b []byte) ([]byte, int64, error) {
	leftover, v, err := DecodeVarintAscending(b)
	return leftover, ^v, err
}

// EncodeUvarintAscending encodes the uint64 value using a variable length
// (length-prefixed) representation. The length is encoded as a single
// byte indicating the number of encoded bytes (-8) to follow. See
// EncodeVarintAscending for rationale. The encoded bytes are appended to the
// supplied buffer and the final buffer is returned.
func EncodeUvarintAscending(b []byte, v uint64) []byte {
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

// EncodeUvarintDescending encodes the uint64 value so that it sorts in
// reverse order, from largest to smallest.
func EncodeUvarintDescending(b []byte, v uint64) []byte {
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

// DecodeUvarintAscending decodes a varint encoded uint64 from the input
// buffer. The remainder of the input buffer and the decoded uint64
// are returned.
func DecodeUvarintAscending(b []byte) ([]byte, uint64, error) {
	if len(b) == 0 {
		return nil, 0, util.Errorf("insufficient bytes to decode uvarint value")
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
		return nil, 0, util.Errorf("insufficient bytes to decode uvarint value: %v", b)
	}
	var v uint64
	// It is faster to range over the elements in a slice than to index
	// into the slice on each loop iteration.
	for _, t := range b[:length] {
		v = (v << 8) | uint64(t)
	}
	return b[length:], v, nil
}

// DecodeUvarintDescending decodes a uint64 value which was encoded
// using EncodeUvarintDescending.
func DecodeUvarintDescending(b []byte) ([]byte, uint64, error) {
	if len(b) == 0 {
		return nil, 0, util.Errorf("insufficient bytes to decode uvarint value")
	}
	length := intZero - int(b[0])
	b = b[1:] // skip length byte
	if length < 0 || length > 8 {
		return nil, 0, util.Errorf("invalid uvarint length of %d", length)
	} else if len(b) < length {
		return nil, 0, util.Errorf("insufficient bytes to decode uvarint value: %v", b)
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
	descendingEscapes = escapes{^escape, ^escapedTerm, ^escaped00, ^escapedFF, bytesDescMarker}
)

// EncodeBytesAscending encodes the []byte value using an escape-based
// encoding. The encoded value is terminated with the sequence
// "\x00\x01" which is guaranteed to not occur elsewhere in the
// encoded value. The encoded bytes are append to the supplied buffer
// and the resulting buffer is returned.
func EncodeBytesAscending(b []byte, data []byte) []byte {
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

// EncodeBytesDescending encodes the []byte value using an
// escape-based encoding and then inverts (ones complement) the result
// so that it sorts in reverse order, from larger to smaller
// lexicographically.
func EncodeBytesDescending(b []byte, data []byte) []byte {
	n := len(b)
	b = EncodeBytesAscending(b, data)
	b[n] = bytesDescMarker
	onesComplement(b[n+1:])
	return b
}

// DecodeBytesAscending decodes a []byte value from the input buffer
// which was encoded using EncodeBytesAscending. The decoded bytes
// are appended to r. The remainder of the input buffer and the
// decoded []byte are returned.
func DecodeBytesAscending(b []byte, r []byte) ([]byte, []byte, error) {
	return decodeBytesInternal(b, r, ascendingEscapes, true)
}

// DecodeBytesDescending decodes a []byte value from the input buffer
// which was encoded using EncodeBytesDescending. The decoded bytes
// are appended to r. The remainder of the input buffer and the
// decoded []byte are returned.
func DecodeBytesDescending(b []byte, r []byte) ([]byte, []byte, error) {
	// Always pass an `r` to make sure we never get back a sub-slice of `b`,
	// since we're going to modify the contents of the slice.
	if r == nil {
		r = []byte{}
	}
	b, r, err := decodeBytesInternal(b, r, descendingEscapes, true)
	onesComplement(r)
	return b, r, err
}

func decodeBytesInternal(b []byte, r []byte, e escapes, expectMarker bool) ([]byte, []byte, error) {
	if expectMarker {
		if len(b) == 0 || b[0] != e.marker {
			return nil, nil, util.Errorf("did not find marker %#x in buffer %#x", e.marker, b)
		}
		b = b[1:]
	}

	for {
		i := bytes.IndexByte(b, e.escape)
		if i == -1 {
			return nil, nil, util.Errorf("did not find terminator %#x in buffer %#x", e.escape, b)
		}
		if i+1 >= len(b) {
			return nil, nil, util.Errorf("malformed escape in buffer %#x", b)
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
			return nil, nil, util.Errorf("unknown escape sequence: %#x %#x", e.escape, v)
		}

		b = b[i+2:]
	}
}

// EncodeStringAscending encodes the string value using an escape-based encoding. See
// EncodeBytes for details. The encoded bytes are append to the supplied buffer
// and the resulting buffer is returned.
func EncodeStringAscending(b []byte, s string) []byte {
	if len(s) == 0 {
		return EncodeBytesAscending(b, nil)
	}
	// We unsafely convert the string to a []byte to avoid the
	// usual allocation when converting to a []byte. This is
	// kosher because we know that EncodeBytes{,Descending} does
	// not keep a reference to the value it encodes. The first
	// step is getting access to the string internals.
	hdr := (*reflect.StringHeader)(unsafe.Pointer(&s))
	// Next we treat the string data as a maximally sized array which we
	// slice. This usage is safe because the pointer value remains in the string.
	arg := (*[0x7fffffff]byte)(unsafe.Pointer(hdr.Data))[:len(s):len(s)]
	return EncodeBytesAscending(b, arg)
}

// EncodeStringDescending is the descending version of EncodeStringAscending.
func EncodeStringDescending(b []byte, s string) []byte {
	if len(s) == 0 {
		return EncodeBytesDescending(b, nil)
	}
	// We unsafely convert the string to a []byte to avoid the
	// usual allocation when converting to a []byte. This is
	// kosher because we know that EncodeBytes{,Descending} does
	// not keep a reference to the value it encodes. The first
	// step is getting access to the string internals.
	hdr := (*reflect.StringHeader)(unsafe.Pointer(&s))
	// Next we treat the string data as a maximally sized array which we
	// slice. This usage is safe because the pointer value remains in the string.
	arg := (*[0x7fffffff]byte)(unsafe.Pointer(hdr.Data))[:len(s):len(s)]
	return EncodeBytesDescending(b, arg)
}

// DecodeStringAscending decodes a string value from the input buffer which was encoded
// using EncodeString or EncodeBytes. The r []byte is used as a temporary
// buffer in order to avoid memory allocations. The remainder of the input
// buffer and the decoded string are returned.
func DecodeStringAscending(b []byte, r []byte) ([]byte, string, error) {
	b, r, err := DecodeBytesAscending(b, r)
	return b, string(r), err
}

// DecodeStringDescending decodes a string value from the input buffer which
// was encoded using EncodeStringDescending or EncodeBytesDescending. The r
// []byte is used as a temporary buffer in order to avoid memory
// allocations. The remainder of the input buffer and the decoded string are
// returned.
func DecodeStringDescending(b []byte, r []byte) ([]byte, string, error) {
	b, r, err := DecodeBytesDescending(b, r)
	return b, string(r), err
}

// EncodeNullAscending encodes a NULL value. The encodes bytes are appended to the
// supplied buffer and the final buffer is returned. The encoded value for a
// NULL is guaranteed to not be a prefix for the EncodeVarint, EncodeFloat,
// EncodeBytes and EncodeString encodings.
func EncodeNullAscending(b []byte) []byte {
	return append(b, encodedNull)
}

// EncodeNullDescending is the descending equivalent of EncodeNullAscending.
func EncodeNullDescending(b []byte) []byte {
	return append(b, encodedNullDesc)
}

// EncodeNotNullAscending encodes a value that is larger than the NULL marker encoded by
// EncodeNull but less than any encoded value returned by EncodeVarint,
// EncodeFloat, EncodeBytes or EncodeString.
func EncodeNotNullAscending(b []byte) []byte {
	return append(b, encodedNotNull)
}

// EncodeNotNullDescending is the descending equivalent of EncodeNotNullAscending.
func EncodeNotNullDescending(b []byte) []byte {
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

// EncodeTimeAscending encodes a time value, appends it to the supplied buffer,
// and returns the final buffer. The encoding is guaranteed to be ordered
// Such that if t1.Before(t2) then after EncodeTime(b1, t1), and
// EncodeTime(b2, t1), Compare(b1, b2) < 0. The time zone offset not
// included in the encoding.
func EncodeTimeAscending(b []byte, t time.Time) []byte {
	return encodeTime(b, t.Unix(), int64(t.Nanosecond()))
}

// EncodeTimeDescending is the descending version of EncodeTimeAscending.
func EncodeTimeDescending(b []byte, t time.Time) []byte {
	return encodeTime(b, ^t.Unix(), ^int64(t.Nanosecond()))
}

func encodeTime(b []byte, unix, nanos int64) []byte {
	// Read the unix absolute time. This is the absolute time and is
	// not time zone offset dependent.
	b = append(b, timeMarker)
	b = EncodeVarintAscending(b, unix)
	b = EncodeVarintAscending(b, nanos)
	return b
}

// DecodeTimeAscending decodes a time.Time value which was encoded using
// EncodeTime. The remainder of the input buffer and the decoded
// time.Time are returned.
func DecodeTimeAscending(b []byte) ([]byte, time.Time, error) {
	b, sec, nsec, err := decodeTime(b)
	if err != nil {
		return b, time.Time{}, err
	}
	return b, time.Unix(sec, nsec), nil
}

// DecodeTimeDescending is the descending version of DecodeTimeAscending.
func DecodeTimeDescending(b []byte) ([]byte, time.Time, error) {
	b, sec, nsec, err := decodeTime(b)
	if err != nil {
		return b, time.Time{}, err
	}
	return b, time.Unix(^sec, ^nsec), nil
}

func decodeTime(b []byte) (r []byte, sec int64, nsec int64, err error) {
	if PeekType(b) != Time {
		return nil, 0, 0, util.Errorf("did not find marker")
	}
	b = b[1:]
	b, sec, err = DecodeVarintAscending(b)
	if err != nil {
		return b, 0, 0, err
	}
	b, nsec, err = DecodeVarintAscending(b)
	if err != nil {
		return b, 0, 0, err
	}
	return b, sec, nsec, nil
}

// EncodeDurationAscending encodes a duration.Duration value, appends it to the
// supplied buffer, and returns the final buffer. The encoding is guaranteed to
// be ordered such that if t1.Compare(t2) < 0 (or = 0 or > 0) then bytes.Compare
// will order them the same way after encoding.
func EncodeDurationAscending(b []byte, d duration.Duration) ([]byte, error) {
	sortNanos, months, days, err := d.Encode()
	if err != nil {
		// TODO(dan): Handle this using d.EncodeBigInt() and the
		// durationBigNeg/durationBigPos markers.
		return b, err
	}
	b = append(b, durationMarker)
	b = EncodeVarintAscending(b, sortNanos)
	b = EncodeVarintAscending(b, months)
	b = EncodeVarintAscending(b, days)
	return b, nil
}

// EncodeDurationDescending is the descending version of EncodeDurationAscending.
func EncodeDurationDescending(b []byte, d duration.Duration) ([]byte, error) {
	sortNanos, months, days, err := d.Encode()
	if err != nil {
		// TODO(dan): Handle this using d.EncodeBigInt() and the
		// durationBigNeg/durationBigPos markers.
		return b, err
	}
	b = append(b, durationMarker)
	b = EncodeVarintDescending(b, sortNanos)
	b = EncodeVarintDescending(b, months)
	b = EncodeVarintDescending(b, days)
	return b, nil
}

// DecodeDurationAscending decodes a duration.Duration value which was encoded
// using EncodeDurationAscending. The remainder of the input buffer and the
// decoded duration.Duration are returned.
func DecodeDurationAscending(b []byte) ([]byte, duration.Duration, error) {
	if PeekType(b) != Duration {
		return nil, duration.Duration{}, util.Errorf("did not find marker %x", b)
	}
	b = b[1:]
	b, sortNanos, err := DecodeVarintAscending(b)
	if err != nil {
		return b, duration.Duration{}, err
	}
	b, months, err := DecodeVarintAscending(b)
	if err != nil {
		return b, duration.Duration{}, err
	}
	b, days, err := DecodeVarintAscending(b)
	if err != nil {
		return b, duration.Duration{}, err
	}
	d, err := duration.Decode(sortNanos, months, days)
	if err != nil {
		return b, duration.Duration{}, err
	}
	return b, d, nil
}

// DecodeDurationDescending is the descending version of DecodeDurationAscending.
func DecodeDurationDescending(b []byte) ([]byte, duration.Duration, error) {
	if PeekType(b) != Duration {
		return nil, duration.Duration{}, util.Errorf("did not find marker")
	}
	b = b[1:]
	b, sortNanos, err := DecodeVarintDescending(b)
	if err != nil {
		return b, duration.Duration{}, err
	}
	b, months, err := DecodeVarintDescending(b)
	if err != nil {
		return b, duration.Duration{}, err
	}
	b, days, err := DecodeVarintDescending(b)
	if err != nil {
		return b, duration.Duration{}, err
	}
	d, err := duration.Decode(sortNanos, months, days)
	if err != nil {
		return b, duration.Duration{}, err
	}
	return b, d, nil
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
	Decimal
	Bytes
	BytesDesc // Bytes encoded descendingly
	Time
	Duration
)

// PeekType peeks at the type of the value encoded at the start of b.
func PeekType(b []byte) Type {
	if len(b) >= 1 {
		m := b[0]
		switch {
		case m == encodedNull, m == encodedNullDesc:
			return Null
		case m == encodedNotNull, m == encodedNotNullDesc:
			return NotNull
		case m == bytesMarker:
			return Bytes
		case m == bytesDescMarker:
			return BytesDesc
		case m == timeMarker:
			return Time
		case m == durationBigNegMarker, m == durationMarker, m == durationBigPosMarker:
			return Duration
		case m >= IntMin && m <= IntMax:
			return Int
		case m >= floatNaN && m <= floatNaNDesc:
			return Float
		case m >= decimalNaN && m <= decimalNaNDesc:
			return Decimal
		}
	}
	return Unknown
}

// PrettyPrintValue returns the string representation of all contiguous decodable
// values in the provided byte slice, separated by a provided separator.
func PrettyPrintValue(b []byte, sep string) string {
	var buf bytes.Buffer
	for len(b) > 0 {
		bb, s, err := prettyPrintFirstValue(b)
		if err != nil {
			fmt.Fprintf(&buf, "%s<%v>", sep, err)
		} else {
			fmt.Fprintf(&buf, "%s%s", sep, s)
		}
		b = bb
	}
	return buf.String()
}

// prettyPrintFirstValue returns a string representation of the first decodable
// value in the provided byte slice, along with the remaining byte slice
// after decoding.
func prettyPrintFirstValue(b []byte) ([]byte, string, error) {
	var err error
	switch PeekType(b) {
	case Null:
		b, _ = DecodeIfNull(b)
		return b, "NULL", nil
	case NotNull:
		b, _ = DecodeIfNotNull(b)
		return b, "#", nil
	case Int:
		var i int64
		b, i, err = DecodeVarintAscending(b)
		if err != nil {
			return b, "", err
		}
		return b, strconv.FormatInt(i, 10), nil
	case Float:
		var f float64
		b, f, err = DecodeFloatAscending(b)
		if err != nil {
			return b, "", err
		}
		return b, strconv.FormatFloat(f, 'g', -1, 64), nil
	case Decimal:
		var d *inf.Dec
		b, d, err = DecodeDecimalAscending(b, nil)
		if err != nil {
			return b, "", err
		}
		return b, d.String(), nil
	case Bytes:
		var s string
		b, s, err = DecodeStringAscending(b, nil)
		if err != nil {
			return b, "", err
		}
		return b, strconv.Quote(s), nil
	case BytesDesc:
		var s string
		b, s, err = DecodeStringDescending(b, nil)
		if err != nil {
			return b, "", err
		}
		return b, strconv.Quote(s), nil
	case Time:
		var t time.Time
		b, t, err = DecodeTimeAscending(b)
		if err != nil {
			return b, "", err
		}
		return b, t.UTC().Format(time.RFC3339Nano), nil
	case Duration:
		var d duration.Duration
		b, d, err = DecodeDurationAscending(b)
		if err != nil {
			return b, "", err
		}
		return b, d.String(), nil
	default:
		// This shouldn't ever happen, but if it does, return an empty slice.
		return nil, strconv.Quote(string(b)), nil
	}
}
