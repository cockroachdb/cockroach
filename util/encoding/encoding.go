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
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"math"
	"reflect"
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/util"
)

var crc32Pool = sync.Pool{
	New: func() interface{} {
		return crc32.NewIEEE()
	},
}

// NewCRC32Checksum returns a CRC32 checksum computed from the input byte slice.
func NewCRC32Checksum(b []byte) hash.Hash32 {
	crc := crc32Pool.Get().(hash.Hash32)
	crc.Write(b)
	return crc
}

// ReleaseCRC32Checksum releases a CRC32 back to the allocation pool.
func ReleaseCRC32Checksum(crc hash.Hash32) {
	crc.Reset()
	crc32Pool.Put(crc)
}

// unwrapChecksum assumes that the input byte slice b ends with a checksum, splits
// the slice accordingly and checks the embedded checksum which should match that
// of k prepended to b.
// Returns the slice with the checksum removed in case of success and an error
// otherwise.
func unwrapChecksum(k []byte, b []byte) ([]byte, error) {
	// Compute the first part of the expected checksum.
	c := NewCRC32Checksum(k)
	size := c.Size()
	if size > len(b) {
		return nil, util.Errorf("not enough bytes for %d character checksum", size)
	}
	// Add the second part.
	c.Write(b[:len(b)-size])
	// Get the reference checksum.
	bWanted := c.Sum(nil)
	// Grab the actual checksum from the end of b.
	bActual := b[len(b)-size:]

	if !bytes.Equal(bWanted, bActual) {
		return nil, util.Errorf("CRC integrity error: %v != %v", bActual, bWanted)
	}
	return b[:len(b)-size], nil
}

// wrapChecksum computes the checksum of the byte slice b appended to k.
// The output is b with the checksum appended.
func wrapChecksum(k []byte, b []byte) []byte {
	chk := NewCRC32Checksum(k)
	chk.Write(b)
	return chk.Sum(b)
}

// Encode translates the given value into a byte representation used to store
// it in the underlying key-value store. It typically applies to user-level
// keys, but not to keys operated on internally, such as accounting keys.
// It returns a byte slice containing, in order, the internal representation
// of v and a checksum of (k+v).
func Encode(k []byte, v interface{}) ([]byte, error) {
	result := []byte(nil)
	switch value := v.(type) {
	case int64:
		// int64 are encoded as varint.
		encoded := make([]byte, binary.MaxVarintLen64)
		numBytes := binary.PutVarint(encoded, value)
		result = encoded[:numBytes]
	case []byte:
		result = value
	default:
		panic(fmt.Sprintf("unable to encode type '%T' of value '%s'", v, v))
	}
	return wrapChecksum(k, result), nil
}

// Decode decodes a Go datatype from a value stored in the key-value store. It returns
// either an error or a variable of the decoded value.
func Decode(k []byte, wrappedValue []byte) (interface{}, error) {
	v, err := unwrapChecksum(k, wrappedValue)
	if err != nil {
		return nil, util.Errorf("integrity error: %v", err)
	}

	// TODO(Tobias): This is highly provisional, interpreting everything as a
	// varint until we have decided upon and implemented the actual encoding.
	// If the value exists, attempt to decode it as a varint.
	var numBytes int
	int64Val, numBytes := binary.Varint(v)
	if numBytes == 0 {
		return nil, util.Errorf("%v cannot be decoded; not varint-encoded", v)
	} else if numBytes < 0 {
		return nil, util.Errorf("%v cannot be decoded; integer overflow", v)
	}
	return int64Val, nil
}

// WillOverflow returns true if and only if adding both inputs
// would under- or overflow the 64 bit integer range.
func WillOverflow(a, b int64) bool {
	// Morally MinInt64 < a+b < MaxInt64, but without overflows.
	// First make sure that a <= b. If not, swap them.
	if a > b {
		a, b = b, a
	}
	// Now b is the larger of the numbers, and we compare sizes
	// in a way that can never over- or underflow.
	if b > 0 {
		return a > math.MaxInt64-b
	}
	return math.MinInt64-b > a
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
			return append(b, 7, byte(v))
		case v >= -0xffff:
			return append(b, 6, byte(v>>8), byte(v))
		case v >= -0xffffff:
			return append(b, 5, byte(v>>16), byte(v>>8), byte(v))
		case v >= -0xffffffff:
			return append(b, 4, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		case v >= -0xffffffffff:
			return append(b, 3, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8),
				byte(v))
		case v >= -0xffffffffffff:
			return append(b, 2, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),
				byte(v>>8), byte(v))
		case v >= -0xffffffffffffff:
			return append(b, 1, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24),
				byte(v>>16), byte(v>>8), byte(v))
		default:
			return append(b, 0, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
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
	length := int(b[0]) - 8
	if length < 0 {
		length = -length
		b = b[1:]
		if len(b) < length {
			panic(fmt.Sprintf("insufficient bytes to decode var uint64 int value: %s", b))
		}
		var v int64
		// Use the ones-complement of each encoded byte in order to build
		// up a positive number, then take the ones-complement again to
		// arrive at our negative value.
		for _, t := range b[:length] {
			v = (v << 8) | int64(^t)
		}
		return b[length:], ^v
	}

	b, v := DecodeUvarint(b)
	return b, int64(v)
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
		return append(b, 8)
	case v <= 0xff:
		return append(b, 9, byte(v))
	case v <= 0xffff:
		return append(b, 10, byte(v>>8), byte(v))
	case v <= 0xffffff:
		return append(b, 11, byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffff:
		return append(b, 12, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffffff:
		return append(b, 13, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8),
			byte(v))
	case v <= 0xffffffffffff:
		return append(b, 14, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),
			byte(v>>8), byte(v))
	case v <= 0xffffffffffffff:
		return append(b, 15, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24),
			byte(v>>16), byte(v>>8), byte(v))
	default:
		return append(b, 16, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
}

// EncodeUvarintDecreasing encodes the uint64 value so that it sorts in
// reverse order, from largest to smallest.
func EncodeUvarintDecreasing(b []byte, v uint64) []byte {
	return EncodeUvarint(b, ^v)
}

// DecodeUvarint decodes a varint encoded uint64 from the input
// buffer. The remainder of the input buffer and the decoded uint64
// are returned.
func DecodeUvarint(b []byte) ([]byte, uint64) {
	if len(b) == 0 {
		panic("insufficient bytes to decode var uint64 int value")
	}
	length := int(b[0]) - 8
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
	leftover, v := DecodeUvarint(b)
	return leftover, ^v
}

const (
	// <term>     -> \x00\x01
	// \x00       -> \x00\xff
	// \xff       -> \xff\x00
	// <infinity> -> \xff\xff
	escape1     byte = 0x00
	escape2     byte = 0xff
	escapedTerm byte = 0x01
	escapedNul  byte = 0xff
	escapedFF   byte = 0x00
)

type escapes struct {
	escape1     byte
	escape2     byte
	escapedTerm byte
	escapedNul  byte
	escapedFF   byte
}

var (
	ascendingEscapes  = escapes{escape1, escape2, escapedTerm, escapedNul, escapedFF}
	descendingEscapes = escapes{^escape1, ^escape2, ^escapedTerm, ^escapedNul, ^escapedFF}
)

var (
	// Infinity compares greater than every other encoded value.
	Infinity = []byte{0xff, 0xff}
)

// EncodeBytes encodes the []byte value using an escape-based
// encoding. The encoded value is terminated with the sequence
// "\x00\x01" which is guaranteed to not occur elsewhere in the
// encoded value. The encoded bytes are append to the supplied buffer
// and the resulting buffer is returned.
//
// The encoded data is guaranteed to compare less than the Infinity
// symbol \xff\xff. This is accomplished by transforming \xff to
// \xff\x00 when it occurs at the beginning of the bytes to encode.
func EncodeBytes(b []byte, data []byte) []byte {
	if len(data) > 0 && data[0] == escape2 {
		b = append(b, escape2, escapedFF)
		data = data[1:]
	}

	for {
		// IndexByte is implemented by the go runtime in assembly and is
		// much faster than looping over the bytes in the slice.
		i := bytes.IndexByte(data, escape1)
		if i == -1 {
			break
		}
		b = append(b, data[:i]...)
		b = append(b, escape1, escapedNul)
		data = data[i+1:]
	}
	b = append(b, data...)
	return append(b, escape1, escapedTerm)
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
	if len(b) > 0 && b[0] == e.escape2 {
		if len(b) == 1 {
			panic("malformed escape")
		}
		if b[1] != e.escapedFF {
			panic("unknown escape")
		}
		r = append(r, e.escapedNul)
		b = b[2:]
	}

	for {
		i := bytes.IndexByte(b, e.escape1)
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

		if v == e.escapedNul {
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

func parseVerb(format string, i int) (verb byte, ascending bool, width int, newI int) {
	if format[i] != '%' {
		panic("invalid format string: " + format)
	}
	i++

	// Process ascending flag (either "+" or "-").
	ascending = true
	if i < len(format) {
		if format[i] == '+' {
			i++
		} else if format[i] == '-' {
			ascending = false
			i++
		}
	}

	// Process width specifier (either blank, "32" or "64").
	width = 0
	if i+1 < len(format) {
		if format[i] == '3' && format[i+1] == '2' {
			width = 32
			i += 2
		} else if format[i] == '6' && format[i+1] == '4' {
			width = 64
			i += 2
		} else if format[i] >= '0' && format[i] <= '9' {
			panic("invalid width specifier; use 32 or 64")
		}
	}

	if i == len(format) {
		panic("no verb: " + format)
	}

	return format[i], ascending, width, i + 1
}

// EncodeKey encodes values to a byte slice according to a format
// string. Returns the byte slice containing the encoded values.
//
// The format string is printf-style with caveats: the primary being
// that the "fixed" portion of the format must occur as a prefix to
// the format. The verbs specify precisely what argument type is
// expected and no attempt is made to perform type conversion. For
// example, '%d' specifies an argument type of 'int64'. A panic will
// occur if any other type is encountered.
//
// The verbs:
//	%d	varint (int64) increasing
//	%-d	varint (int64) decreasing
//	%u	uvarint (uint64) increasing
//	%-u	uvarint (uint64) increasing
//	%32u	uint32 increasing
//	%-32u	uint32 increasing
//	%64u	uint64 increasing
//	%-64u	uint64 increasing
//	%s	bytes ([]byte,string) increasing
//	%-s	bytes ([]byte,string) decreasing
func EncodeKey(b []byte, format string, args ...interface{}) []byte {
	i, end := 0, len(format)
	for i < end && format[i] != '%' {
		i++
	}
	b = append(b, format[:i]...)

	for i < end {
		var verb byte
		var ascending bool
		var width int
		verb, ascending, width, i = parseVerb(format, i)

		switch verb {
		case 'd':
			if ascending {
				b = EncodeVarint(b, args[0].(int64))
			} else {
				b = EncodeVarintDecreasing(b, args[0].(int64))
			}
		case 'u':
			if ascending {
				if width == 0 {
					b = EncodeUvarint(b, args[0].(uint64))
				} else if width == 32 {
					b = EncodeUint32(b, args[0].(uint32))
				} else {
					b = EncodeUint64(b, args[0].(uint64))
				}
			} else {
				if width == 0 {
					b = EncodeUvarintDecreasing(b, args[0].(uint64))
				} else if width == 32 {
					b = EncodeUint32Decreasing(b, args[0].(uint32))
				} else {
					b = EncodeUint64Decreasing(b, args[0].(uint64))
				}
			}
		case 's':
			var arg []byte
			if s, ok := args[0].(string); ok {
				// We unsafely convert the string to a []byte to avoid the
				// usual allocation when converting to a []byte. This is
				// kosher because we know that EncodeBytes{,Decreasing} does
				// not keep a reference to the value it encodes. The first
				// step is getting access to the string internals.
				hdr := (*reflect.StringHeader)(unsafe.Pointer(&s))
				// Next we treat the string data as a maximally sized array
				// which we slice.
				//
				// Go vet complains about possible misuse of unsafe.Pointer
				// here (converting a uintptr to an unsafe.Pointer). This
				// usage is safe because the pointer value remains in the
				// string.
				arg = (*[0x7fffffff]byte)(unsafe.Pointer(hdr.Data))[:len(s):len(s)]
			} else {
				arg = args[0].([]byte)
			}
			if ascending {
				b = EncodeBytes(b, arg)
			} else {
				b = EncodeBytesDecreasing(b, arg)
			}
		default:
			panic("unknown format verb")
		}
		args = args[1:]
	}
	return b
}

// DecodeKey decodes values from a byte slice according to the format
// string. Returns the remainder of the byte slice if it was not
// completely consumed by the format string. See EncodeKey for details
// of the format specifiers.
//
// The variadic arguments must be pointers to the types specified by
// the format string. For example, '%d" requires a '*int64' argument.
func DecodeKey(b []byte, format string, args ...interface{}) []byte {
	i, end := 0, len(format)
	for i < end && format[i] != '%' {
		if len(b) == 0 || b[i] != format[i] {
			panic("format mismatch")
		}
		i++
	}
	if i > 0 {
		b = b[i:]
	}

	for i < end {
		var verb byte
		var ascending bool
		var width int
		verb, ascending, width, i = parseVerb(format, i)

		switch verb {
		case 'd':
			var r int64
			if ascending {
				b, r = DecodeVarint(b)
			} else {
				b, r = DecodeVarintDecreasing(b)
			}
			*(args[0].(*int64)) = r
		case 'u':
			if ascending {
				if width == 0 {
					b, *(args[0].(*uint64)) = DecodeUvarint(b)
				} else if width == 32 {
					b, *(args[0].(*uint32)) = DecodeUint32(b)
				} else {
					b, *(args[0].(*uint64)) = DecodeUint64(b)
				}
			} else {
				if width == 0 {
					b, *(args[0].(*uint64)) = DecodeUvarintDecreasing(b)
				} else if width == 32 {
					b, *(args[0].(*uint32)) = DecodeUint32Decreasing(b)
				} else {
					b, *(args[0].(*uint64)) = DecodeUint64Decreasing(b)
				}
			}
		case 's':
			var r []byte
			if ascending {
				b, r = DecodeBytes(b, nil)
			} else {
				b, r = DecodeBytesDecreasing(b, nil)
			}
			if s, ok := args[0].(*string); ok {
				*s = string(r)
			} else {
				*(args[0].(*[]byte)) = r
			}
		default:
			panic("unknown format verb")
		}
		args = args[1:]
	}
	return b
}
