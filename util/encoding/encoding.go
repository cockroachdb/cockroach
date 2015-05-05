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
	"sync"

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
	if v > math.MaxInt64 {
		panic(fmt.Sprintf("varint %d overflows int64", v))
	}
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
	escape1     = 0x00
	escape2     = 0xff
	escapedTerm = 0x01
	escapedNul  = 0xff
	escapedFF   = 0x00
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

// DecodeBytes decodes a []byte value from the input buffer which was
// encoded using EncodeBytes. The remainder of the input buffer and
// the decoded []byte are returned.
func DecodeBytes(b []byte) ([]byte, []byte) {
	var r []byte

	if len(b) > 0 && b[0] == escape2 {
		if len(b) == 1 {
			panic("malformed escape")
		}
		if b[1] != escapedFF {
			panic("unknown escape")
		}
		r = append(r, 0xff)
		b = b[2:]
	}

	for {
		i := bytes.IndexByte(b, escape1)
		if i == -1 {
			panic("did not find terminator")
		}
		if i+1 >= len(b) {
			panic("malformed escape")
		}

		v := b[i+1]
		if v == escapedTerm {
			if r == nil {
				r = b[:i]
			} else {
				r = append(r, b[:i]...)
			}
			return b[i+2:], r
		}

		if v == escapedNul {
			r = append(r, b[:i]...)
			r = append(r, 0)
		} else {
			panic("unknown escape")
		}

		b = b[i+2:]
	}
}
