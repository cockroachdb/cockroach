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
	"encoding/gob"
	"fmt"
	"hash"
	"hash/crc32"
	"math"

	"github.com/cockroachdb/cockroach/util"
)

// GobEncode is a convenience function to return the gob representation
// of the given value. If this value implements an interface, it needs
// to be registered before GobEncode can be used.
func GobEncode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(&v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GobDecode is a convenience function to return the unmarshaled value
// of the given byte slice. If the value implements an interface, it
// needs to be registered before GobEncode can be used.
func GobDecode(b []byte) (interface{}, error) {
	var result interface{}
	err := gob.NewDecoder(bytes.NewBuffer(b)).Decode(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// MustGobDecode calls GobDecode and panics in case of an error.
func MustGobDecode(b []byte) interface{} {
	bDecoded, err := GobDecode(b)
	if err != nil {
		panic(err)
	}
	return bDecoded
}

// MustGobEncode calls GobEncode and panics in case of an error.
func MustGobEncode(o interface{}) []byte {
	oEncoded, err := GobEncode(o)
	if err != nil {
		panic(err)
	}
	return oEncoded
}

// NewCRC32Checksum returns a CRC32 checksum computed from the input byte slice.
func NewCRC32Checksum(b []byte) hash.Hash32 {
	crc := crc32.NewIEEE()
	crc.Write(b)
	return crc
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
	var enc [4]byte
	for i := 3; i >= 0; i-- {
		enc[i] = byte(v & 0xff)
		v >>= 8
	}
	return append(b, enc[:]...)
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
	var v uint32
	for i := 0; i < 4; i++ {
		v = (v << 8) | uint32(b[i])
	}
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
	var enc [8]byte
	for i := 7; i >= 0; i-- {
		enc[i] = byte(v & 0xff)
		v >>= 8
	}
	return append(b, enc[:]...)
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
	var v uint64
	for i := 0; i < 8; i++ {
		v = (v << 8) | uint64(b[i])
	}
	return b[8:], v
}

// DecodeUint64Decreasing decodes a uint64 value which was encoded
// using EncodeUint64Decreasing.
func DecodeUint64Decreasing(b []byte) ([]byte, uint64) {
	leftover, v := DecodeUint64(b)
	return leftover, ^v
}

// EncodeVarUint64 encodes the uint64 value using a variable length
// (length-prefixed) big-endian 8 byte representation. The bytes are
// appended to the supplied buffer and the final buffer is returned.
func EncodeVarUint64(b []byte, v uint64) []byte {
	var enc [9]byte
	i := 8
	for v > 0 {
		enc[i] = byte(v & 0xff)
		i--
		v >>= 8
	}
	enc[i] = byte(8 - i)
	return append(b, enc[i:]...)
}

// EncodeVarUint64Decreasing encodes the uint64 value so that it sorts in
// reverse order, from largest to smallest.
func EncodeVarUint64Decreasing(b []byte, v uint64) []byte {
	return EncodeVarUint64(b, ^v)
}

// DecodeVarUint64 decodes a uint64 from the input buffer, treating
// the input as a big-endian 8 byte uint64 representation. The remainder
// of the input buffer and the decoded uint64 are returned.
func DecodeVarUint64(b []byte) ([]byte, uint64) {
	if len(b) == 0 {
		panic("insufficient bytes to decode var uint64 int value")
	}
	length := int(b[0])
	b = b[1:] // skip length byte
	if len(b) < length {
		panic(fmt.Sprintf("insufficient bytes to decode var uint64 int value: %s", b))
	}
	var v uint64
	for i := 0; i < length; i++ {
		v = (v << 8) | uint64(b[i])
	}
	return b[length:], v
}

// DecodeVarUint64Decreasing decodes a uint64 value which was encoded
// using EncodeVarUint64Decreasing.
func DecodeVarUint64Decreasing(b []byte) ([]byte, uint64) {
	leftover, v := DecodeVarUint64(b)
	return leftover, ^v
}

const (
	escape = 0x00
	term   = 0x01
	null   = 0xff
)

// EncodeBytes ...
func EncodeBytes(b []byte, data []byte) []byte {
	copyStart := 0
	for i, v := range data {
		if v == escape {
			b = append(b, data[copyStart:i]...)
			b = append(b, escape, null)
			copyStart = i + 1
		}
	}
	b = append(b, data[copyStart:]...)
	return append(b, escape, term)
}

// DecodeBytes ...
func DecodeBytes(b []byte) ([]byte, []byte) {
	var r []byte
	copyStart := 0
	for i := 0; i < len(b); i++ {
		v := b[i]
		if v == escape {
			v = b[i+1]
			if v == term {
				if r == nil {
					return b[i+2:], b[:i]
				}
				r = append(r, b[copyStart:i]...)
				return b[i+2:], r
			}
			r = append(r, b[copyStart:i]...)
			i++
			if v == null {
				r = append(r, 0)
			}
			copyStart = i + 1
		}
	}
	panic("did not find terminator")
}
