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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package util

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash"
	"hash/crc32"
	"math"
	"reflect"

	"github.com/golang/glog"
)

// HashNoCheck is a special value that can be passed as a checksum
// to indicate that CRC32 checking should not be performed.
var HashNoCheck = crc32.NewIEEE()

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
// of the given string. If the value implements an interface, it needs
// to be registered before GobEncode can be used.
func GobDecode(s []byte) (interface{}, error) {
	var result interface{}
	err := gob.NewDecoder(bytes.NewBuffer(s)).Decode(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GobDecodeOrDie calls GobDecode and panics in case of an error.
func GobDecodeOrDie(b []byte) interface{} {
	bDecoded, err := GobDecode(b)
	if err != nil {
		glog.Fatal(err)
	}
	return bDecoded
}

// GobEncodeOrDie calls GobEncode and panics in case of an error.
func GobEncodeOrDie(o interface{}) []byte {
	oEncoded, err := GobEncode(o)
	if err != nil {
		glog.Fatal(err)
	}
	return oEncoded
}

// NewChecksum returns a CRC32 checksum computed from the input byte slice.
func NewChecksum(b []byte) hash.Hash32 {
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
	c := NewChecksum(k)
	size := c.Size()
	if size > len(b) {
		return nil, Errorf("not enough bytes for %d character checksum", size)
	}
	// Add the second part.
	c.Write(b[:len(b)-size])
	// Get the reference checksum.
	bWanted := c.Sum(nil)
	// Grab the actual checksum from the end of b.
	bActual := b[len(b)-size:]

	if !bytes.Equal(bWanted, bActual) {
		return nil, Errorf("CRC integrity error: %v != %v", bActual, bWanted)
	}
	return b[:len(b)-size], nil
}

// wrapChecksum computes the checksum of the byte slice b appended to k.
// The output is b with the checksum appended.
func wrapChecksum(k []byte, b []byte) []byte {
	chk := NewChecksum(k)
	chk.Write(b)
	return chk.Sum(b)
}

// TODO(Tobias): implement SQLite4-encoding.

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
		panic(fmt.Sprintf("unable to encode type '%v' of value '%s'", reflect.TypeOf(v), v))
	}
	return wrapChecksum(k, result), nil
}

// Decode decodes a Go datatype from a value stored in the key-value store. It returns
// either an error or a variable of the decoded value.
func Decode(k []byte, wrappedValue []byte) (interface{}, error) {
	v, err := unwrapChecksum(k, wrappedValue)
	if err != nil {
		return nil, Errorf("integrity error: %v", err)
	}

	// TODO(Tobias): This is highly provisional, interpreting everything as a
	// varint until we have decided upon and implemented the actual encoding.
	// If the value exists, attempt to decode it as a varint.
	var numBytes int
	int64Val, numBytes := binary.Varint(v)
	if numBytes == 0 {
		return nil, Errorf("%v cannot be decoded; not varint-encoded", v)
	} else if numBytes < 0 {
		return nil, Errorf("%v cannot be decoded; integer overflow", v)
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
