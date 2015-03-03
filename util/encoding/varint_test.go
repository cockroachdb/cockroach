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
// An ordered key encoding scheme based on sqlite4's key encoding:
// http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
//
// Author: Andrew Bonventre (andybons@gmail.com)

package encoding

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestVarint(t *testing.T) {
	testCases := []struct {
		val     uint64
		encoded []byte
	}{
		{240, []byte{0xf0}},
		{2287, []byte{0xf8, 0xff}},
		{67823, []byte{0xf9, 0xff, 0xff}},
		{16777215, []byte{0xfa, 0xff, 0xff, 0xff}},
		{4294967295, []byte{0xfb, 0xff, 0xff, 0xff, 0xff}},
		{1099511627775, []byte{0xfc, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{281474976710655, []byte{0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{72057594037927935, []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{9223372036854775807, []byte{0xff, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{9223372036854775808, []byte{0xff, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{18446744073709551615, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}
	for _, c := range testCases {
		buf := make([]byte, len(c.encoded))
		n := PutUvarint(buf, c.val)
		if n != len(c.encoded) {
			t.Errorf("short write: %d bytes written; %d expected", n, len(c.encoded))
		}
		if !bytes.Equal(buf, c.encoded) {
			t.Errorf("byte mismatch: expected %v, got %v", c.encoded, buf)
		}
		decoded, _ := GetUvarint(buf)
		if decoded != c.val {
			t.Errorf("decoded value mismatch: expected %v, got %v", c.val, decoded)
		}
	}
}

// uint64Slice attaches the methods of sort.Interface to []uint64,
// sorting in increasing order.
type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// byteSlice attaches the methods of sort.Interface to [][]byte,
// sorting in increasing lexicographical order.
type byteSlice [][]byte

func (p byteSlice) Len() int           { return len(p) }
func (p byteSlice) Less(i, j int) bool { return bytes.Compare(p[i], p[j]) < 0 }
func (p byteSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// TestVarintOrdering ensures that lexicographical and numeric ordering
// for varints are the same
func TestVarintOrdering(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(seed)
	ints := make(uint64Slice, 50)
	varints := make(byteSlice, len(ints))
	for i := range ints {
		// rand.Uint64() doesn't exist within the stdlib.
		// https://groups.google.com/forum/#!topic/golang-nuts/Kle874lT1Eo
		val := uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
		ints[i] = val
		varints[i] = make([]byte, maxVarintSize)
		PutUvarint(varints[i], val)
	}
	sort.Sort(ints)
	sort.Sort(varints)
	for i := range ints {
		decoded, _ := GetUvarint(varints[i])
		if decoded != ints[i] {
			t.Errorf("mismatched ordering at index %d: expected: %d, got %d [seed: %d]", i, ints[i], decoded, seed)
		}
	}
}
