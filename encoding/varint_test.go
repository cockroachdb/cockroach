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
// An ordered key encoding scheme based on sqlite4's key encoding:
// http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
//
// Author: Andrew Bonventre (andybons@gmail.com)

package encoding

import (
	"bytes"
	"testing"
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
	}
}
