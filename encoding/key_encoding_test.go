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
// Author: Andrew Bonventre (andybons@gmail.com)

package encoding

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
)

func TestEncodeDecodeString(t *testing.T) {
	testCases := []struct {
		text   string
		length int
	}{
		{"foo", 5},
		{"baaaar", 8},
		{"bazz", 6},
		{"Hello, 世界", 15},
		{"", 2},
		{"abcd", 6},
		{"☺☻☹", 11},
		{"日a本b語ç日ð本Ê語þ日¥本¼語i日©", 49},
		{"日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©", 143},
	}
	for _, c := range testCases {
		buf := EncodeString(nil, c.text)
		n := len(buf)
		if n != c.length {
			t.Errorf("short write for %q: %d bytes written; %d expected", c.text, n, c.length)
		}
		if buf[n-1] != orderedEncodingTerminator {
			t.Errorf("expected terminating byte (%#x), got %#x", orderedEncodingTerminator, buf[n-1])
		}
		_, s := DecodeString(buf)

		if s != c.text {
			t.Errorf("error decoding string: expected %q, got %q", c.text, s)
		}
	}
}

func TestStringOrdering(t *testing.T) {
	strs := []string{
		"foo",
		"baaaar",
		"bazz",
		"Hello, 世界",
		"",
		"abcd",
		"☺☻☹",
		"日a本b語ç日ð本Ê語þ日¥本¼語i日©",
		"日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©",
	}
	encodedStrs := make(byteSlice, len(strs))
	for i := range strs {
		encodedStrs[i] = EncodeString(nil, strs[i])
	}
	sort.Strings(strs)
	sort.Sort(encodedStrs)
	for i := range strs {
		_, decoded := DecodeString(encodedStrs[i])
		if decoded != strs[i] {
			t.Errorf("mismatched ordering at index %d: expected: %s, got %s", i, strs[i], decoded)
		}
	}
}

func TestInvalidUTF8String(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic due to invalid utf-8 string")
		}
	}()
	EncodeString(nil, "\x80\x80\x80\x80")
}

func TestStringNullBytePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic due to intervening 0x00 byte in string")
		}
	}()
	EncodeString(nil, string([]byte{0x00, 0x01, 0x02}))
}

func TestStringNoTerminatorPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic due to absence of terminator byte in encoded string")
		}
	}()
	DecodeString([]byte{orderedEncodingText, byte('a')})
}

func TestEncodeBinary(t *testing.T) {
	testCases := []struct{ blob, encoded []byte }{
		{[]byte{}, []byte{orderedEncodingBinary, orderedEncodingTerminator}},
		{[]byte{0xff}, []byte{orderedEncodingBinary, 0xff, 0x40}},
		{[]byte("1"), []byte{orderedEncodingBinary, 0x98, 0x40}},
		{[]byte("22"), []byte{orderedEncodingBinary, 0x99, 0x8c, 0x40}},
		{[]byte("333"), []byte{orderedEncodingBinary, 0x99, 0xcc, 0xe6, 0x30}},
		{[]byte("4444"), []byte{orderedEncodingBinary, 0x9a, 0x8d, 0x86, 0xc3, 0x20}},
		{[]byte("55555"), []byte{orderedEncodingBinary, 0x9a, 0xcd, 0xa6, 0xd3, 0xa9, 0x54}},
		{[]byte("666666"), []byte{orderedEncodingBinary, 0x9b, 0x8d, 0xc6, 0xe3, 0xb1, 0xd8, 0x6c}},
		{[]byte("7777777"), []byte{orderedEncodingBinary, 0x9b, 0xcd, 0xe6, 0xf3, 0xb9, 0xdc, 0xee, 0x37}},
		{[]byte("88888888"), []byte{orderedEncodingBinary, 0x9c, 0x8e, 0x87, 0x83, 0xc1, 0xe0, 0xf0, 0xb8, 0x9c, 0x0}},
		{[]byte("Carl"), []byte{orderedEncodingBinary, 0xa1, 0xd8, 0xae, 0xa6, 0x60}},
		{[]byte("Hello, 世界"), []byte{orderedEncodingBinary, 0xa4, 0x99, 0xad, 0xc6, 0xe3, 0xbc, 0xd8, 0xa0, 0xf2, 0xae, 0x92, 0xee, 0xbc, 0xd6, 0x18}},
	}
	for _, c := range testCases {
		b := EncodeBinary([]byte{}, c.blob)
		if !bytes.Equal(b, c.encoded) {
			t.Errorf("unexpected mismatch of encoded value: expected %s, got %s", prettyBytes(c.encoded), prettyBytes(b))
		}
	}
	blobs := make(byteSlice, len(testCases))
	encodedBlobs := make(byteSlice, len(testCases))
	for i, c := range testCases {
		blobs[i] = c.blob
		encodedBlobs[i] = c.encoded
	}
	sort.Sort(blobs)
	sort.Sort(encodedBlobs)
	for i := range encodedBlobs {
		// TODO(andybons): Use DecodeBinary once that's landed.
		var decoded []byte
		for _, c := range testCases {
			if bytes.Equal(encodedBlobs[i], c.encoded) {
				decoded = c.blob
			}
		}
		if !bytes.Equal(decoded, blobs[i]) {
			t.Errorf("mismatched ordering at index %d: expected: %s, got %s", i, prettyBytes(blobs[i]), prettyBytes(decoded))
		}
	}
}

func prettyBytes(b []byte) string {
	str := "["
	for i, v := range b {
		str += fmt.Sprintf("%#x", v)
		if i < len(b)-1 {
			str += ", "
		}
	}
	str += "]"
	return str
}

func TestIntMandE(t *testing.T) {
	testCases := []struct {
		Value int64
		E     int
		M     []byte
	}{
		{-9223372036854775808, 10, []byte{0x13, 0x2d, 0x43, 0x91, 0x07, 0x89, 0x6d, 0x9b, 0x75, 0x10}},
		{-9223372036854775807, 10, []byte{0x13, 0x2d, 0x43, 0x91, 0x07, 0x89, 0x6d, 0x9b, 0x75, 0x0e}},
		{-10000, 3, []byte{0x03, 0x01}},
		{-9999, 2, []byte{0xc7, 0xc6}},
		{-100, 2, []byte{0x03}},
		{-99, 1, []byte{0xc6}},
		{-1, 1, []byte{0x02}},
		{1, 1, []byte{0x02}},
		{10, 1, []byte{0x14}},
		{99, 1, []byte{0xc6}},
		{100, 2, []byte{0x03}},
		{110, 2, []byte{0x03, 0x14}},
		{999, 2, []byte{0x13, 0xc6}},
		{1234, 2, []byte{0x19, 0x44}},
		{9999, 2, []byte{0xc7, 0xc6}},
		{10000, 3, []byte{0x03, 0x01}},
		{10001, 3, []byte{0x03, 0x01, 0x02}},
		{12345, 3, []byte{0x03, 0x2f, 0x5a}},
		{123450, 3, []byte{0x19, 0x45, 0x64}},
		{9223372036854775807, 10, []byte{0x13, 0x2d, 0x43, 0x91, 0x07, 0x89, 0x6d, 0x9b, 0x75, 0x0e}},
	}
	for _, c := range testCases {
		if e, m := intMandE(c.Value); e != c.E || !bytes.Equal(m, c.M) {
			t.Errorf("unexpected mismatch in E/M for %v. expected E=%v | M=%+v, got E=%v | M=%+v", c.Value, c.E, c.M, e, m)
		}
		if v := makeIntFromMandE(c.Value < 0, c.E, c.M); v != c.Value {
			t.Errorf("unexpected mismatch in Value for E=%v and M=%+v. expected value=%v, got value=%v", c.E, c.M, c.Value, v)
		}
	}
}

func TestEncodeInt(t *testing.T) {
	testCases := []struct {
		Value    int64
		Encoding []byte
	}{
		{-9223372036854775808, []byte{0x09, 0xec, 0xd2, 0xbc, 0x6e, 0xf8, 0x76, 0x92, 0x64, 0x8a, 0xef, 0x00}},
		{-9223372036854775807, []byte{0x09, 0xec, 0xd2, 0xbc, 0x6e, 0xf8, 0x76, 0x92, 0x64, 0x8a, 0xf1, 0x00}},
		{-10000, []byte{0x10, 0xfc, 0xfe, 0x00}},
		{-9999, []byte{0x11, 0x38, 0x39, 0x00}},
		{-100, []byte{0x11, 0xfc, 0x00}},
		{-99, []byte{0x12, 0x39, 0x00}},
		{-1, []byte{0x12, 0xfd, 0x00}},
		{0, []byte{0x15}},
		{1, []byte{0x18, 0x02, 0x00}},
		{10, []byte{0x18, 0x14, 0x00}},
		{99, []byte{0x18, 0xc6, 0x00}},
		{100, []byte{0x19, 0x03, 0x00}},
		{110, []byte{0x19, 0x03, 0x14, 0x00}},
		{999, []byte{0x19, 0x13, 0xc6, 0x00}},
		{1234, []byte{0x19, 0x19, 0x44, 0x00}},
		{9999, []byte{0x19, 0xc7, 0xc6, 0x00}},
		{10000, []byte{0x1a, 0x03, 0x01, 0x00}},
		{10001, []byte{0x1a, 0x03, 0x01, 0x02, 0x00}},
		{12345, []byte{0x1a, 0x03, 0x2f, 0x5a, 0x00}},
		{123450, []byte{0x1a, 0x19, 0x45, 0x64, 0x00}},
		{9223372036854775807, []byte{0x21, 0x13, 0x2d, 0x43, 0x91, 0x07, 0x89, 0x6d, 0x9b, 0x75, 0x0e, 0x00}},
	}
	for i, c := range testCases {
		enc := EncodeInt([]byte{}, c.Value)
		if !bytes.Equal(enc, c.Encoding) {
			t.Errorf("unexpected mismatch for %v. expected %v, got %v", c.Value, c.Encoding, enc)
		}
		if i > 0 {
			if bytes.Compare(testCases[i-1].Encoding, enc) >= 0 {
				t.Errorf("expected %v to be less than %v", testCases[i-1].Encoding, enc)
			}
		}
		_, dec := DecodeInt(enc)
		if dec != c.Value {
			t.Errorf("unexpected mismatch for %v. got %v", c.Value, dec)
		}
	}
}

func disabledTestFloatMandE(t *testing.T) {
	testCases := []struct {
		Value float64
		E     int
		M     []byte
	}{
		{1.0, 1, []byte{0x02}},
		{10.0, 1, []byte{0x14}},
		{99.0, 1, []byte{0xc6}},
		{99.01, 1, []byte{0xc7, 0x02}},
		{99.0001, 1, []byte{0xc7, 0x01, 0x02}},
		{100.0, 2, []byte{0x02}},
		{100.01, 2, []byte{0x03, 0x01, 0x02}},
		{100.1, 2, []byte{0x03, 0x01, 0x14}},
		{1234, 2, []byte{0x19, 0x44}},
		{9999, 2, []byte{0xc7, 0xc6}},
		{9999.000001, 2, []byte{0xc7, 0xc7, 0x01, 0x01, 0x02}},
		{9999.000009, 2, []byte{0xc7, 0xc7, 0x01, 0x01, 0x12}},
		{9999.00001, 2, []byte{0xc7, 0xc7, 0x01, 0x01, 0x14}},
		{9999.00009, 2, []byte{0xc7, 0xc7, 0x01, 0x01, 0xb4}},
		{9999.000099, 2, []byte{0xc7, 0xc7, 0x01, 0x01, 0xc6}},
		{9999.0001, 2, []byte{0xc7, 0xc7, 0x01, 0x02}},
		{9999.001, 2, []byte{0xc7, 0xc7, 0x01, 0x14}},
		{9999.01, 2, []byte{0xc7, 0xc7, 0x02}},
		{9999.1, 2, []byte{0xc7, 0xc7, 0x14}},
		{10000, 3, []byte{0x02}},
		{10001, 3, []byte{0x03, 0x01, 0x02}},
		{12345, 3, []byte{0x03, 0x2f, 0x5a}},
		{123450, 3, []byte{0x19, 0x45, 0x64}},
		{1234.5, 2, []byte{0x19, 0x45, 0x64}},
		{12.345, 1, []byte{0x19, 0x45, 0x64}},
		{0.123, 0, []byte{0x19, 0x3c}},
		{0.0123, 0, []byte{0x03, 0x2e}},
		{0.00123, -1, []byte{0x19, 0x3c}},
		{9223372036854775807, 10, []byte{0x13, 0x2d, 0x43, 0x91, 0x07, 0x89, 0x6d, 0x9b, 0x75, 0x0e}},
	}
	for _, c := range testCases {
		if e, m := floatMandE(c.Value); e != c.E || !bytes.Equal(m, c.M) {
			t.Errorf("unexpected mismatch in E/M for %v. expected E=%v | M=%+v, got E=%v | M=%+v", c.Value, c.E, c.M, e, m)
		}
	}
}
