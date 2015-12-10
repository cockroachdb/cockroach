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
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/randutil"
)

func testBasicEncodeDecode32(encFunc func([]byte, uint32) []byte,
	dec func([]byte) ([]byte, uint32, error), decreasing bool, t *testing.T) {
	testCases := []uint32{
		0, 1,
		1<<8 - 1, 1 << 8,
		1<<16 - 1, 1 << 16,
		1<<24 - 1, 1 << 24,
		math.MaxUint32 - 1, math.MaxUint32,
	}

	var lastEnc []byte
	for i, v := range testCases {
		enc := encFunc(nil, v)
		if i > 0 {
			if (decreasing && bytes.Compare(enc, lastEnc) >= 0) ||
				(!decreasing && bytes.Compare(enc, lastEnc) < 0) {
				t.Errorf("ordered constraint violated for %d: [% x] vs. [% x]", v, enc, lastEnc)
			}
		}
		b, decode, err := dec(enc)
		if err != nil {
			t.Error(err)
			continue
		}
		if len(b) != 0 {
			t.Errorf("leftover bytes: [% x]", b)
		}
		if decode != v {
			t.Errorf("decode yielded different value than input: %d vs. %d", decode, v)
		}
		lastEnc = enc
	}
}

type testCaseUint32 struct {
	value  uint32
	expEnc []byte
}

func testCustomEncodeUint32(testCases []testCaseUint32,
	encFunc func([]byte, uint32) []byte, t *testing.T) {
	for _, test := range testCases {
		enc := encFunc(nil, test.value)
		if bytes.Compare(enc, test.expEnc) != 0 {
			t.Errorf("expected [% x]; got [% x]", test.expEnc, enc)
		}
	}
}

func TestEncodeDecodeUint32(t *testing.T) {
	testBasicEncodeDecode32(EncodeUint32, DecodeUint32, false, t)
	testCases := []testCaseUint32{
		{0, []byte{0x00, 0x00, 0x00, 0x00}},
		{1, []byte{0x00, 0x00, 0x00, 0x01}},
		{1 << 8, []byte{0x00, 0x00, 0x01, 0x00}},
		{math.MaxUint32, []byte{0xff, 0xff, 0xff, 0xff}},
	}
	testCustomEncodeUint32(testCases, EncodeUint32, t)
}

func TestEncodeDecodeUint32Decreasing(t *testing.T) {
	testBasicEncodeDecode32(EncodeUint32Decreasing, DecodeUint32Decreasing, true, t)
	testCases := []testCaseUint32{
		{0, []byte{0xff, 0xff, 0xff, 0xff}},
		{1, []byte{0xff, 0xff, 0xff, 0xfe}},
		{1 << 8, []byte{0xff, 0xff, 0xfe, 0xff}},
		{math.MaxUint32, []byte{0x00, 0x00, 0x00, 0x00}},
	}
	testCustomEncodeUint32(testCases, EncodeUint32Decreasing, t)
}

func testBasicEncodeDecodeUint64(encFunc func([]byte, uint64) []byte,
	dec func([]byte) ([]byte, uint64, error), decreasing bool, t *testing.T) {
	testCases := []uint64{
		0, 1,
		1<<8 - 1, 1 << 8,
		1<<16 - 1, 1 << 16,
		1<<24 - 1, 1 << 24,
		1<<32 - 1, 1 << 32,
		1<<40 - 1, 1 << 40,
		1<<48 - 1, 1 << 48,
		1<<56 - 1, 1 << 56,
		math.MaxUint64 - 1, math.MaxUint64,
	}

	var lastEnc []byte
	for i, v := range testCases {
		enc := encFunc(nil, v)
		if i > 0 {
			if (decreasing && bytes.Compare(enc, lastEnc) >= 0) ||
				(!decreasing && bytes.Compare(enc, lastEnc) < 0) {
				t.Errorf("ordered constraint violated for %d: [% x] vs. [% x]", v, enc, lastEnc)
			}
		}
		b, decode, err := dec(enc)
		if err != nil {
			t.Error(err)
			continue
		}
		if len(b) != 0 {
			t.Errorf("leftover bytes: [% x]", b)
		}
		if decode != v {
			t.Errorf("decode yielded different value than input: %d vs. %d", decode, v)
		}
		lastEnc = enc
	}
}

func testBasicEncodeDecodeInt64(encFunc func([]byte, int64) []byte,
	dec func([]byte) ([]byte, int64, error), decreasing bool, t *testing.T) {
	testCases := []int64{
		math.MinInt64, math.MinInt64 + 1,
		-1<<56 - 1, -1 << 56,
		-1<<48 - 1, -1 << 48,
		-1<<40 - 1, -1 << 40,
		-1<<32 - 1, -1 << 32,
		-1<<24 - 1, -1 << 24,
		-1<<16 - 1, -1 << 16,
		-1<<8 - 1, -1 << 8,
		-1, 0, 1,
		1<<8 - 1, 1 << 8,
		1<<16 - 1, 1 << 16,
		1<<24 - 1, 1 << 24,
		1<<32 - 1, 1 << 32,
		1<<40 - 1, 1 << 40,
		1<<48 - 1, 1 << 48,
		1<<56 - 1, 1 << 56,
		math.MaxInt64 - 1, math.MaxInt64,
	}

	var lastEnc []byte
	for i, v := range testCases {
		enc := encFunc(nil, v)
		if i > 0 {
			if (decreasing && bytes.Compare(enc, lastEnc) >= 0) ||
				(!decreasing && bytes.Compare(enc, lastEnc) < 0) {
				t.Errorf("ordered constraint violated for %d: [% x] vs. [% x]", v, enc, lastEnc)
			}
		}
		b, decode, err := dec(enc)
		if err != nil {
			t.Errorf("%v: %d [%x]", err, v, enc)
			continue
		}
		if len(b) != 0 {
			t.Errorf("leftover bytes: [% x]", b)
		}
		if decode != v {
			t.Errorf("decode yielded different value than input: %d vs. %d [%x]", decode, v, enc)
		}
		lastEnc = enc
	}
}

type testCaseInt64 struct {
	value  int64
	expEnc []byte
}

func testCustomEncodeInt64(testCases []testCaseInt64,
	encFunc func([]byte, int64) []byte, t *testing.T) {
	for _, test := range testCases {
		enc := encFunc(nil, test.value)
		if bytes.Compare(enc, test.expEnc) != 0 {
			t.Errorf("expected [% x]; got [% x]", test.expEnc, enc)
		}
	}
}

type testCaseUint64 struct {
	value  uint64
	expEnc []byte
}

func testCustomEncodeUint64(testCases []testCaseUint64,
	encFunc func([]byte, uint64) []byte, t *testing.T) {
	for _, test := range testCases {
		enc := encFunc(nil, test.value)
		if bytes.Compare(enc, test.expEnc) != 0 {
			t.Errorf("expected [% x]; got [% x]", test.expEnc, enc)
		}
	}
}

func TestEncodeDecodeUint64(t *testing.T) {
	testBasicEncodeDecodeUint64(EncodeUint64, DecodeUint64, false, t)
	testCases := []testCaseUint64{
		{0, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{1, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{1 << 8, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00}},
		{math.MaxUint64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}
	testCustomEncodeUint64(testCases, EncodeUint64, t)
}

func TestEncodeDecodeUint64Decreasing(t *testing.T) {
	testBasicEncodeDecodeUint64(EncodeUint64Decreasing, DecodeUint64Decreasing, true, t)
	testCases := []testCaseUint64{
		{0, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{1, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{1 << 8, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 0xff}},
		{math.MaxUint64, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}
	testCustomEncodeUint64(testCases, EncodeUint64Decreasing, t)
}

func TestEncodeDecodeVarint(t *testing.T) {
	testBasicEncodeDecodeInt64(EncodeVarint, DecodeVarint, false, t)
	testCases := []testCaseInt64{
		{math.MinInt64, []byte{0x80, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{math.MinInt64 + 1, []byte{0x80, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{-1 << 8, []byte{0x86, 0xff, 0x00}},
		{-1, []byte{0x87, 0xff}},
		{0, []byte{0x88}},
		{1, []byte{0x89}},
		{111, []byte{0xf7}},
		{112, []byte{0xf8, 0x70}},
		{1 << 8, []byte{0xf9, 0x01, 0x00}},
		{math.MaxInt64, []byte{0xff, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}
	testCustomEncodeInt64(testCases, EncodeVarint, t)
}

func TestEncodeDecodeVarintDecreasing(t *testing.T) {
	testBasicEncodeDecodeInt64(EncodeVarintDecreasing, DecodeVarintDecreasing, true, t)
	testCases := []testCaseInt64{
		{math.MinInt64, []byte{0xff, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{math.MinInt64 + 1, []byte{0xff, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{-1 << 8, []byte{0xf8, 0xff}},
		{-112, []byte{0xf7}},
		{-111, []byte{0xf6}},
		{-1, []byte{0x88}},
		{0, []byte{0x87, 0xff}},
		{1, []byte{0x87, 0xfe}},
		{1 << 8, []byte{0x86, 0xfe, 0xff}},
		{math.MaxInt64, []byte{0x80, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}
	testCustomEncodeInt64(testCases, EncodeVarintDecreasing, t)
}

func TestEncodeDecodeUvarint(t *testing.T) {
	testBasicEncodeDecodeUint64(EncodeUvarint, DecodeUvarint, false, t)
	testCases := []testCaseUint64{
		{0, []byte{0x88}},
		{1, []byte{0x89}},
		{111, []byte{0xf7}},
		{112, []byte{0xf8, 0x70}},
		{1 << 8, []byte{0xf9, 0x01, 0x00}},
		{math.MaxUint64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}
	testCustomEncodeUint64(testCases, EncodeUvarint, t)
}

// TestDecodeInvalid tests that decoding invalid bytes panics.
func TestDecodeInvalid(t *testing.T) {
	tests := []struct {
		name    string             // name printed with errors.
		buf     []byte             // buf contains an invalid uvarint to decode.
		pattern string             // pattern matches the panic string.
		decode  func([]byte) error // decode is called with buf.
	}{
		{
			name:    "DecodeVarint, overflows int64",
			buf:     []byte{intMax, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			pattern: "varint [0-9]+ overflows int64",
			decode:  func(b []byte) error { _, _, err := DecodeVarint(b); return err },
		},
		{
			name:    "Bytes, no marker",
			buf:     []byte{'a'},
			pattern: "did not find marker",
			decode:  func(b []byte) error { _, _, err := DecodeBytes(b, nil); return err },
		},
		{
			name:    "Bytes, no terminator",
			buf:     []byte{bytesMarker, 'a'},
			pattern: "did not find terminator",
			decode:  func(b []byte) error { _, _, err := DecodeBytes(b, nil); return err },
		},
		{
			name:    "Bytes, malformed escape",
			buf:     []byte{bytesMarker, 'a', 0x00},
			pattern: "malformed escape",
			decode:  func(b []byte) error { _, _, err := DecodeBytes(b, nil); return err },
		},
		{
			name:    "Bytes, invalid escape 1",
			buf:     []byte{bytesMarker, 'a', 0x00, 0x00},
			pattern: "unknown escape",
			decode:  func(b []byte) error { _, _, err := DecodeBytes(b, nil); return err },
		},
		{
			name:    "Bytes, invalid escape 2",
			buf:     []byte{bytesMarker, 'a', 0x00, 0x02},
			pattern: "unknown escape",
			decode:  func(b []byte) error { _, _, err := DecodeBytes(b, nil); return err },
		},
		{
			name:    "BytesDecreasing, no marker",
			buf:     []byte{'a'},
			pattern: "did not find marker",
			decode:  func(b []byte) error { _, _, err := DecodeBytes(b, nil); return err },
		},
		{
			name:    "BytesDecreasing, no terminator",
			buf:     []byte{bytesMarker, ^byte('a')},
			pattern: "did not find terminator",
			decode:  func(b []byte) error { _, _, err := DecodeBytesDecreasing(b, nil); return err },
		},
		{
			name:    "BytesDecreasing, malformed escape",
			buf:     []byte{bytesMarker, ^byte('a'), 0xff},
			pattern: "malformed escape",
			decode:  func(b []byte) error { _, _, err := DecodeBytesDecreasing(b, nil); return err },
		},
		{
			name:    "BytesDecreasing, invalid escape 1",
			buf:     []byte{bytesMarker, ^byte('a'), 0xff, 0xff},
			pattern: "unknown escape",
			decode:  func(b []byte) error { _, _, err := DecodeBytesDecreasing(b, nil); return err },
		},
		{
			name:    "BytesDecreasing, invalid escape 2",
			buf:     []byte{bytesMarker, ^byte('a'), 0xff, 0xfd},
			pattern: "unknown escape",
			decode:  func(b []byte) error { _, _, err := DecodeBytesDecreasing(b, nil); return err },
		},
	}
	for _, test := range tests {
		err := test.decode(test.buf)
		if !regexp.MustCompile(test.pattern).MatchString(err.Error()) {
			t.Errorf("%q, pattern %q doesn't match %q", test.name, test.pattern, err)
		}
	}
}

func TestEncodeDecodeUvarintDecreasing(t *testing.T) {
	testBasicEncodeDecodeUint64(EncodeUvarintDecreasing, DecodeUvarintDecreasing, true, t)
	testCases := []testCaseUint64{
		{0, []byte{0x88}},
		{1, []byte{0x87, 0xfe}},
		{1 << 8, []byte{0x86, 0xfe, 0xff}},
		{math.MaxUint64 - 1, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{math.MaxUint64, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}
	testCustomEncodeUint64(testCases, EncodeUvarintDecreasing, t)
}

func TestEncodeDecodeBytes(t *testing.T) {
	testCases := []struct {
		value   []byte
		encoded []byte
	}{
		{[]byte{0, 1, 'a'}, []byte{0x20, 0x00, 0xff, 1, 'a', 0x00, 0x01}},
		{[]byte{0, 'a'}, []byte{0x20, 0x00, 0xff, 'a', 0x00, 0x01}},
		{[]byte{0, 0xff, 'a'}, []byte{0x20, 0x00, 0xff, 0xff, 'a', 0x00, 0x01}},
		{[]byte{'a'}, []byte{0x20, 'a', 0x00, 0x01}},
		{[]byte{'b'}, []byte{0x20, 'b', 0x00, 0x01}},
		{[]byte{'b', 0}, []byte{0x20, 'b', 0x00, 0xff, 0x00, 0x01}},
		{[]byte{'b', 0, 0}, []byte{0x20, 'b', 0x00, 0xff, 0x00, 0xff, 0x00, 0x01}},
		{[]byte{'b', 0, 0, 'a'}, []byte{0x20, 'b', 0x00, 0xff, 0x00, 0xff, 'a', 0x00, 0x01}},
		{[]byte{'b', 0xff}, []byte{0x20, 'b', 0xff, 0x00, 0x01}},
		{[]byte("hello"), []byte{0x20, 'h', 'e', 'l', 'l', 'o', 0x00, 0x01}},
	}
	for i, c := range testCases {
		enc := EncodeBytes(nil, c.value)
		if !bytes.Equal(enc, c.encoded) {
			t.Errorf("unexpected encoding mismatch for %v. expected [% x], got [% x]",
				c.value, c.encoded, enc)
		}
		if i > 0 {
			if bytes.Compare(testCases[i-1].encoded, enc) >= 0 {
				t.Errorf("%v: expected [% x] to be less than [% x]",
					c.value, testCases[i-1].encoded, enc)
			}
		}
		remainder, dec, err := DecodeBytes(enc, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if !bytes.Equal(c.value, dec) {
			t.Errorf("unexpected decoding mismatch for %v. got %v", c.value, dec)
		}
		if len(remainder) != 0 {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}

		enc = append(enc, []byte("remainder")...)
		remainder, _, err = DecodeBytes(enc, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if string(remainder) != "remainder" {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}
	}
}

func TestEncodeDecodeBytesDecreasing(t *testing.T) {
	testCases := []struct {
		value   []byte
		encoded []byte
	}{
		{[]byte("hello"), []byte{0x20, ^byte('h'), ^byte('e'), ^byte('l'), ^byte('l'), ^byte('o'), 0xff, 0xfe}},
		{[]byte{'b', 0xff}, []byte{0x20, ^byte('b'), 0x00, 0xff, 0xfe}},
		{[]byte{'b', 0, 0, 'a'}, []byte{0x20, ^byte('b'), 0xff, 0x00, 0xff, 0x00, ^byte('a'), 0xff, 0xfe}},
		{[]byte{'b', 0, 0}, []byte{0x20, ^byte('b'), 0xff, 0x00, 0xff, 0x00, 0xff, 0xfe}},
		{[]byte{'b', 0}, []byte{0x20, ^byte('b'), 0xff, 0x00, 0xff, 0xfe}},
		{[]byte{'b'}, []byte{0x20, ^byte('b'), 0xff, 0xfe}},
		{[]byte{'a'}, []byte{0x20, ^byte('a'), 0xff, 0xfe}},
		{[]byte{0, 0xff, 'a'}, []byte{0x20, 0xff, 0x00, 0x00, ^byte('a'), 0xff, 0xfe}},
		{[]byte{0, 'a'}, []byte{0x20, 0xff, 0x00, ^byte('a'), 0xff, 0xfe}},
		{[]byte{0, 1, 'a'}, []byte{0x20, 0xff, 0x00, 0xfe, ^byte('a'), 0xff, 0xfe}},
	}
	for i, c := range testCases {
		enc := EncodeBytesDecreasing(nil, c.value)
		if !bytes.Equal(enc, c.encoded) {
			t.Errorf("unexpected encoding mismatch for %v. expected [% x], got [% x]",
				c.value, c.encoded, enc)
		}
		if i > 0 {
			if bytes.Compare(testCases[i-1].encoded, enc) >= 0 {
				t.Errorf("%v: expected [% x] to be less than [% x]",
					c.value, testCases[i-1].encoded, enc)
			}
		}
		remainder, dec, err := DecodeBytesDecreasing(enc, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if !bytes.Equal(c.value, dec) {
			t.Errorf("unexpected decoding mismatch for %v. got %v", c.value, dec)
		}
		if len(remainder) != 0 {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}

		enc = append(enc, []byte("remainder")...)
		remainder, _, err = DecodeBytesDecreasing(enc, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if string(remainder) != "remainder" {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}
	}
}

func TestEncodeDecodeString(t *testing.T) {
	testCases := []struct {
		value   string
		encoded []byte
	}{
		{"\x00\x01a", []byte{0x20, 0x00, 0xff, 1, 'a', 0x00, 0x01}},
		{"\x00a", []byte{0x20, 0x00, 0xff, 'a', 0x00, 0x01}},
		{"\x00\xffa", []byte{0x20, 0x00, 0xff, 0xff, 'a', 0x00, 0x01}},
		{"a", []byte{0x20, 'a', 0x00, 0x01}},
		{"b", []byte{0x20, 'b', 0x00, 0x01}},
		{"b\x00", []byte{0x20, 'b', 0x00, 0xff, 0x00, 0x01}},
		{"b\x00\x00", []byte{0x20, 'b', 0x00, 0xff, 0x00, 0xff, 0x00, 0x01}},
		{"b\x00\x00a", []byte{0x20, 'b', 0x00, 0xff, 0x00, 0xff, 'a', 0x00, 0x01}},
		{"b\xff", []byte{0x20, 'b', 0xff, 0x00, 0x01}},
		{"hello", []byte{0x20, 'h', 'e', 'l', 'l', 'o', 0x00, 0x01}},
	}
	for i, c := range testCases {
		enc := EncodeString(nil, c.value)
		if !bytes.Equal(enc, c.encoded) {
			t.Errorf("unexpected encoding mismatch for %v. expected [% x], got [% x]",
				c.value, c.encoded, enc)
		}
		if i > 0 {
			if bytes.Compare(testCases[i-1].encoded, enc) >= 0 {
				t.Errorf("%v: expected [% x] to be less than [% x]",
					c.value, testCases[i-1].encoded, enc)
			}
		}
		remainder, dec, err := DecodeString(enc, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if c.value != dec {
			t.Errorf("unexpected decoding mismatch for %v. got %v", c.value, dec)
		}
		if len(remainder) != 0 {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}

		enc = append(enc, "remainder"...)
		remainder, _, err = DecodeString(enc, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if string(remainder) != "remainder" {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}
	}
}

func TestEncodeDecodeStringDecreasing(t *testing.T) {
	testCases := []struct {
		value   string
		encoded []byte
	}{
		{"hello", []byte{0x20, ^byte('h'), ^byte('e'), ^byte('l'), ^byte('l'), ^byte('o'), 0xff, 0xfe}},
		{"b\xff", []byte{0x20, ^byte('b'), 0x00, 0xff, 0xfe}},
		{"b\x00\x00a", []byte{0x20, ^byte('b'), 0xff, 0x00, 0xff, 0x00, ^byte('a'), 0xff, 0xfe}},
		{"b\x00\x00", []byte{0x20, ^byte('b'), 0xff, 0x00, 0xff, 0x00, 0xff, 0xfe}},
		{"b\x00", []byte{0x20, ^byte('b'), 0xff, 0x00, 0xff, 0xfe}},
		{"b", []byte{0x20, ^byte('b'), 0xff, 0xfe}},
		{"a", []byte{0x20, ^byte('a'), 0xff, 0xfe}},
		{"\x00\xffa", []byte{0x20, 0xff, 0x00, 0x00, ^byte('a'), 0xff, 0xfe}},
		{"\x00a", []byte{0x20, 0xff, 0x00, ^byte('a'), 0xff, 0xfe}},
		{"\x00\x01a", []byte{0x20, 0xff, 0x00, 0xfe, ^byte('a'), 0xff, 0xfe}},
	}
	for i, c := range testCases {
		enc := EncodeStringDecreasing(nil, c.value)
		if !bytes.Equal(enc, c.encoded) {
			t.Errorf("unexpected encoding mismatch for %v. expected [% x], got [% x]",
				c.value, c.encoded, enc)
		}
		if i > 0 {
			if bytes.Compare(testCases[i-1].encoded, enc) >= 0 {
				t.Errorf("%v: expected [% x] to be less than [% x]",
					c.value, testCases[i-1].encoded, enc)
			}
		}
		remainder, dec, err := DecodeStringDecreasing(enc, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if c.value != dec {
			t.Errorf("unexpected decoding mismatch for %v. got %v", c.value, dec)
		}
		if len(remainder) != 0 {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}

		enc = append(enc, "remainder"...)
		remainder, _, err = DecodeStringDecreasing(enc, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if string(remainder) != "remainder" {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}
	}
}

func TestEncodeDecodeNull(t *testing.T) {
	const hello = "hello"

	buf := EncodeNull([]byte(hello))
	expected := []byte(hello + "\x00")
	if !bytes.Equal(expected, buf) {
		t.Fatalf("expected %q, but found %q", expected, buf)
	}

	if remaining, isNull := DecodeIfNull([]byte(hello)); isNull {
		t.Fatalf("expected isNull=false, but found isNull=%v", isNull)
	} else if hello != string(remaining) {
		t.Fatalf("expected %q, but found %q", hello, remaining)
	}

	if remaining, isNull := DecodeIfNull([]byte("\x00" + hello)); !isNull {
		t.Fatalf("expected isNull=true, but found isNull=%v", isNull)
	} else if hello != string(remaining) {
		t.Fatalf("expected %q, but found %q", hello, remaining)
	}
}

func TestEncodeDecodeTime(t *testing.T) {
	zeroTime := time.Unix(0, 0)

	// test cases are negative, increasing, duration offsets from the
	// zeroTime. The positive, increasing, duration offsets are automatically
	// genarated below.
	testCases := []string{
		"-1345600h45m34s234ms",
		"-600h45m34s234ms",
		"-590h47m34s234ms",
		"-310h45m34s234ms",
		"-310h45m34s233ms",
		"-25h45m34s234ms",
		"-23h45m35s",
		"-23h45m34s999999999ns",
		"-23h45m34s234ms",
		"-23h45m34s101ms",
		"-23h45m34s1ns",
		"-23h45m34s",
		"-23h45m33s901ms",
		"-23h45m",
		"-23h",
		"-23612ms",
		"-345ms",
		"-1ms",
		"-201us",
		"-1us",
		"-201ns",
		"-1ns",
		"0",
	}

	// Append all the positive values in ascending order, excluding zero.
	for i := len(testCases) - 2; i >= 0; i-- {
		testCases = append(testCases, testCases[i][1:])
	}

	var last time.Time
	var lastEncoded []byte
	for i := range testCases {
		d, err := time.ParseDuration(testCases[i])
		if err != nil {
			t.Fatal(err)
		}
		current := zeroTime.Add(d)
		var b []byte
		if !last.IsZero() {
			b = EncodeTime(b, current)
			_, decodedCurrent, err := DecodeTime(b)
			if err != nil {
				t.Error(err)
				continue
			}
			if !decodedCurrent.Equal(current) {
				t.Fatalf("lossy transport: before (%v) vs after (%v)", current, decodedCurrent)
			}
			if bytes.Compare(lastEncoded, b) >= 0 {
				t.Fatalf("encodings %s, %s not increasing", testCases[i-1], testCases[i])
			}
		}
		last = current
		lastEncoded = b
	}

	// Check that the encoding hasn't changed.
	if a, e := lastEncoded, []byte("\x21\xfc\x01 \xbc\x0e\xae\xfb\r\xf2\x8e\x80"); !bytes.Equal(a, e) {
		t.Errorf("encoding has changed:\nexpected %x\nactual   %x", e, a)
	}
}

func TestPeekType(t *testing.T) {
	testCases := []struct {
		enc []byte
		typ Type
	}{
		{EncodeNull(nil), Null},
		{EncodeNotNull(nil), NotNull},
		{EncodeVarint(nil, 0), Int},
		{EncodeUvarint(nil, 0), Int},
		{EncodeFloat(nil, 0), Float},
		{EncodeBytes(nil, []byte("")), Bytes},
		{EncodeTime(nil, time.Now()), Time},
	}
	for i, c := range testCases {
		typ := PeekType(c.enc)
		if c.typ != typ {
			t.Fatalf("%d: expected %d, but found %d", i, c.typ, typ)
		}
	}
}

func BenchmarkEncodeUint32(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]uint32, 10000)
	for i := range vals {
		vals[i] = uint32(rng.Int31())
	}

	buf := make([]byte, 0, 16)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeUint32(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeUint32(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUint32(nil, uint32(rng.Int31()))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeUint32(vals[i%len(vals)])
	}
}

func BenchmarkEncodeUint64(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]uint64, 10000)
	for i := range vals {
		vals[i] = uint64(rng.Int63())
	}

	buf := make([]byte, 0, 16)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeUint64(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeUint64(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUint64(nil, uint64(rng.Int63()))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeUint64(vals[i%len(vals)])
	}
}

func BenchmarkEncodeVarint(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]int64, 10000)
	for i := range vals {
		vals[i] = rng.Int63()
	}

	buf := make([]byte, 0, 16)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeVarint(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeVarint(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeVarint(nil, rng.Int63())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeVarint(vals[i%len(vals)])
	}
}

func BenchmarkEncodeUvarint(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]uint64, 10000)
	for i := range vals {
		vals[i] = uint64(rng.Int63())
	}

	buf := make([]byte, 0, 16)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeUvarint(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeUvarint(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUvarint(nil, uint64(rng.Int63()))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeUvarint(vals[i%len(vals)])
	}
}

func BenchmarkEncodeBytes(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = randutil.RandBytes(rng, 100)
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeBytes(buf, vals[i%len(vals)])
	}
}

func BenchmarkEncodeBytesDecreasing(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = randutil.RandBytes(rng, 100)
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeBytesDecreasing(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeBytes(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeBytes(nil, randutil.RandBytes(rng, 100))
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeBytes(vals[i%len(vals)], buf)
	}
}

func BenchmarkDecodeBytesDecreasing(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeBytesDecreasing(nil, randutil.RandBytes(rng, 100))
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeBytesDecreasing(vals[i%len(vals)], buf)
	}
}

func BenchmarkEncodeString(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]string, 10000)
	for i := range vals {
		vals[i] = string(randutil.RandBytes(rng, 100))
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeString(buf, vals[i%len(vals)])
	}
}

func BenchmarkEncodeStringDecreasing(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]string, 10000)
	for i := range vals {
		vals[i] = string(randutil.RandBytes(rng, 100))
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeStringDecreasing(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeString(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeString(nil, string(randutil.RandBytes(rng, 100)))
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeString(vals[i%len(vals)], buf)
	}
}

func BenchmarkDecodeStringDecreasing(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeStringDecreasing(nil, string(randutil.RandBytes(rng, 100)))
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeStringDecreasing(vals[i%len(vals)], buf)
	}
}
