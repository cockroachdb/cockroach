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
	"regexp"
	"testing"
	"time"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/util/duration"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

func testBasicEncodeDecode32(encFunc func([]byte, uint32) []byte,
	decFunc func([]byte) ([]byte, uint32, error), descending bool, t *testing.T) {
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
			if (descending && bytes.Compare(enc, lastEnc) >= 0) ||
				(!descending && bytes.Compare(enc, lastEnc) < 0) {
				t.Errorf("ordered constraint violated for %d: [% x] vs. [% x]", v, enc, lastEnc)
			}
		}
		b, decode, err := decFunc(enc)
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
	testBasicEncodeDecode32(EncodeUint32Ascending, DecodeUint32Ascending, false, t)
	testCases := []testCaseUint32{
		{0, []byte{0x00, 0x00, 0x00, 0x00}},
		{1, []byte{0x00, 0x00, 0x00, 0x01}},
		{1 << 8, []byte{0x00, 0x00, 0x01, 0x00}},
		{math.MaxUint32, []byte{0xff, 0xff, 0xff, 0xff}},
	}
	testCustomEncodeUint32(testCases, EncodeUint32Ascending, t)
}

func TestEncodeDecodeUint32Descending(t *testing.T) {
	testBasicEncodeDecode32(EncodeUint32Descending, DecodeUint32Descending, true, t)
	testCases := []testCaseUint32{
		{0, []byte{0xff, 0xff, 0xff, 0xff}},
		{1, []byte{0xff, 0xff, 0xff, 0xfe}},
		{1 << 8, []byte{0xff, 0xff, 0xfe, 0xff}},
		{math.MaxUint32, []byte{0x00, 0x00, 0x00, 0x00}},
	}
	testCustomEncodeUint32(testCases, EncodeUint32Descending, t)
}

func testBasicEncodeDecodeUint64(encFunc func([]byte, uint64) []byte,
	decFunc func([]byte) ([]byte, uint64, error), descending bool, t *testing.T) {
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
			if (descending && bytes.Compare(enc, lastEnc) >= 0) ||
				(!descending && bytes.Compare(enc, lastEnc) < 0) {
				t.Errorf("ordered constraint violated for %d: [% x] vs. [% x]", v, enc, lastEnc)
			}
		}
		b, decode, err := decFunc(enc)
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
	decFunc func([]byte) ([]byte, int64, error), descending bool, t *testing.T) {
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
			if (descending && bytes.Compare(enc, lastEnc) >= 0) ||
				(!descending && bytes.Compare(enc, lastEnc) < 0) {
				t.Errorf("ordered constraint violated for %d: [% x] vs. [% x]", v, enc, lastEnc)
			}
		}
		b, decode, err := decFunc(enc)
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
			t.Errorf("expected [% x]; got [% x] (value: %d)", test.expEnc, enc, test.value)
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
			t.Errorf("expected [% x]; got [% x] (value: %d)", test.expEnc, enc, test.value)
		}
	}
}

func TestEncodeDecodeUint64(t *testing.T) {
	testBasicEncodeDecodeUint64(EncodeUint64Ascending, DecodeUint64Ascending, false, t)
	testCases := []testCaseUint64{
		{0, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{1, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{1 << 8, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00}},
		{math.MaxUint64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}
	testCustomEncodeUint64(testCases, EncodeUint64Ascending, t)
}

func TestEncodeDecodeUint64Descending(t *testing.T) {
	testBasicEncodeDecodeUint64(EncodeUint64Descending, DecodeUint64Descending, true, t)
	testCases := []testCaseUint64{
		{0, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{1, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{1 << 8, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 0xff}},
		{math.MaxUint64, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}
	testCustomEncodeUint64(testCases, EncodeUint64Descending, t)
}

func TestEncodeDecodeVarint(t *testing.T) {
	testBasicEncodeDecodeInt64(EncodeVarintAscending, DecodeVarintAscending, false, t)
	testCases := []testCaseInt64{
		{math.MinInt64, []byte{0x80, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{math.MinInt64 + 1, []byte{0x80, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{-1 << 8, []byte{0x86, 0xff, 0x00}},
		{-1, []byte{0x87, 0xff}},
		{0, []byte{0x88}},
		{1, []byte{0x89}},
		{109, []byte{0xf5}},
		{112, []byte{0xf6, 0x70}},
		{1 << 8, []byte{0xf7, 0x01, 0x00}},
		{math.MaxInt64, []byte{0xfd, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}
	testCustomEncodeInt64(testCases, EncodeVarintAscending, t)
}

func TestEncodeDecodeVarintDescending(t *testing.T) {
	testBasicEncodeDecodeInt64(EncodeVarintDescending, DecodeVarintDescending, true, t)
	testCases := []testCaseInt64{
		{math.MinInt64, []byte{0xfd, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{math.MinInt64 + 1, []byte{0xfd, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{-1 << 8, []byte{0xf6, 0xff}},
		{-110, []byte{0xf5}},
		{-1, []byte{0x88}},
		{0, []byte{0x87, 0xff}},
		{1, []byte{0x87, 0xfe}},
		{1 << 8, []byte{0x86, 0xfe, 0xff}},
		{math.MaxInt64, []byte{0x80, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}
	testCustomEncodeInt64(testCases, EncodeVarintDescending, t)
}

func TestEncodeDecodeUvarint(t *testing.T) {
	testBasicEncodeDecodeUint64(EncodeUvarintAscending, DecodeUvarintAscending, false, t)
	testCases := []testCaseUint64{
		{0, []byte{0x88}},
		{1, []byte{0x89}},
		{109, []byte{0xf5}},
		{110, []byte{0xf6, 0x6e}},
		{1 << 8, []byte{0xf7, 0x01, 0x00}},
		{math.MaxUint64, []byte{0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}
	testCustomEncodeUint64(testCases, EncodeUvarintAscending, t)
}

func TestEncodeDecodeUvarintDescending(t *testing.T) {
	testBasicEncodeDecodeUint64(EncodeUvarintDescending, DecodeUvarintDescending, true, t)
	testCases := []testCaseUint64{
		{0, []byte{0x88}},
		{1, []byte{0x87, 0xfe}},
		{1 << 8, []byte{0x86, 0xfe, 0xff}},
		{math.MaxUint64 - 1, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{math.MaxUint64, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}
	testCustomEncodeUint64(testCases, EncodeUvarintDescending, t)
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
			buf:     []byte{IntMax, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			pattern: "varint [0-9]+ overflows int64",
			decode:  func(b []byte) error { _, _, err := DecodeVarintAscending(b); return err },
		},
		{
			name:    "Bytes, no marker",
			buf:     []byte{'a'},
			pattern: "did not find marker",
			decode:  func(b []byte) error { _, _, err := DecodeBytesAscending(b, nil); return err },
		},
		{
			name:    "Bytes, no terminator",
			buf:     []byte{bytesMarker, 'a'},
			pattern: "did not find terminator",
			decode:  func(b []byte) error { _, _, err := DecodeBytesAscending(b, nil); return err },
		},
		{
			name:    "Bytes, malformed escape",
			buf:     []byte{bytesMarker, 'a', 0x00},
			pattern: "malformed escape",
			decode:  func(b []byte) error { _, _, err := DecodeBytesAscending(b, nil); return err },
		},
		{
			name:    "Bytes, invalid escape 1",
			buf:     []byte{bytesMarker, 'a', 0x00, 0x00},
			pattern: "unknown escape",
			decode:  func(b []byte) error { _, _, err := DecodeBytesAscending(b, nil); return err },
		},
		{
			name:    "Bytes, invalid escape 2",
			buf:     []byte{bytesMarker, 'a', 0x00, 0x02},
			pattern: "unknown escape",
			decode:  func(b []byte) error { _, _, err := DecodeBytesAscending(b, nil); return err },
		},
		{
			name:    "BytesDescending, no marker",
			buf:     []byte{'a'},
			pattern: "did not find marker",
			decode:  func(b []byte) error { _, _, err := DecodeBytesAscending(b, nil); return err },
		},
		{
			name:    "BytesDescending, no terminator",
			buf:     []byte{bytesDescMarker, ^byte('a')},
			pattern: "did not find terminator",
			decode:  func(b []byte) error { _, _, err := DecodeBytesDescending(b, nil); return err },
		},
		{
			name:    "BytesDescending, malformed escape",
			buf:     []byte{bytesDescMarker, ^byte('a'), 0xff},
			pattern: "malformed escape",
			decode:  func(b []byte) error { _, _, err := DecodeBytesDescending(b, nil); return err },
		},
		{
			name:    "BytesDescending, invalid escape 1",
			buf:     []byte{bytesDescMarker, ^byte('a'), 0xff, 0xff},
			pattern: "unknown escape",
			decode:  func(b []byte) error { _, _, err := DecodeBytesDescending(b, nil); return err },
		},
		{
			name:    "BytesDescending, invalid escape 2",
			buf:     []byte{bytesDescMarker, ^byte('a'), 0xff, 0xfd},
			pattern: "unknown escape",
			decode:  func(b []byte) error { _, _, err := DecodeBytesDescending(b, nil); return err },
		},
		{
			name:    "Decimal, malformed uvarint",
			buf:     []byte{decimalPosLarge},
			pattern: "insufficient bytes to decode uvarint value",
			decode:  func(b []byte) error { _, _, err := DecodeDecimalAscending(b, nil); return err },
		},
		{
			name:    "DecimalDescending, malformed uvarint",
			buf:     []byte{decimalPosLarge},
			pattern: "insufficient bytes to decode uvarint value",
			decode:  func(b []byte) error { _, _, err := DecodeDecimalDescending(b, nil); return err },
		},
	}
	for _, test := range tests {
		err := test.decode(test.buf)
		if !regexp.MustCompile(test.pattern).MatchString(err.Error()) {
			t.Errorf("%q, pattern %q doesn't match %q", test.name, test.pattern, err)
		}
	}
}

func TestEncodeDecodeBytes(t *testing.T) {
	testCases := []struct {
		value   []byte
		encoded []byte
	}{
		{[]byte{0, 1, 'a'}, []byte{0x12, 0x00, 0xff, 1, 'a', 0x00, 0x01}},
		{[]byte{0, 'a'}, []byte{0x12, 0x00, 0xff, 'a', 0x00, 0x01}},
		{[]byte{0, 0xff, 'a'}, []byte{0x12, 0x00, 0xff, 0xff, 'a', 0x00, 0x01}},
		{[]byte{'a'}, []byte{0x12, 'a', 0x00, 0x01}},
		{[]byte{'b'}, []byte{0x12, 'b', 0x00, 0x01}},
		{[]byte{'b', 0}, []byte{0x12, 'b', 0x00, 0xff, 0x00, 0x01}},
		{[]byte{'b', 0, 0}, []byte{0x12, 'b', 0x00, 0xff, 0x00, 0xff, 0x00, 0x01}},
		{[]byte{'b', 0, 0, 'a'}, []byte{0x12, 'b', 0x00, 0xff, 0x00, 0xff, 'a', 0x00, 0x01}},
		{[]byte{'b', 0xff}, []byte{0x12, 'b', 0xff, 0x00, 0x01}},
		{[]byte("hello"), []byte{0x12, 'h', 'e', 'l', 'l', 'o', 0x00, 0x01}},
	}
	for i, c := range testCases {
		enc := EncodeBytesAscending(nil, c.value)
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
		remainder, dec, err := DecodeBytesAscending(enc, nil)
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
		remainder, _, err = DecodeBytesAscending(enc, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if string(remainder) != "remainder" {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}
	}
}

func TestEncodeDecodeBytesDescending(t *testing.T) {
	testCases := []struct {
		value   []byte
		encoded []byte
	}{
		{[]byte("hello"), []byte{0x13, ^byte('h'), ^byte('e'), ^byte('l'), ^byte('l'), ^byte('o'), 0xff, 0xfe}},
		{[]byte{'b', 0xff}, []byte{0x13, ^byte('b'), 0x00, 0xff, 0xfe}},
		{[]byte{'b', 0, 0, 'a'}, []byte{0x13, ^byte('b'), 0xff, 0x00, 0xff, 0x00, ^byte('a'), 0xff, 0xfe}},
		{[]byte{'b', 0, 0}, []byte{0x13, ^byte('b'), 0xff, 0x00, 0xff, 0x00, 0xff, 0xfe}},
		{[]byte{'b', 0}, []byte{0x13, ^byte('b'), 0xff, 0x00, 0xff, 0xfe}},
		{[]byte{'b'}, []byte{0x13, ^byte('b'), 0xff, 0xfe}},
		{[]byte{'a'}, []byte{0x13, ^byte('a'), 0xff, 0xfe}},
		{[]byte{0, 0xff, 'a'}, []byte{0x13, 0xff, 0x00, 0x00, ^byte('a'), 0xff, 0xfe}},
		{[]byte{0, 'a'}, []byte{0x13, 0xff, 0x00, ^byte('a'), 0xff, 0xfe}},
		{[]byte{0, 1, 'a'}, []byte{0x13, 0xff, 0x00, 0xfe, ^byte('a'), 0xff, 0xfe}},
	}
	for i, c := range testCases {
		enc := EncodeBytesDescending(nil, c.value)
		if !bytes.Equal(enc, c.encoded) {
			t.Errorf("%d: unexpected encoding mismatch for %v ([% x]). expected [% x], got [% x]",
				i, c.value, c.value, c.encoded, enc)
		}
		if i > 0 {
			if bytes.Compare(testCases[i-1].encoded, enc) >= 0 {
				t.Errorf("%v: expected [% x] to be less than [% x]",
					c.value, testCases[i-1].encoded, enc)
			}
		}
		remainder, dec, err := DecodeBytesDescending(enc, nil)
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
		remainder, _, err = DecodeBytesDescending(enc, nil)
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
		{"\x00\x01a", []byte{0x12, 0x00, 0xff, 1, 'a', 0x00, 0x01}},
		{"\x00a", []byte{0x12, 0x00, 0xff, 'a', 0x00, 0x01}},
		{"\x00\xffa", []byte{0x12, 0x00, 0xff, 0xff, 'a', 0x00, 0x01}},
		{"a", []byte{0x12, 'a', 0x00, 0x01}},
		{"b", []byte{0x12, 'b', 0x00, 0x01}},
		{"b\x00", []byte{0x12, 'b', 0x00, 0xff, 0x00, 0x01}},
		{"b\x00\x00", []byte{0x12, 'b', 0x00, 0xff, 0x00, 0xff, 0x00, 0x01}},
		{"b\x00\x00a", []byte{0x12, 'b', 0x00, 0xff, 0x00, 0xff, 'a', 0x00, 0x01}},
		{"b\xff", []byte{0x12, 'b', 0xff, 0x00, 0x01}},
		{"hello", []byte{0x12, 'h', 'e', 'l', 'l', 'o', 0x00, 0x01}},
	}
	for i, c := range testCases {
		enc := EncodeStringAscending(nil, c.value)
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
		remainder, dec, err := DecodeStringAscending(enc, nil)
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
		remainder, _, err = DecodeStringAscending(enc, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if string(remainder) != "remainder" {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}
	}
}

func TestEncodeDecodeStringDescending(t *testing.T) {
	testCases := []struct {
		value   string
		encoded []byte
	}{
		{"hello", []byte{0x13, ^byte('h'), ^byte('e'), ^byte('l'), ^byte('l'), ^byte('o'), 0xff, 0xfe}},
		{"b\xff", []byte{0x13, ^byte('b'), 0x00, 0xff, 0xfe}},
		{"b\x00\x00a", []byte{0x13, ^byte('b'), 0xff, 0x00, 0xff, 0x00, ^byte('a'), 0xff, 0xfe}},
		{"b\x00\x00", []byte{0x13, ^byte('b'), 0xff, 0x00, 0xff, 0x00, 0xff, 0xfe}},
		{"b\x00", []byte{0x13, ^byte('b'), 0xff, 0x00, 0xff, 0xfe}},
		{"b", []byte{0x13, ^byte('b'), 0xff, 0xfe}},
		{"a", []byte{0x13, ^byte('a'), 0xff, 0xfe}},
		{"\x00\xffa", []byte{0x13, 0xff, 0x00, 0x00, ^byte('a'), 0xff, 0xfe}},
		{"\x00a", []byte{0x13, 0xff, 0x00, ^byte('a'), 0xff, 0xfe}},
		{"\x00\x01a", []byte{0x13, 0xff, 0x00, 0xfe, ^byte('a'), 0xff, 0xfe}},
	}
	for i, c := range testCases {
		enc := EncodeStringDescending(nil, c.value)
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
		remainder, dec, err := DecodeStringDescending(enc, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if c.value != dec {
			t.Errorf("unexpected decoding mismatch for %v. got [% x]", c.value, dec)
		}
		if len(remainder) != 0 {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}

		enc = append(enc, "remainder"...)
		remainder, _, err = DecodeStringDescending(enc, nil)
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

	buf := EncodeNullAscending([]byte(hello))
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
	for _, dir := range []Direction{Ascending, Descending} {
		for i := range testCases {
			d, err := time.ParseDuration(testCases[i])
			if err != nil {
				t.Fatal(err)
			}
			current := zeroTime.Add(d)
			var b []byte
			var decodedCurrent time.Time
			if !last.IsZero() {
				if dir == Ascending {
					b = EncodeTimeAscending(b, current)
					_, decodedCurrent, err = DecodeTimeAscending(b)
				} else {
					b = EncodeTimeDescending(b, current)
					_, decodedCurrent, err = DecodeTimeDescending(b)
				}
				if err != nil {
					t.Error(err)
					continue
				}
				if !decodedCurrent.Equal(current) {
					t.Fatalf("lossy transport: before (%v) vs after (%v)", current, decodedCurrent)
				}
				if i > 0 {
					if (bytes.Compare(lastEncoded, b) >= 0 && dir == Ascending) ||
						(bytes.Compare(lastEncoded, b) <= 0 && dir == Descending) {
						t.Fatalf("encodings %s, %s not increasing", testCases[i-1], testCases[i])
					}
				}
			}
			last = current
			lastEncoded = b
		}

		// Check that the encoding hasn't changed.
		if dir == Ascending {
			a, e := lastEncoded, []byte("\x14\xfa\x01 \xbc\x0e\xae\xf9\r\xf2\x8e\x80")
			if !bytes.Equal(a, e) {
				t.Errorf("encoding has changed:\nexpected [% x]\nactual   [% x]", e, a)
			}
		}
	}
}

type testCaseDuration struct {
	value  duration.Duration
	expEnc []byte
}

func testBasicEncodeDuration(
	testCases []testCaseDuration,
	encFunc func([]byte, duration.Duration) ([]byte, error),
	t *testing.T,
) {
	var lastEnc []byte
	for i, test := range testCases {
		enc, err := encFunc(nil, test.value)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(lastEnc, enc) != -1 {
			t.Errorf("%d ordered constraint violated for %s: [% x] vs. [% x]", i, test.value, enc, lastEnc)
		}
		lastEnc = enc
	}
}

func testCustomEncodeDuration(
	testCases []testCaseDuration,
	encFunc func([]byte, duration.Duration) ([]byte, error),
	decFunc func([]byte) ([]byte, duration.Duration, error),
	t *testing.T,
) {
	for i, test := range testCases {
		enc, err := encFunc(nil, test.value)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(enc, test.expEnc) != 0 {
			t.Errorf("%d expected [% x]; got [% x] (value: %d)", i, test.expEnc, enc, test.value)
		}
		_, decoded, err := decFunc(enc)
		if err != nil {
			t.Fatal(err)
		}
		if test.value != decoded {
			t.Errorf("%d duration changed during roundtrip [%s] vs [%s]", i, test.value, decoded)
		}
	}
}

func TestEncodeDecodeDuration(t *testing.T) {
	testCases := []testCaseDuration{
		{duration.Duration{Months: 0, Days: 0, Nanos: 0}, []byte{0x16, 0x88, 0x88, 0x88}},
		{duration.Duration{Months: 0, Days: 0, Nanos: 1}, []byte{0x16, 0x89, 0x88, 0x88}},
		{duration.Duration{Months: 0, Days: 1, Nanos: 0}, []byte{0x16, 0xfb, 0x4e, 0x94, 0x91, 0x4f, 0x00, 0x00, 0x88, 0x89}},
		{duration.Duration{Months: 1, Days: 0, Nanos: 0}, []byte{0x16, 0xfc, 0x09, 0x35, 0x69, 0x07, 0x42, 0x00, 0x00, 0x89, 0x88}},
		{duration.Duration{Months: 0, Days: 40, Nanos: 0}, []byte{0x16, 0xfc, 0x0c, 0x47, 0x36, 0xb4, 0x58, 0x00, 0x00, 0x88, 0xb0}},
	}
	testBasicEncodeDuration(testCases, EncodeDurationAscending, t)
	testCustomEncodeDuration(testCases, EncodeDurationAscending, DecodeDurationAscending, t)
}

func TestEncodeDecodeDescending(t *testing.T) {
	testCases := []testCaseDuration{
		{duration.Duration{Months: 0, Days: 40, Nanos: 0}, []byte{0x16, 0x81, 0xf3, 0xb8, 0xc9, 0x4b, 0xa7, 0xff, 0xff, 0x87, 0xff, 0x87, 0xd7}},
		{duration.Duration{Months: 1, Days: 0, Nanos: 0}, []byte{0x16, 0x81, 0xf6, 0xca, 0x96, 0xf8, 0xbd, 0xff, 0xff, 0x87, 0xfe, 0x87, 0xff}},
		{duration.Duration{Months: 0, Days: 1, Nanos: 0}, []byte{0x16, 0x82, 0xb1, 0x6b, 0x6e, 0xb0, 0xff, 0xff, 0x87, 0xff, 0x87, 0xfe}},
		{duration.Duration{Months: 0, Days: 0, Nanos: 1}, []byte{0x16, 0x87, 0xfe, 0x87, 0xff, 0x87, 0xff}},
		{duration.Duration{Months: 0, Days: 0, Nanos: 0}, []byte{0x16, 0x87, 0xff, 0x87, 0xff, 0x87, 0xff}},
	}
	testBasicEncodeDuration(testCases, EncodeDurationDescending, t)
	testCustomEncodeDuration(testCases, EncodeDurationDescending, DecodeDurationDescending, t)
}

func TestPeekType(t *testing.T) {
	encodedDurationAscending, _ := EncodeDurationAscending(nil, duration.Duration{})
	encodedDurationDescending, _ := EncodeDurationDescending(nil, duration.Duration{})
	testCases := []struct {
		enc []byte
		typ Type
	}{
		{EncodeNullAscending(nil), Null},
		{EncodeNotNullAscending(nil), NotNull},
		{EncodeNullDescending(nil), Null},
		{EncodeNotNullDescending(nil), NotNull},
		{EncodeVarintAscending(nil, 0), Int},
		{EncodeVarintDescending(nil, 0), Int},
		{EncodeUvarintAscending(nil, 0), Int},
		{EncodeUvarintDescending(nil, 0), Int},
		{EncodeFloatAscending(nil, 0), Float},
		{EncodeFloatDescending(nil, 0), Float},
		{EncodeDecimalAscending(nil, inf.NewDec(0, 0)), Decimal},
		{EncodeDecimalDescending(nil, inf.NewDec(0, 0)), Decimal},
		{EncodeBytesAscending(nil, []byte("")), Bytes},
		{EncodeBytesDescending(nil, []byte("")), BytesDesc},
		{EncodeTimeAscending(nil, timeutil.Now()), Time},
		{EncodeTimeDescending(nil, timeutil.Now()), Time},
		{encodedDurationAscending, Duration},
		{encodedDurationDescending, Duration},
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
		_ = EncodeUint32Ascending(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeUint32(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUint32Ascending(nil, uint32(rng.Int31()))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeUint32Ascending(vals[i%len(vals)])
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
		_ = EncodeUint64Ascending(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeUint64(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUint64Ascending(nil, uint64(rng.Int63()))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeUint64Ascending(vals[i%len(vals)])
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
		_ = EncodeVarintAscending(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeVarint(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeVarintAscending(nil, rng.Int63())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeVarintAscending(vals[i%len(vals)])
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
		_ = EncodeUvarintAscending(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeUvarint(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUvarintAscending(nil, uint64(rng.Int63()))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeUvarintAscending(vals[i%len(vals)])
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
		_ = EncodeBytesAscending(buf, vals[i%len(vals)])
	}
}

func BenchmarkEncodeBytesDescending(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = randutil.RandBytes(rng, 100)
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeBytesDescending(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeBytes(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeBytesAscending(nil, randutil.RandBytes(rng, 100))
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeBytesAscending(vals[i%len(vals)], buf)
	}
}

func BenchmarkDecodeBytesDescending(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeBytesDescending(nil, randutil.RandBytes(rng, 100))
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeBytesDescending(vals[i%len(vals)], buf)
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
		_ = EncodeStringAscending(buf, vals[i%len(vals)])
	}
}

func BenchmarkEncodeStringDescending(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]string, 10000)
	for i := range vals {
		vals[i] = string(randutil.RandBytes(rng, 100))
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeStringDescending(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeString(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeStringAscending(nil, string(randutil.RandBytes(rng, 100)))
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeStringAscending(vals[i%len(vals)], buf)
	}
}

func BenchmarkDecodeStringDescending(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeStringDescending(nil, string(randutil.RandBytes(rng, 100)))
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeStringDescending(vals[i%len(vals)], buf)
	}
}

func BenchmarkEncodeDuration(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]duration.Duration, 10000)
	for i := range vals {
		vals[i] = duration.Duration{Months: rng.Int63(), Days: rng.Int63(), Nanos: rng.Int63()}
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeDurationAscending(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeDuration(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		d := duration.Duration{Months: rng.Int63(), Days: rng.Int63(), Nanos: rng.Int63()}
		vals[i], _ = EncodeDurationAscending(nil, d)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeDurationAscending(vals[i%len(vals)])
	}
}
