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
	"fmt"
	"io"
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/randutil"
)

func testBasicEncodeDecode32(encFunc func([]byte, uint32) []byte,
	decFunc func(Reader) (uint32, error), decreasing bool, t *testing.T) {
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
		var dirs []Direction
		if decreasing {
			dirs = []Direction{Descending}
		} else {
			dirs = []Direction{Ascending}
		}
		r := NewKeyReader(enc, dirs)
		decode, err := decFunc(r)
		if err != nil {
			t.Error(err)
			continue
		}
		if !r.EOF() {
			t.Errorf("leftover bytes: [% x]", r.RawBytesRemaining())
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
	testBasicEncodeDecode32(EncodeUint32Decreasing, DecodeUint32, true, t)
	testCases := []testCaseUint32{
		{0, []byte{0xff, 0xff, 0xff, 0xff}},
		{1, []byte{0xff, 0xff, 0xff, 0xfe}},
		{1 << 8, []byte{0xff, 0xff, 0xfe, 0xff}},
		{math.MaxUint32, []byte{0x00, 0x00, 0x00, 0x00}},
	}
	testCustomEncodeUint32(testCases, EncodeUint32Decreasing, t)
}

func testBasicEncodeDecodeUint64(encFunc func([]byte, uint64) []byte,
	decFunc func(Reader) (uint64, error), decreasing bool, t *testing.T) {
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
		var dirs []Direction
		if decreasing {
			dirs = []Direction{Descending}
		} else {
			dirs = []Direction{Ascending}
		}
		r := NewKeyReader(enc, dirs)
		decode, err := decFunc(r)
		if err != nil {
			t.Error(err)
			continue
		}
		if !r.EOF() {
			t.Errorf("leftover bytes: [% x]", r.RawBytesRemaining())
		}
		if decode != v {
			t.Errorf("decode yielded different value than input: %d vs. %d", decode, v)
		}
		lastEnc = enc
	}
}

func testBasicEncodeDecodeInt64(encFunc func([]byte, int64) []byte,
	decFunc func(Reader) (int64, error), decreasing bool, t *testing.T) {
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

	var dirs []Direction
	if decreasing {
		dirs = []Direction{Descending}
	} else {
		dirs = []Direction{Ascending}
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
		r := NewKeyReader(enc, dirs)
		decode, err := decFunc(r)
		if err != nil {
			t.Errorf("%v: %d [%x]", err, v, enc)
			continue
		}
		if !r.EOF() {
			t.Errorf("leftover bytes: [% x]", r.RawBytesRemaining())
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
	testBasicEncodeDecodeUint64(EncodeUint64Decreasing, DecodeUint64, true, t)
	nums := []uint64{0, 1, 1 << 8, math.MaxUint64}
	var testCases []testCaseUint64
	for _, n := range nums {
		enc := EncodeUint64(nil, n)
		onesComplement(enc)
		testCases = append(testCases, testCaseUint64{n, enc})
	}
	testCustomEncodeUint64(testCases, EncodeUint64Decreasing, t)
}

// !!! testing - delete this
func TestEncodeDecodeVarintXXX(t *testing.T) {
	testBasicEncodeDecodeInt64XXX(EncodeVarint, DecodeVarintXXX, false, t)
}
func TestEncodeDecodeVarintDecreasingXXX(t *testing.T) {
	testBasicEncodeDecodeInt64XXX(EncodeVarintDecreasing, DecodeVarintXXX, true, t)
}

// !!! delete this
func testBasicEncodeDecodeInt64XXX(encFunc func([]byte, int64) []byte,
	decFunc func(*RReader) (int64, error), decreasing bool, t *testing.T) {
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

	//var dirs []Direction
	//if decreasing {
	//  dirs = []Direction{Descending}
	//} else {
	//  dirs = []Direction{Ascending}
	//}
	var lastEnc []byte
	for i, v := range testCases {
		enc := encFunc(nil, v)
		if i > 0 {
			if (decreasing && bytes.Compare(enc, lastEnc) >= 0) ||
				(!decreasing && bytes.Compare(enc, lastEnc) < 0) {
				t.Errorf("ordered constraint violated for %d: [% x] vs. [% x]", v, enc, lastEnc)
			}
		}
		lastEnc = make([]byte, len(enc))
		copy(lastEnc, enc)

		fmt.Printf("!!! TESTING value: %d encoded: %v\n", v, enc)

		var r *RReader
		r = NewRReader(enc) // !!!, dirs)
		if decreasing {     // !!! move to ctor
			r.dir = Descending
		}
		decode, err := decFunc(r)
		if err != nil {
			t.Errorf("%v: %d [%x]", err, v, enc)
			continue
		}
		if !r.EOF() {
			t.Errorf("leftover bytes: [% x]", r.RawBytesRemaining())
		}
		if decode != v {
			// !!!
			t.Fatalf("decode yielded different value than input: %d vs. %d [%x]", decode, v, enc)
			//t.Errorf("decode yielded different value than input: %d vs. %d [%x]", decode, v, enc)
		}
	}
}

// !!! unify all these pairs of tests after I make the encoding be direction-agnostic
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
	testBasicEncodeDecodeInt64(EncodeVarintDecreasing, DecodeVarint, true, t)
	nums := []int64{math.MinInt64, math.MinInt64 + 1, -1 << 8,
		-112, -111, -1, 0, 1, 1 << 8, math.MaxInt64}
	var testCases []testCaseInt64
	for _, n := range nums {
		enc := EncodeVarint(nil, n)
		onesComplement(enc)
		testCases = append(testCases, testCaseInt64{n, enc})
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
		desc    bool               // true if we decode as a descendingly-encoded buffer
		pattern string             // pattern matches the panic string.
		decode  func(Reader) error // decode is called with buf.
	}{
		{
			name:    "DecodeVarint, overflows int64",
			buf:     []byte{IntMax, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			desc:    false,
			pattern: "varint [0-9]+ overflows int64",
			decode:  func(r Reader) error { _, err := DecodeVarint(r); return err },
		},
		{
			name:    "Bytes, no marker",
			buf:     []byte{'a'},
			desc:    false,
			pattern: "did not find marker",
			decode:  func(r Reader) error { _, err := DecodeBytes(r, nil); return err },
		},
		{
			name:    "Bytes, no terminator",
			buf:     []byte{bytesMarker, 'a'},
			desc:    false,
			pattern: "did not find terminator",
			decode:  func(r Reader) error { _, err := DecodeBytes(r, nil); return err },
		},
		{
			name:    "Bytes, malformed escape",
			buf:     []byte{bytesMarker, 'a', 0x00},
			desc:    false,
			pattern: "malformed escape",
			decode:  func(r Reader) error { _, err := DecodeBytes(r, nil); return err },
		},
		{
			name:    "Bytes, invalid escape 1",
			buf:     []byte{bytesMarker, 'a', 0x00, 0x00},
			desc:    false,
			pattern: "unknown escape",
			decode:  func(r Reader) error { _, err := DecodeBytes(r, nil); return err },
		},
		{
			name:    "Bytes, invalid escape 2",
			buf:     []byte{bytesMarker, 'a', 0x00, 0x02},
			desc:    false,
			pattern: "unknown escape",
			decode:  func(r Reader) error { _, err := DecodeBytes(r, nil); return err },
		},
		{
			name:    "BytesDecreasing, no marker",
			buf:     []byte{'a'},
			desc:    false,
			pattern: "did not find marker",
			decode:  func(r Reader) error { _, err := DecodeBytes(r, nil); return err },
		},
		{
			name:    "BytesDecreasing, no terminator",
			buf:     []byte{^bytesMarker, ^byte('a')},
			desc:    true,
			pattern: "did not find terminator",
			decode:  func(r Reader) error { _, err := DecodeBytesDecreasing(r, nil); return err },
		},
		{
			name:    "BytesDecreasing, malformed escape",
			buf:     []byte{^bytesMarker, ^byte('a'), 0xff},
			desc:    true,
			pattern: "malformed escape",
			decode:  func(r Reader) error { _, err := DecodeBytesDecreasing(r, nil); return err },
		},
		{
			name:    "BytesDecreasing, invalid escape 1",
			buf:     []byte{^bytesMarker, ^byte('a'), 0xff, 0xff},
			desc:    true,
			pattern: "unknown escape",
			decode:  func(r Reader) error { _, err := DecodeBytesDecreasing(r, nil); return err },
		},
		{
			name:    "BytesDecreasing, invalid escape 2",
			buf:     []byte{^bytesMarker, ^byte('a'), 0xff, 0xfd},
			desc:    true,
			pattern: "unknown escape",
			decode:  func(r Reader) error { _, err := DecodeBytesDecreasing(r, nil); return err },
		},
	}
	for _, test := range tests {
		var dirs []Direction
		if test.desc {
			dirs = []Direction{Descending}
		} else {
			dirs = []Direction{Ascending}
		}
		r := NewKeyReader(test.buf, dirs)
		err := test.decode(r)
		if !regexp.MustCompile(test.pattern).MatchString(err.Error()) {
			t.Errorf("%q, pattern %q doesn't match %q", test.name, test.pattern, err)
		}
	}
}

func TestEncodeDecodeUvarintDecreasing(t *testing.T) {
	testBasicEncodeDecodeUint64(EncodeUvarintDecreasing, DecodeUvarint, true, t)
	nums := []uint64{0, 1, 1 << 8, math.MaxUint64 - 1, math.MaxUint64}
	var testCases []testCaseUint64
	for _, n := range nums {
		enc := EncodeUvarint(nil, n)
		onesComplement(enc)
		testCases = append(testCases, testCaseUint64{n, enc})
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
		dirs := []Direction{Ascending}
		r := NewKeyReader(enc, dirs)
		dec, err := DecodeBytes(r, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if !bytes.Equal(c.value, dec) {
			t.Errorf("unexpected decoding mismatch for %v. got %v", c.value, dec)
		}
		if !r.EOF() {
			t.Errorf("unexpected remaining bytes: %v", r.RawBytesRemaining())
		}

		// Test again with a remainder. The reader will be reset.
		enc = EncodeBytes(nil, c.value)
		enc = append(enc, []byte("remainder")...)
		r = NewKeyReader(enc, dirs)
		_, err = DecodeBytes(r, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if string(r.RawBytesRemaining()) != "remainder" {
			t.Errorf("unexpected remaining bytes: %v", r.RawBytesRemaining())
		}
	}
}

func TestEncodeDecodeBytesDecreasing(t *testing.T) {
	vals := [][]byte{
		[]byte("hello"),
		[]byte{'b', 0xff},
		[]byte{'b', 0, 0, 'a'},
		[]byte{'b', 0, 0},
		[]byte{'b', 0},
		[]byte{'b'},
		[]byte{'a'},
		[]byte{0, 0xff, 'a'},
		[]byte{0, 'a'},
		[]byte{0, 1, 'a'}}
	type testCaseBytes struct {
		value   []byte
		encoded []byte
	}
	var testCases []testCaseBytes
	for _, v := range vals {
		enc := EncodeBytes(nil, v)
		onesComplement(enc)
		testCases = append(testCases, testCaseBytes{v, enc})
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
		dirs := []Direction{Descending}
		r := NewKeyReader(enc, dirs)
		dec, err := DecodeBytesDecreasing(r, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if !bytes.Equal(c.value, dec) {
			t.Errorf("unexpected decoding mismatch for %v. got %v", c.value, dec)
		}
		if !r.EOF() {
			t.Errorf("unexpected remaining bytes: %v", r.RawBytesRemaining())
		}

		// Test again with a remainder. The reader will be reset.
		enc = append(enc, []byte("remainder")...)
		r = NewKeyReader(enc, dirs)
		_, err = DecodeBytesDecreasing(r, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if string(r.RawBytesRemaining()) != "remainder" {
			t.Errorf("unexpected remaining bytes: %v", r.RawBytesRemaining())
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
		// !!! add test for empty string
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
		dirs := []Direction{Ascending}
		r := NewKeyReader(enc, dirs)
		dec, err := DecodeString(r, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if c.value != dec {
			t.Errorf("unexpected decoding mismatch for %v. got %v", c.value, dec)
		}
		if !r.EOF() {
			t.Errorf("unexpected remaining bytes: %v", r.RawBytesRemaining())
		}

		// Test again with a remainder. The reader will be reset.
		enc = EncodeString(nil, c.value)
		enc = append(enc, "remainder"...)
		r = NewKeyReader(enc, dirs)
		_, err = DecodeString(r, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if string(r.RawBytesRemaining()) != "remainder" {
			t.Errorf("unexpected remaining bytes: %v", r.RawBytesRemaining())
		}
	}
}

func TestEncodeDecodeStringDecreasing(t *testing.T) {
	vals := []string{"hello", "b\xff", "b\x00\x00a", "b\x00\x00", "b\x00", "b",
		"a", "\x00\xffa", "\x00a", "\x00\x01a"}
	type stringTestCase struct {
		value   string
		encoded []byte
	}
	var testCases []stringTestCase
	for _, s := range vals {
		enc := EncodeString(nil, s)
		onesComplement(enc)
		testCases = append(testCases, stringTestCase{s, enc})
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
		dirs := []Direction{Descending}
		r := NewKeyReader(enc, dirs)
		dec, err := DecodeStringDecreasing(r, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if c.value != dec {
			t.Errorf("unexpected decoding mismatch for %v. got %v", c.value, dec)
		}
		if !r.EOF() {
			t.Errorf("unexpected remaining bytes: %v", r.RawBytesRemaining())
		}

		// Test again with a remainder. The reader will be reset.
		enc = EncodeStringDecreasing(nil, c.value)
		enc = append(enc, "remainder"...)
		r = NewKeyReader(enc, dirs)
		_, err = DecodeStringDecreasing(r, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if string(r.RawBytesRemaining()) != "remainder" {
			t.Errorf("unexpected remaining bytes: %v", r.RawBytesRemaining())
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

	dirs := []Direction{Ascending}
	r := NewKeyReader([]byte(hello), dirs)
	if isNull, err := DecodeIfNull(r); isNull || err != nil {
		t.Fatalf("expected isNull=false, but found isNull=%v", isNull)
	} else if hello != string(r.RawBytesRemaining()) {
		t.Fatalf("expected %q, but found %q", hello, r.RawBytesRemaining())
	}

	r = NewKeyReader([]byte("\x00"+hello), dirs)
	isNull, err := DecodeIfNull(r)
	if err != nil {
		t.Fatalf("Error decoding null: ", err)
	}
	if !isNull {
		t.Fatalf("expected isNull=true, but found isNull=%v", isNull)
	} else if hello != string(r.RawBytesRemaining()) {
		t.Fatalf("expected %q, but found %q", hello, r.RawBytesRemaining())
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
		var enc []byte
		if !last.IsZero() {
			enc = EncodeTime(enc, current)
			dirs := []Direction{Ascending}
			r := NewKeyReader(enc, dirs)
			decodedCurrent, err := DecodeTime(r)
			if err != nil {
				t.Error(err)
				continue
			}
			if !decodedCurrent.Equal(current) {
				t.Fatalf("lossy transport: before (%v) vs after (%v)", current, decodedCurrent)
			}
			if bytes.Compare(lastEncoded, enc) >= 0 {
				t.Fatalf("encodings %s, %s not increasing", testCases[i-1], testCases[i])
			}
		}
		last = current
		lastEncoded = enc
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
		// PeekType is only used with all-ascending keys, so we use a BufferReader.
		r := NewBufferReader(c.enc)
		typ := PeekType(r)
		if c.typ != typ {
			t.Fatalf("%d: expected %d, but found %d", i, c.typ, typ)
		}
	}
}

//func BenchmarkEncodeUint32(b *testing.B) {
//  rng, _ := randutil.NewPseudoRand()

//  vals := make([]uint32, 10000)
//  for i := range vals {
//    vals[i] = uint32(rng.Int31())
//  }

//  buf := make([]byte, 0, 16)

//  b.ResetTimer()
//  for i := 0; i < b.N; i++ {
//    _ = EncodeUint32(buf, vals[i%len(vals)])
//  }
//}

func TestDecodeUint32XXX(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10)
	nums := make([]uint32, 10)
	for i := range vals {
		nums[i] = uint32(rng.Int31())
		vals[i] = EncodeUint32(nil, nums[i])
	}

	r := NewRReader([]byte{})
	for i := 0; i < len(vals); i++ {
		//r := NewRReader(vals[i%len(vals)])
		r.i = 0
		r.s = vals[i]
		v, _ := DecodeUint32XXX(r)
		fmt.Printf("decoded: %d vs value: %d\n", v, nums[i])
	}
}

var result uint32

func BenchmarkDecodeUint32XXX(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	nums := make([]uint32, 10000)
	for i := range vals {
		nums[i] = uint32(rng.Int31())
		vals[i] = EncodeUint32(nil, nums[i])
	}

	r := NewRReader([]byte{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Reset(vals[i%len(vals)])
		//NewRReader(vals[i%len(vals)])
		//r := NewRReader(vals[i%len(vals)])
		//r.i = 0
		//r.s = vals[i%len(vals)]
		//r.dir = Ascending
		result, _ = DecodeUint32XXX(r)
		//if result != nums[i%len(vals)] {
		//  panic("not equal!!!")
		//}
	}
}

// !!!
func Dummy() error {
	return util.Errorf("insufficient bytes to decode uint32 int value")
}

// DecodeVarint decodes a varint encoded int64 from the input
// key.
func DecodeVarintXXX(r *RReader) (int64, error) {
	var b byte
	var length int
	var err error
	if b, err = r.PeekByte(); err != nil {
		return 0, err
	}
	length = int(b) - intZero
	if length < 0 {
		length = -length
		r.ReadByte()
		var remB *UnsafeArray
		remB, err = r.Read(length)
		if err == io.EOF {
			return 0, util.Errorf("insufficient bytes to decode var uint64 int value: %s", remB)
		}
		var v int64
		// Use the ones-complement of each encoded byte in order to build
		// up a positive number, then take the ones-complement again to
		// arrive at our negative value.
		for i := 0; i < length; i++ {
			t := remB[i]
			v = (v << 8) | int64(^t)
		}
		return ^v, nil
	}

	//r.i-- // !!!
	v, err := DecodeUvarintXXX(r)
	if err != nil {
		return 0, err
	}
	if v > math.MaxInt64 {
		return 0, util.Errorf("varint %d overflows int64", v)
	}
	return int64(v), nil
}

// !!!
func DecodeUvarintYYY(r *RReader) (uint64, error) {
	//buf, _ := r.ReadAtMost(9) // we purposefully ignore the possible EOF here !!!
	_, v, err := DecodeUvarintClassic(r.RawBytesRemaining())
	// !!! here need to rewind - put the remainder back. Careful that it might need to be inverted again.
	return v, err
}

// !!! the old version of DecodeUvarint before the Reader
// DecodeUvarint decodes a varint encoded uint64 from the input
// buffer. The remainder of the input buffer and the decoded uint64
// are returned.
func DecodeUvarintClassic(b []byte) ([]byte, uint64, error) {
	if len(b) == 0 {
		return nil, 0, util.Errorf("insufficient bytes to decode var uint64 int value")
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
		return nil, 0, util.Errorf("insufficient bytes to decode var uint64 int value: %v", b)
	}
	var v uint64
	// It is faster to range over the elements in a slice than to index
	// into the slice on each loop iteration.
	for _, t := range b[:length] {
		v = (v << 8) | uint64(t)
	}
	return b, v, nil
}

func DecodeUvarintXXX(r *RReader) (uint64, error) {
	var b byte
	var length int
	var err error
	if b, err = r.ReadByte(); err != nil {
		return 0, err
	}
	length = int(b) - intZero
	if length <= intSmall {
		return uint64(length), nil
	}
	length -= intSmall
	var buf *UnsafeArray
	if length < 0 || length > 8 {
		return 0, util.Errorf("invalid uvarint length of %d", length)
	} else {
		buf, err = r.Read(length)
		if err == io.EOF {
			return 0, util.Errorf("insufficient bytes to decode var uint64 int value: %v", buf)
		}
	}
	var v uint64
	//fmt.Printf("!!! read len: %d bytes: %v\n", length, buf[:length])
	//if length >= 4 {
	//  bw := (*[1000]int32)(unsafe.Pointer(buf))
	//  fmt.Printf("!!! first bytes: %v %v\n", bw[0], bw[1])
	//  v = v<<32 | uint64(bw[0])
	//  buf = (*UnsafeArray)(unsafe.Pointer(&buf[4]))
	//  length -= 4
	//}
	// It is faster to range over the elements in a slice than to index
	// into the slice on each loop iteration.
	for _, t := range buf[:length] {
		v = (v << 8) | uint64(t)
	}
	//for i := 0; i < length; i++ {
	//  t := buf[i]
	//  v = (v << 8) | uint64(t)
	//}
	return v, nil
}

//func DecodeUint32FixXXX(r *RReader) (uint32, error) {
//  //var b []byte
//  var b *UnsafeArray
//  var err error
//  //b = r.RawBytesRemaining()[:4]
//  if b, err = r.Read(4); err != nil {
//    return 0, util.Errorf("insufficient bytes to decode uint32 int value")
//  }
//  v := (uint32(b[0]) << 24) | (uint32(b[1]) << 16) |
//    (uint32(b[2]) << 8) | uint32(b[3])
//  return v, nil
//}

// DecodeUint32 decodes a uint32 from the input buffer, treating
// the input as a big-endian 4 byte uint32 representation.
func DecodeUint32XXX(r *RReader) (uint32, error) {
	//var b *UnsafeArray
	//var err error
	//if b, err = r.Read(4); err != nil {
	//  return 0, util.Errorf("insufficient bytes to decode uint32 int value")
	//}
	//v := (uint32(b[0]) << 24) | (uint32(b[1]) << 16) |
	//  (uint32(b[2]) << 8) | uint32(b[3])
	c := r.RawBytesRemaining()
	v := (uint32(c[0]) << 24) | (uint32(c[1]) << 16) |
		(uint32(c[2]) << 8) | uint32(c[3])
	return v, nil
}

//func BenchmarkEncodeUint64(b *testing.B) {
//  rng, _ := randutil.NewPseudoRand()

//  vals := make([]uint64, 10000)
//  for i := range vals {
//    vals[i] = uint64(rng.Int63())
//  }

//  buf := make([]byte, 0, 16)

//  b.ResetTimer()
//  for i := 0; i < b.N; i++ {
//    _ = EncodeUint64(buf, vals[i%len(vals)])
//  }
//}

func BenchmarkDecodeUint64(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUint64(nil, uint64(rng.Int63()))
	}

	dirs := []Direction{Ascending}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := NewKeyReader(vals[i%len(vals)], dirs)
		_, _ = DecodeUint64(r)
	}
}

// !!!
//func BenchmarkEncodeVarint(b *testing.B) {
//  rng, _ := randutil.NewPseudoRand()

//  vals := make([]int64, 10000)
//  for i := range vals {
//    vals[i] = rng.Int63()
//  }

//  buf := make([]byte, 0, 16)

//  b.ResetTimer()
//  for i := 0; i < b.N; i++ {
//    _ = EncodeVarint(buf, vals[i%len(vals)])
//  }
//}

func BenchmarkDecodeVarintXXX(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeVarint(nil, rng.Int63())
	}

	r := NewRReader([]byte{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Reset(vals[i%len(vals)])
		//r := NewRReader(vals[i%len(vals)])
		//r.i = 0
		//r.s = vals[i%len(vals)]
		//r.slice = r.slice[:0]
		_, _ = DecodeVarintXXX(r)
	}
}

func BenchmarkDecodeVarintDecreasingXXX(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	//vals := make([][]byte, 10000)
	vals := make([][]byte, b.N)
	for i := range vals {
		vals[i] = EncodeVarintDecreasing(nil, rng.Int63())
	}

	r := NewRReader([]byte{})
	r.dir = Descending
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// r := NewRReader(vals[i%len(vals)]) // !!! Don't reallocate the reader?
		r.i = 0
		r.dir = Descending
		r.s = vals[i%len(vals)]
		_, _ = DecodeVarintXXX(r)
	}
}

// !!!
//func BenchmarkEncodeUvarint(b *testing.B) {
//  rng, _ := randutil.NewPseudoRand()
//  vals := make([]uint64, 10000)
//  for i := range vals {
//    vals[i] = uint64(rng.Int63())
//  }

//  buf := make([]byte, 0, 16)

//  b.ResetTimer()
//  for i := 0; i < b.N; i++ {
//    _ = EncodeUvarint(buf, vals[i%len(vals)])
//  }
//}

func BenchmarkDecodeUvarint(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUvarint(nil, uint64(rng.Int63()))
	}

	dirs := []Direction{Ascending}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := NewKeyReader(vals[i%len(vals)], dirs)
		_, _ = DecodeUvarint(r)
	}
}

func BenchmarkDecodeUvarintXXX(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUvarint(nil, uint64(rng.Int63()))
	}

	//r := NewRReader([]byte{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := NewRReader(vals[i%len(vals)])
		//r.Reset(vals[i%len(vals)])
		_, _ = DecodeUvarintXXX(r)
	}
}

//func BenchmarkDecodeUvarintDecreasingXXX(b *testing.B) {
//  rng, _ := randutil.NewPseudoRand()

//  vals := make([][]byte, 10000)
//  for i := range vals {
//    vals[i] = EncodeUvarintDecreasing(nil, uint64(rng.Int63()))
//  }

//  b.ResetTimer()
//  for i := 0; i < b.N; i++ {
//    r := NewRReader(vals[i%len(vals)])
//    r.dir = Descending
//    _, _ = DecodeUvarintXXX(r)
//  }
//}

//func BenchmarkEncodeBytes(b *testing.B) {
//  rng, _ := randutil.NewPseudoRand()

//  vals := make([][]byte, 10000)
//  for i := range vals {
//    vals[i] = randutil.RandBytes(rng, 100)
//  }

//  buf := make([]byte, 0, 1000)

//  b.ResetTimer()
//  for i := 0; i < b.N; i++ {
//    _ = EncodeBytes(buf, vals[i%len(vals)])
//  }
//}

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
		r := NewKeyReader(vals[i%len(vals)], []Direction{Ascending})
		_, _ = DecodeBytes(r, buf)
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
		r := NewKeyReader(vals[i%len(vals)], []Direction{Descending})
		_, _ = DecodeBytesDecreasing(r, buf)
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
		r := NewKeyReader(vals[i%len(vals)], []Direction{Ascending})
		_, _ = DecodeString(r, buf)
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
		r := NewKeyReader(vals[i%len(vals)], []Direction{Descending})
		_, _ = DecodeStringDecreasing(r, buf)
	}
}
