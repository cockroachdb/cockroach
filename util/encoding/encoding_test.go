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
	"math"
	"reflect"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/util"
)

func TestEncoding(t *testing.T) {
	k := []byte("asd")
	n := int64(3142595)
	encoded, err := Encode(k, int64(n))
	if err != nil {
		t.Errorf("encoding error for %d", n)
	}
	decoded, err := Decode(k, encoded)
	if err != nil {
		t.Errorf("decoding error for %d", n)
	}
	switch v := decoded.(type) {
	case int64:
		if v != n {
			t.Errorf("int64 decoding error, got the wrong result")
		}
	default:
		t.Errorf("int64 decoding error, did not get a uint64 back but instead type %v: %v", reflect.TypeOf(v), v)
	}
}

func TestChecksums(t *testing.T) {
	testKey := []byte("whatever")
	doubleWrap := func(a []byte) bool {
		_, err := unwrapChecksum(testKey, wrapChecksum(testKey, a))
		return err == nil
	}

	_, err := unwrapChecksum([]byte("does not matter"), []byte("srt"))
	if err == nil {
		t.Fatalf("expected error unwrapping a too short string")
	}

	testCases := [][]byte{
		[]byte(""),
		[]byte("Hello"),
		[]byte("	tab "),
		func() []byte {
			b := make([]byte, math.MaxUint8, math.MaxUint8)
			for i := 0; i < math.MaxUint8; i++ {
				b[i] = 'z'
			}
			return b
		}(),
		[]byte("لاحول ولا قوة الا بالله"),
	}

	for i, c := range testCases {
		if !doubleWrap(c) {
			t.Errorf("unexpected integrity error for '%v'", c)
		}
		// Glue an extra byte to a copy that kills the checksum.
		distorted := append(wrapChecksum(testKey, append([]byte(nil), c...)), '1')
		if _, err := unwrapChecksum(testKey, distorted); err == nil {
			t.Errorf("%d: unexpected integrity match for corrupt value", i)
		}

	}
}

func TestWillOverflow(t *testing.T) {
	testCases := []struct {
		a, b     int64
		overflow bool // will a+b over- or underflow?
	}{
		{0, 0, false},
		{math.MaxInt64, 0, false},
		{math.MaxInt64, 1, true},
		{math.MaxInt64, math.MinInt64, false},
		{math.MinInt64, 0, false},
		{math.MinInt64, -1, true},
		{math.MinInt64, math.MinInt64, true},
	}

	for i, c := range testCases {
		if WillOverflow(c.a, c.b) != c.overflow ||
			WillOverflow(c.b, c.a) != c.overflow {
			t.Errorf("%d: overflow recognition error", i)
		}
	}
}

func testBasicEncodeDecode32(encFunc func([]byte, uint32) []byte,
	dec func([]byte) ([]byte, uint32), decreasing bool, t *testing.T) {
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
		b, decode := dec(enc)
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
	dec func([]byte) ([]byte, uint64), decreasing bool, t *testing.T) {
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
		b, decode := dec(enc)
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
	dec func([]byte) ([]byte, int64), decreasing bool, t *testing.T) {
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
		b, decode := dec(enc)
		if len(b) != 0 {
			t.Errorf("leftover bytes: [% x]", b)
		}
		if decode != v {
			t.Errorf("decode yielded different value than input: %d vs. %d", decode, v)
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
		{math.MinInt64, []byte{0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{math.MinInt64 + 1, []byte{0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{-1 << 8, []byte{0x06, 0xff, 0x00}},
		{-1, []byte{0x07, 0xff}},
		{0, []byte{0x08}},
		{1, []byte{0x09, 0x01}},
		{1 << 8, []byte{0x0a, 0x01, 0x00}},
		{math.MaxInt64, []byte{0x10, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}
	testCustomEncodeInt64(testCases, EncodeVarint, t)
}

func TestEncodeDecodeVarintDecreasing(t *testing.T) {
	testBasicEncodeDecodeInt64(EncodeVarintDecreasing, DecodeVarintDecreasing, true, t)
	testCases := []testCaseInt64{
		{math.MinInt64, []byte{0x10, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{math.MinInt64 + 1, []byte{0x10, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{-1 << 8, []byte{0x09, 0xff}},
		{-1, []byte{0x08}},
		{0, []byte{0x07, 0xff}},
		{1, []byte{0x07, 0xfe}},
		{1 << 8, []byte{0x06, 0xfe, 0xff}},
		{math.MaxInt64, []byte{0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}
	testCustomEncodeInt64(testCases, EncodeVarintDecreasing, t)
}

func TestEncodeDecodeUvarint(t *testing.T) {
	testBasicEncodeDecodeUint64(EncodeUvarint, DecodeUvarint, false, t)
	testCases := []testCaseUint64{
		{0, []byte{0x08}},
		{1, []byte{0x09, 0x01}},
		{1 << 8, []byte{0x0a, 0x01, 0x00}},
		{math.MaxUint64, []byte{0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}
	testCustomEncodeUint64(testCases, EncodeUvarint, t)
}

// TestDecodeInvalid tests that decoding invalid bytes panics.
func TestDecodeInvalid(t *testing.T) {
	tests := []struct {
		name    string       // name printed with errors.
		buf     []byte       // buf contains an invalid uvarint to decode.
		pattern string       // pattern matches the panic string.
		decode  func([]byte) // decode is called with buf.
	}{
		{
			name:    "DecodeUvarint, length of 9 bytes",
			buf:     []byte{0x11, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01},
			pattern: "invalid uvarint length of [0-9]+",
			decode:  func(b []byte) { DecodeUvarint(b) },
		},
		{
			name:    "DecodeVarint, overflows int64",
			buf:     []byte{0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			pattern: "varint [0-9]+ overflows int64",
			decode:  func(b []byte) { DecodeVarint(b) },
		},
		{
			name:    "Bytes, no terminator",
			buf:     []byte{'a'},
			pattern: "did not find terminator",
			decode:  func(b []byte) { DecodeBytes(b, nil) },
		},
		{
			name:    "Bytes, malformed escape",
			buf:     []byte{'a', 0x00},
			pattern: "malformed escape",
			decode:  func(b []byte) { DecodeBytes(b, nil) },
		},
		{
			name:    "Bytes, invalid escape 1",
			buf:     []byte{'a', 0x00, 0x00},
			pattern: "unknown escape",
			decode:  func(b []byte) { DecodeBytes(b, nil) },
		},
		{
			name:    "Bytes, invalid escape 2",
			buf:     []byte{'a', 0x00, 0x02},
			pattern: "unknown escape",
			decode:  func(b []byte) { DecodeBytes(b, nil) },
		},
		{
			name:    "BytesDecreasing, no terminator",
			buf:     []byte{^byte('a')},
			pattern: "did not find terminator",
			decode:  func(b []byte) { DecodeBytesDecreasing(b, nil) },
		},
		{
			name:    "BytesDecreasing, malformed escape",
			buf:     []byte{^byte('a'), 0xff},
			pattern: "malformed escape",
			decode:  func(b []byte) { DecodeBytesDecreasing(b, nil) },
		},
		{
			name:    "BytesDecreasing, invalid escape 1",
			buf:     []byte{^byte('a'), 0xff, 0xff},
			pattern: "unknown escape",
			decode:  func(b []byte) { DecodeBytesDecreasing(b, nil) },
		},
		{
			name:    "BytesDecreasing, invalid escape 2",
			buf:     []byte{^byte('a'), 0xff, 0xfd},
			pattern: "unknown escape",
			decode:  func(b []byte) { DecodeBytesDecreasing(b, nil) },
		},
	}
	for _, test := range tests {
		func() {
			defer func() {
				r := recover()
				if r == nil {
					t.Errorf("%q, expected panic", test.name)
					return
				}
				str := fmt.Sprint(r)
				match, err := regexp.MatchString(test.pattern, str)
				if err != nil {
					t.Errorf("%q, couldn't match regexp: %v", test.name, err)
					return
				}
				if !match {
					t.Errorf("%q, pattern %q doesn't match %q", test.name, test.pattern, str)
				}
			}()
			test.decode(test.buf)
		}()
	}
}

func TestEncodeDecodeUvarintDecreasing(t *testing.T) {
	testBasicEncodeDecodeUint64(EncodeUvarintDecreasing, DecodeUvarintDecreasing, true, t)
	testCases := []testCaseUint64{
		{0, []byte{0x08}},
		{1, []byte{0x07, 0xfe}},
		{1 << 8, []byte{0x06, 0xfe, 0xff}},
		{math.MaxUint64 - 1, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{math.MaxUint64, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}
	testCustomEncodeUint64(testCases, EncodeUvarintDecreasing, t)
}

func TestEncodeDecodeBytes(t *testing.T) {
	testCases := []struct {
		value   []byte
		encoded []byte
	}{
		{[]byte{0, 1, 'a'}, []byte{0x00, 0xff, 1, 'a', 0x00, 0x01}},
		{[]byte{0, 'a'}, []byte{0x00, 0xff, 'a', 0x00, 0x01}},
		{[]byte{0, 0xff, 'a'}, []byte{0x00, 0xff, 0xff, 'a', 0x00, 0x01}},
		{[]byte{'a'}, []byte{'a', 0x00, 0x01}},
		{[]byte{'b'}, []byte{'b', 0x00, 0x01}},
		{[]byte{'b', 0}, []byte{'b', 0x00, 0xff, 0x00, 0x01}},
		{[]byte{'b', 0, 0}, []byte{'b', 0x00, 0xff, 0x00, 0xff, 0x00, 0x01}},
		{[]byte{'b', 0, 0, 'a'}, []byte{'b', 0x00, 0xff, 0x00, 0xff, 'a', 0x00, 0x01}},
		{[]byte{'b', 0xff}, []byte{'b', 0xff, 0x00, 0x01}},
		{[]byte("hello"), []byte{'h', 'e', 'l', 'l', 'o', 0x00, 0x01}},
		{[]byte{0xff, 0, 'a'}, []byte{0xff, 0x00, 0x00, 0xff, 'a', 0x00, 0x01}},
		{[]byte{0xff, 'b'}, []byte{0xff, 0x00, 'b', 0x00, 0x01}},
		{[]byte{0xff, 0xff, 'b'}, []byte{0xff, 0x00, 0xff, 'b', 0x00, 0x01}},
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
		remainder, dec := DecodeBytes(enc, nil)
		if !bytes.Equal(c.value, dec) {
			t.Errorf("unexpected decoding mismatch for %v. got %v", c.value, dec)
		}
		if len(remainder) != 0 {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}

		enc = append(enc, []byte("remainder")...)
		remainder, dec = DecodeBytes(enc, nil)
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
		{[]byte{0xff, 0xff, 'b'}, []byte{0x00, 0xff, 0x00, ^byte('b'), 0xff, 0xfe}},
		{[]byte{0xff, 'b'}, []byte{0x00, 0xff, ^byte('b'), 0xff, 0xfe}},
		{[]byte{0xff, 0, 'a'}, []byte{0x00, 0xff, 0xff, 0x00, ^byte('a'), 0xff, 0xfe}},
		{[]byte("hello"), []byte{^byte('h'), ^byte('e'), ^byte('l'), ^byte('l'), ^byte('o'), 0xff, 0xfe}},
		{[]byte{'b', 0xff}, []byte{^byte('b'), 0x00, 0xff, 0xfe}},
		{[]byte{'b', 0, 0, 'a'}, []byte{^byte('b'), 0xff, 0x00, 0xff, 0x00, ^byte('a'), 0xff, 0xfe}},
		{[]byte{'b', 0, 0}, []byte{^byte('b'), 0xff, 0x00, 0xff, 0x00, 0xff, 0xfe}},
		{[]byte{'b', 0}, []byte{^byte('b'), 0xff, 0x00, 0xff, 0xfe}},
		{[]byte{'b'}, []byte{^byte('b'), 0xff, 0xfe}},
		{[]byte{'a'}, []byte{^byte('a'), 0xff, 0xfe}},
		{[]byte{0, 0xff, 'a'}, []byte{0xff, 0x00, 0x00, ^byte('a'), 0xff, 0xfe}},
		{[]byte{0, 'a'}, []byte{0xff, 0x00, ^byte('a'), 0xff, 0xfe}},
		{[]byte{0, 1, 'a'}, []byte{0xff, 0x00, 0xfe, ^byte('a'), 0xff, 0xfe}},
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
		remainder, dec := DecodeBytesDecreasing(enc, nil)
		if !bytes.Equal(c.value, dec) {
			t.Errorf("unexpected decoding mismatch for %v. got %v", c.value, dec)
		}
		if len(remainder) != 0 {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}

		enc = append(enc, []byte("remainder")...)
		remainder, dec = DecodeBytesDecreasing(enc, nil)
		if string(remainder) != "remainder" {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}
	}
}

func TestEncodeDecodeKey(t *testing.T) {
	testCases := []struct {
		format   string
		args     []interface{}
		results  []interface{}
		expected []byte
	}{
		{"%d",
			[]interface{}{int64(1)},
			[]interface{}{new(int64)},
			EncodeVarint(nil, 1)},
		{"%-d",
			[]interface{}{int64(2)},
			[]interface{}{new(int64)},
			EncodeVarintDecreasing(nil, 2)},
		{"%u",
			[]interface{}{uint64(3)},
			[]interface{}{new(uint64)},
			EncodeUvarint(nil, 3)},
		{"%-u",
			[]interface{}{uint64(4)},
			[]interface{}{new(uint64)},
			EncodeUvarintDecreasing(nil, 4)},
		{"%32u",
			[]interface{}{uint32(5)},
			[]interface{}{new(uint32)},
			EncodeUint32(nil, 5)},
		{"%-32u",
			[]interface{}{uint32(6)},
			[]interface{}{new(uint32)},
			EncodeUint32Decreasing(nil, 6)},
		{"%64u",
			[]interface{}{uint64(7)},
			[]interface{}{new(uint64)},
			EncodeUint64(nil, 7)},
		{"%-64u",
			[]interface{}{uint64(8)},
			[]interface{}{new(uint64)},
			EncodeUint64Decreasing(nil, 8)},
		{"%s",
			[]interface{}{[]byte("9")},
			[]interface{}{new([]byte)},
			EncodeBytes(nil, []byte("9"))},
		{"%s",
			[]interface{}{"10"},
			[]interface{}{new(string)},
			EncodeBytes(nil, []byte("10"))},
		{"%-s",
			[]interface{}{[]byte("11")},
			[]interface{}{new([]byte)},
			EncodeBytesDecreasing(nil, []byte("11"))},
		{"%-s",
			[]interface{}{"12"},
			[]interface{}{new(string)},
			EncodeBytesDecreasing(nil, []byte("12"))},
		{"hello%d",
			[]interface{}{int64(13)},
			[]interface{}{new(int64)},
			EncodeVarint([]byte("hello"), 13)},
		{"%d%-d",
			[]interface{}{int64(14), int64(15)},
			[]interface{}{new(int64), new(int64)},
			EncodeVarintDecreasing(EncodeVarint(nil, 14), 15)},
		{"%s%-s%s",
			[]interface{}{"a", "b", "c"},
			[]interface{}{new(string), new(string), new(string)},
			EncodeBytes(EncodeBytesDecreasing(EncodeBytes(nil, []byte("a")), []byte("b")), []byte("c"))},
	}
	for i, c := range testCases {
		enc := EncodeKey(nil, c.format, c.args...)
		if !bytes.Equal(c.expected, enc) {
			t.Errorf("expected [% x]; got [% x]", c.expected, enc)
		}

		dec := DecodeKey(enc, c.format, c.results...)
		if len(dec) != 0 {
			t.Errorf("expected complete decoding, %d remaining", len(dec))
		}

		var dresults []interface{}
		for _, r := range c.results {
			dresults = append(dresults, reflect.ValueOf(r).Elem().Interface())
		}
		if !reflect.DeepEqual(c.args, dresults) {
			t.Errorf("%d: expected %v; got %v", i, c.args, dresults)
		}
	}
}

func TestEncodeDecodeKeyInvalidFormat(t *testing.T) {
	testCases := []struct {
		format   string
		expected string
		args     []interface{}
	}{
		{"%", "no verb: ", nil},
		{"%v", "unknown format verb", nil},
		{"%33d", "invalid width specifier; use 32 or 64", nil},
		{"%da", "invalid format string:", []interface{}{int64(1)}},
	}
	for _, c := range testCases {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic")
				} else if ok, err := regexp.MatchString(c.expected, r.(string)); err != nil {
					t.Error(err)
				} else if !ok {
					t.Errorf("expected match \"%s\", but got %s", c.expected, r)
				}
			}()
			EncodeKey(nil, c.format, c.args...)
		}()
	}
}

func TestEncodeDecodeKeyInvalidArgs(t *testing.T) {
	testCases := []struct {
		format string
		arg    interface{}
		result interface{}
	}{
		{"%d", int32(1), new(int32)},
		{"%u", int32(1), new(int32)},
		{"%s", int32(1), new(int32)},
	}

	for _, c := range testCases {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("expected panic")
				}
			}()
			EncodeKey(nil, c.format, c.arg)
		}()
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic")
				}
			}()
			DecodeKey(nil, c.format, c.result)
		}()
	}
}

func TestDecodeKeyFormatMismatch(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic")
		}
	}()
	DecodeKey([]byte("world"), "hello")
}

func BenchmarkEncodeUint32(b *testing.B) {
	rng, _ := util.NewPseudoRand()

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
	rng, _ := util.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUint32(nil, uint32(rng.Int31()))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeUint32(vals[i%len(vals)])
	}
}

func BenchmarkEncodeUint64(b *testing.B) {
	rng, _ := util.NewPseudoRand()

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
	rng, _ := util.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUint64(nil, uint64(rng.Int63()))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeUint64(vals[i%len(vals)])
	}
}

func BenchmarkEncodeVarint(b *testing.B) {
	rng, _ := util.NewPseudoRand()

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
	rng, _ := util.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeVarint(nil, rng.Int63())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeVarint(vals[i%len(vals)])
	}
}

func BenchmarkEncodeUvarint(b *testing.B) {
	rng, _ := util.NewPseudoRand()

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
	rng, _ := util.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUvarint(nil, uint64(rng.Int63()))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeUvarint(vals[i%len(vals)])
	}
}

func BenchmarkEncodeBytes(b *testing.B) {
	rng, _ := util.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = util.RandBytes(rng, 100)
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeBytes(buf, vals[i%len(vals)])
	}
}

func BenchmarkEncodeBytesDecreasing(b *testing.B) {
	rng, _ := util.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = util.RandBytes(rng, 100)
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeBytesDecreasing(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeBytes(b *testing.B) {
	rng, _ := util.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeBytes(nil, util.RandBytes(rng, 100))
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeBytes(vals[i%len(vals)], buf)
	}
}

func BenchmarkDecodeBytesDecreasing(b *testing.B) {
	rng, _ := util.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeBytesDecreasing(nil, util.RandBytes(rng, 100))
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeBytesDecreasing(vals[i%len(vals)], buf)
	}
}

func BenchmarkEncodeKeyUint32(b *testing.B) {
	rng, _ := util.NewPseudoRand()

	vals := make([]interface{}, 10000)
	for i := range vals {
		vals[i] = uint32(rng.Int31())
	}

	buf := make([]byte, 0, 16)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeKey(buf, "%32u", vals[i%len(vals)])
	}
}

func BenchmarkDecodeKeyUint32(b *testing.B) {
	rng, _ := util.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUint32(nil, uint32(rng.Int31()))
	}

	result := uint32(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecodeKey(vals[i%len(vals)], "%32u", &result)
	}
}

func BenchmarkEncodeKeyUint64(b *testing.B) {
	rng, _ := util.NewPseudoRand()

	vals := make([]interface{}, 10000)
	for i := range vals {
		vals[i] = uint64(rng.Int63())
	}

	buf := make([]byte, 0, 16)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeKey(buf, "%64u", vals[i%len(vals)])
	}
}

func BenchmarkDecodeKeyUint64(b *testing.B) {
	rng, _ := util.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUint64(nil, uint64(rng.Int63()))
	}

	result := uint64(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecodeKey(vals[i%len(vals)], "%64u", &result)
	}
}

func BenchmarkEncodeKeyVarint(b *testing.B) {
	rng, _ := util.NewPseudoRand()

	vals := make([]interface{}, 10000)
	for i := range vals {
		vals[i] = rng.Int63()
	}

	buf := make([]byte, 0, 16)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeKey(buf, "%d", vals[i%len(vals)])
	}
}

func BenchmarkDecodeKeyVarint(b *testing.B) {
	rng, _ := util.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeVarint(nil, rng.Int63())
	}

	result := int64(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecodeKey(vals[i%len(vals)], "%d", &result)
	}
}

func BenchmarkEncodeKeyUvarint(b *testing.B) {
	rng, _ := util.NewPseudoRand()

	vals := make([]interface{}, 10000)
	for i := range vals {
		vals[i] = uint64(rng.Int63())
	}

	buf := make([]byte, 0, 16)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeKey(buf, "%u", vals[i%len(vals)])
	}
}

func BenchmarkDecodeKeyUvarint(b *testing.B) {
	rng, _ := util.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUvarint(nil, uint64(rng.Int63()))
	}

	result := uint64(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecodeKey(vals[i%len(vals)], "%u", &result)
	}
}

func BenchmarkEncodeKeyBytes(b *testing.B) {
	rng, _ := util.NewPseudoRand()

	vals := make([]interface{}, 10000)
	for i := range vals {
		vals[i] = util.RandBytes(rng, 100)
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeKey(buf, "%s", vals[i%len(vals)])
	}
}

func BenchmarkDecodeKeyBytes(b *testing.B) {
	rng, _ := util.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeBytes(nil, util.RandBytes(rng, 100))
	}

	result := []byte(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecodeKey(vals[i%len(vals)], "%s", &result)
	}
}
