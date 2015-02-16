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
	"reflect"
	"testing"
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

func testBasicEncodeDecode32(enc func([]byte, uint32) []byte,
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
		enc := enc(nil, v)
		if i > 0 {
			if (decreasing && bytes.Compare(enc, lastEnc) >= 0) ||
				(!decreasing && bytes.Compare(enc, lastEnc) < 0) {
				t.Errorf("ordered constraint violated for %d: %s vs. %s", v, enc, lastEnc)
			}
		}
		b, decode := dec(enc)
		if len(b) != 0 {
			t.Errorf("leftover bytes: %s", b)
		}
		if decode != v {
			t.Errorf("decode yielded different value than input: %d vs. %d", decode, v)
		}
		lastEnc = enc
	}
}

type testCase32 struct {
	value  uint32
	expEnc []byte
}

func testCustomEncode32(testCases []testCase32, enc func([]byte, uint32) []byte, t *testing.T) {
	for _, test := range testCases {
		enc := enc(nil, test.value)
		if bytes.Compare(enc, test.expEnc) != 0 {
			t.Errorf("expected %s; got %s", test.expEnc, enc)
		}
	}
}

func TestEncodeDecodeUint32(t *testing.T) {
	testBasicEncodeDecode32(EncodeUint32, DecodeUint32, false, t)
	testCases := []testCase32{
		{0, []byte{0x00, 0x00, 0x00, 0x00}},
		{1, []byte{0x00, 0x00, 0x00, 0x01}},
		{1 << 8, []byte{0x00, 0x00, 0x01, 0x00}},
		{math.MaxUint32, []byte{0xff, 0xff, 0xff, 0xff}},
	}
	testCustomEncode32(testCases, EncodeUint32, t)
}

func TestEncodeDecodeUint32Decreasing(t *testing.T) {
	testBasicEncodeDecode32(EncodeUint32Decreasing, DecodeUint32Decreasing, true, t)
	testCases := []testCase32{
		{0, []byte{0xff, 0xff, 0xff, 0xff}},
		{1, []byte{0xff, 0xff, 0xff, 0xfe}},
		{1 << 8, []byte{0xff, 0xff, 0xfe, 0xff}},
		{math.MaxUint32, []byte{0x00, 0x00, 0x00, 0x00}},
	}
	testCustomEncode32(testCases, EncodeUint32Decreasing, t)
}

func testBasicEncodeDecode64(enc func([]byte, uint64) []byte,
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
		enc := enc(nil, v)
		if i > 0 {
			if (decreasing && bytes.Compare(enc, lastEnc) >= 0) ||
				(!decreasing && bytes.Compare(enc, lastEnc) < 0) {
				t.Errorf("ordered constraint violated for %d: %s vs. %s", v, enc, lastEnc)
			}
		}
		b, decode := dec(enc)
		if len(b) != 0 {
			t.Errorf("leftover bytes: %s", b)
		}
		if decode != v {
			t.Errorf("decode yielded different value than input: %d vs. %d", decode, v)
		}
		lastEnc = enc
	}
}

type testCase64 struct {
	value  uint64
	expEnc []byte
}

func testCustomEncode64(testCases []testCase64, enc func([]byte, uint64) []byte, t *testing.T) {
	for _, test := range testCases {
		enc := enc(nil, test.value)
		if bytes.Compare(enc, test.expEnc) != 0 {
			t.Errorf("expected %s; got %s", test.expEnc, enc)
		}
	}
}

func TestEncodeDecodeUint64(t *testing.T) {
	testBasicEncodeDecode64(EncodeUint64, DecodeUint64, false, t)
	testCases := []testCase64{
		{0, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{1, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{1 << 8, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00}},
		{math.MaxUint64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}
	testCustomEncode64(testCases, EncodeUint64, t)
}

func TestEncodeDecodeUint64Decreasing(t *testing.T) {
	testBasicEncodeDecode64(EncodeUint64Decreasing, DecodeUint64Decreasing, true, t)
	testCases := []testCase64{
		{0, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{1, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{1 << 8, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 0xff}},
		{math.MaxUint64, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}
	testCustomEncode64(testCases, EncodeUint64Decreasing, t)
}

func TestEncodeDecodeVarUint64(t *testing.T) {
	testBasicEncodeDecode64(EncodeVarUint64, DecodeVarUint64, false, t)
	testCases := []testCase64{
		{0, []byte{0x00}},
		{1, []byte{0x01, 0x01}},
		{1 << 8, []byte{0x02, 0x01, 0x00}},
		{math.MaxUint64, []byte{0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}
	testCustomEncode64(testCases, EncodeVarUint64, t)
}

func TestEncodeDecodeVarUint64Decreasing(t *testing.T) {
	testBasicEncodeDecode64(EncodeVarUint64Decreasing, DecodeVarUint64Decreasing, true, t)
	testCases := []testCase64{
		{0, []byte{0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{1, []byte{0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{1 << 8, []byte{0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 0xff}},
		{math.MaxUint64 - 1, []byte{0x01, 0x01}},
		{math.MaxUint64, []byte{0x00}},
	}
	testCustomEncode64(testCases, EncodeVarUint64Decreasing, t)
}
