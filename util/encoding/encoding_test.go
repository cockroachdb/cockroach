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
	enc func([]byte, uint32) []byte, t *testing.T) {
	for _, test := range testCases {
		enc := enc(nil, test.value)
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

func testBasicEncodeDecodeUint64(enc func([]byte, uint64) []byte,
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

func testBasicEncodeDecodeInt64(enc func([]byte, int64) []byte,
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
		enc := enc(nil, v)
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
	enc func([]byte, int64) []byte, t *testing.T) {
	for _, test := range testCases {
		enc := enc(nil, test.value)
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
	enc func([]byte, uint64) []byte, t *testing.T) {
	for _, test := range testCases {
		enc := enc(nil, test.value)
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

// TestDecodeInvalidUvarint tests that invalid uvarint encodings panic.
func TestDecodeInvalidUvarint(t *testing.T) {
	testCases := [][]byte{
		// length of 9 bytes should cause an error.
		{0x11, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01},
	}
	for _, c := range testCases {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic")
				}
			}()
			DecodeUvarint(c)
		}()
	}
}

// TestDecodeInvalidVarint tests that invalid uvarint encodings panic.
func TestDecodeInvalidVarint(t *testing.T) {
	testCases := [][]byte{
		// overflows int64.
		{0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	}
	for _, c := range testCases {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic")
				}
			}()
			DecodeVarint(c)
		}()
	}
}

func TestEncodeDecodeUvarintDecreasing(t *testing.T) {
	testBasicEncodeDecodeUint64(EncodeUvarintDecreasing, DecodeUvarintDecreasing, true, t)
	testCases := []testCaseUint64{
		{0, []byte{0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{1, []byte{0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{1 << 8, []byte{0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 0xff}},
		{math.MaxUint64 - 1, []byte{0x09, 0x01}},
		{math.MaxUint64, []byte{0x08}},
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
		remainder, dec := DecodeBytes(enc)
		if !bytes.Equal(c.value, dec) {
			t.Errorf("unexpected decoding mismatch for %v. got %v", c.value, dec)
		}
		if len(remainder) != 0 {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}

		enc = append(enc, []byte("remainder")...)
		remainder, dec = DecodeBytes(enc)
		if string(remainder) != "remainder" {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}
	}
}

func TestDecodeInvalidBytes(t *testing.T) {
	testCases := []struct {
		value []byte
	}{
		{[]byte{'a'}},             // no terminator
		{[]byte{'a', 0x00}},       // malformed escape
		{[]byte{'a', 0x00, 0x00}}, // invalid escape
		{[]byte{'a', 0x00, 0x02}}, // invalid escape
	}
	for _, c := range testCases {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic")
				}
			}()
			DecodeBytes(c.value)
		}()
	}
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

func BenchmarkDecodeBytes(b *testing.B) {
	rng, _ := util.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeBytes(nil, util.RandBytes(rng, 100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeBytes(vals[i%len(vals)])
	}
}
