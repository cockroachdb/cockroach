// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package encoding

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testBasicEncodeDecode32(
	encFunc func([]byte, uint32) []byte,
	decFunc func([]byte) ([]byte, uint32, error),
	descending bool,
	t *testing.T,
) {
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

func testCustomEncodeUint32(
	testCases []testCaseUint32, encFunc func([]byte, uint32) []byte, t *testing.T,
) {
	for _, test := range testCases {
		enc := encFunc(nil, test.value)
		if !bytes.Equal(enc, test.expEnc) {
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

func testBasicEncodeDecodeUint64(
	encFunc func([]byte, uint64) []byte,
	decFunc func([]byte) ([]byte, uint64, error),
	descending, testPeekLen, testUvarintEncLen bool,
	t *testing.T,
) {
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
		if testPeekLen {
			testPeekLength(t, enc)
		}
		if testUvarintEncLen {
			var encLen int
			if descending {
				encLen = EncLenUvarintDescending(v)
			} else {
				encLen = EncLenUvarintAscending(v)
			}
			if encLen != len(enc) {
				t.Errorf("EncLenUvarint for %d returned incorrect length %d, should be %d",
					v, encLen, len(enc))
			}
		}
		lastEnc = enc
	}
}

var int64TestCases = [...]int64{
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

func testBasicEncodeDecodeInt64(
	encFunc func([]byte, int64) []byte,
	decFunc func([]byte) ([]byte, int64, error),
	descending, testPeekLen bool,
	t *testing.T,
) {
	var lastEnc []byte
	for i, v := range int64TestCases {
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
		if testPeekLen {
			testPeekLength(t, enc)
		}
		lastEnc = enc
	}
}

type testCaseInt64 struct {
	value  int64
	expEnc []byte
}

func testCustomEncodeInt64(
	testCases []testCaseInt64, encFunc func([]byte, int64) []byte, t *testing.T,
) {
	for _, test := range testCases {
		enc := encFunc(nil, test.value)
		if !bytes.Equal(enc, test.expEnc) {
			t.Errorf("expected [% x]; got [% x] (value: %d)", test.expEnc, enc, test.value)
		}
	}
}

type testCaseUint64 struct {
	value  uint64
	expEnc []byte
}

func testCustomEncodeUint64(
	testCases []testCaseUint64, encFunc func([]byte, uint64) []byte, t *testing.T,
) {
	for _, test := range testCases {
		enc := encFunc(nil, test.value)
		if !bytes.Equal(enc, test.expEnc) {
			t.Errorf("expected [% x]; got [% x] (value: %d)", test.expEnc, enc, test.value)
		}
	}
}

func TestEncodeDecodeUint64(t *testing.T) {
	testBasicEncodeDecodeUint64(EncodeUint64Ascending, DecodeUint64Ascending, false, false, false, t)
	testCases := []testCaseUint64{
		{0, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{1, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{1 << 8, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00}},
		{math.MaxUint64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}
	testCustomEncodeUint64(testCases, EncodeUint64Ascending, t)
}

func TestEncodeDecodeUint64Descending(t *testing.T) {
	testBasicEncodeDecodeUint64(EncodeUint64Descending, DecodeUint64Descending, true, false, false, t)
	testCases := []testCaseUint64{
		{0, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{1, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{1 << 8, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 0xff}},
		{math.MaxUint64, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}
	testCustomEncodeUint64(testCases, EncodeUint64Descending, t)
}

func TestEncodeDecodeVarint(t *testing.T) {
	testBasicEncodeDecodeInt64(EncodeVarintAscending, DecodeVarintAscending, false, true, t)
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
	testBasicEncodeDecodeInt64(EncodeVarintDescending, DecodeVarintDescending, true, true, t)
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
	testBasicEncodeDecodeUint64(EncodeUvarintAscending, DecodeUvarintAscending, false, true, true, t)
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

func TestEncodedLengthUvarintAscending(t *testing.T) {
	for i := 0; i < 100; i++ {
		v := rand.Uint64()
		exp := len(EncodeUvarintAscending(nil, v))
		actual := EncodedLengthUvarintAscending(v)
		if actual != exp {
			t.Fatalf("incorrect encoded length for %d: %d (expected %d)", v, actual, exp)
		}
	}
}

func TestEncodeDecodeUvarintDescending(t *testing.T) {
	testBasicEncodeDecodeUint64(EncodeUvarintDescending, DecodeUvarintDescending, true, true, true, t)
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

// testPeekLength appends some random garbage to an encoding and verifies
// that PeekLength returns the correct length.
func testPeekLength(t *testing.T, encoded []byte) {
	gLen := rand.Intn(10)
	garbage := make([]byte, gLen)
	_, _ = rand.Read(garbage)

	var buf []byte
	buf = append(buf, encoded...)
	buf = append(buf, garbage...)

	if l, err := PeekLength(buf); err != nil {
		t.Fatal(err)
	} else if l != len(encoded) {
		t.Errorf("PeekLength returned incorrect length: %d, expected %d", l, len(encoded))
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

		testPeekLength(t, enc)

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

		testPeekLength(t, enc)

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

func TestEncodeDecodeUnsafeString(t *testing.T) {
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
		remainder, dec, err := DecodeUnsafeStringAscending(enc, nil)
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

		testPeekLength(t, enc)

		enc = append(enc, "remainder"...)
		remainder, _, err = DecodeUnsafeStringAscending(enc, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if string(remainder) != "remainder" {
			t.Errorf("unexpected remaining bytes: %v", remainder)
		}
	}
}

func encodeBitArrayWithDir(dir Direction, buf []byte, d bitarray.BitArray) []byte {
	if dir == Ascending {
		return EncodeBitArrayAscending(buf, d)
	}
	return EncodeBitArrayDescending(buf, d)
}

func decodeBitArrayWithDir(
	t *testing.T, dir Direction, buf []byte, tmp []byte,
) ([]byte, bitarray.BitArray) {
	var err error
	var resBuf []byte
	var res bitarray.BitArray
	if dir == Ascending {
		resBuf, res, err = DecodeBitArrayAscending(buf)
	} else {
		resBuf, res, err = DecodeBitArrayDescending(buf)
	}
	if err != nil {
		t.Fatal(err)
	}
	return resBuf, res
}

func TestEncodeBitArray(t *testing.T) {
	ba := func(s string) bitarray.BitArray {
		res, _ := bitarray.Parse(s)
		return res
	}

	testCases := []struct {
		Value    bitarray.BitArray
		Encoding []byte
	}{
		{ba(""),
			[]byte{0x3a, 0 /* no words */, 0x88}},
		{ba("0"),
			[]byte{0x3a, 0x88 /* word 0 */, 0, 0x89}},
		{ba("1"),
			[]byte{0x3a,
				0xfd, 0x80, 0, 0, 0, 0, 0, 0, 0, // word 0
				0, 0x89}},
		{ba("00"),
			[]byte{0x3a, 0x88 /* word 0 */, 0, 0x8a}},
		{ba("01"),
			[]byte{0x3a,
				0xfd, 0x40, 0, 0, 0, 0, 0, 0, 0, // word 0
				0, 0x8a}},
		{ba("10"),
			[]byte{0x3a,
				0xfd, 0x80, 0, 0, 0, 0, 0, 0, 0, // word 0
				0, 0x8a}},
		{ba("11"),
			[]byte{0x3a,
				0xfd, 0xc0, 0, 0, 0, 0, 0, 0, 0, // word 0
				0, 0x8a}},
		{bitarray.MakeZeroBitArray(32),
			[]byte{0x3a, 0x88 /* word 0*/, 0, 0xa8}},
		{bitarray.Not(bitarray.MakeZeroBitArray(32)),
			[]byte{0x3a,
				0xfd, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, // word 0
				0, 0xa8}},
		{bitarray.Concat(bitarray.MakeZeroBitArray(32), ba("00")),
			[]byte{0x3a, 0x88 /* word 0 */, 0, 0xaa}},
		{bitarray.Concat(bitarray.MakeZeroBitArray(32), ba("01")),
			[]byte{0x3a,
				0xf9, 0x40, 0, 0, 0, // word 0
				0, 0xaa}},
		{bitarray.Concat(bitarray.MakeZeroBitArray(32), ba("10")),
			[]byte{0x3a,
				0xf9, 0x80, 0, 0, 0, // word 0
				0, 0xaa}},
		{bitarray.Concat(bitarray.MakeZeroBitArray(32), ba("11")),
			[]byte{0x3a,
				0xf9, 0xc0, 0, 0, 0, // word 0
				0, 0xaa}},
		{bitarray.MakeZeroBitArray(48),
			[]byte{0x3a, 0x88 /* word 0 */, 0, 0xb8}},
		{bitarray.Not(bitarray.MakeZeroBitArray(48)),
			[]byte{0x3a,
				0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, // word 0
				0, 0xb8}},
		{bitarray.Concat(bitarray.MakeZeroBitArray(48), ba("00")),
			[]byte{0x3a, 0x88 /* word 0 */, 0, 0xba}},
		{bitarray.Concat(bitarray.MakeZeroBitArray(48), ba("01")),
			[]byte{0x3a,
				0xf7, 0x40, 0, // word 0
				0, 0xba}},
		{bitarray.Concat(bitarray.MakeZeroBitArray(48), ba("10")),
			[]byte{0x3a,
				0xf7, 0x80, 0, // word 0
				0, 0xba}},
		{bitarray.Concat(bitarray.MakeZeroBitArray(48), ba("11")),
			[]byte{0x3a,
				0xf7, 0xc0, 0, // word 0
				0, 0xba}},
		{bitarray.MakeZeroBitArray(62),
			[]byte{0x3a,
				0x88, //word 0
				0, 0xc6}},
		{bitarray.Not(bitarray.MakeZeroBitArray(62)),
			[]byte{0x3a,
				0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfc, // word 0
				0, 0xc6}},
		{bitarray.Concat(bitarray.MakeZeroBitArray(62), ba("00")),
			[]byte{0x3a, 0x88 /* word 0*/, 0, 0xc8}},
		{bitarray.Concat(bitarray.MakeZeroBitArray(62), ba("01")),
			[]byte{0x3a, 0x89 /* word 0*/, 0, 0xc8}},
		{bitarray.Concat(bitarray.MakeZeroBitArray(62), ba("10")),
			[]byte{0x3a, 0x8a /* word 0*/, 0, 0xc8}},
		{bitarray.Concat(bitarray.MakeZeroBitArray(62), ba("11")),
			[]byte{0x3a, 0x8b /* word 0*/, 0, 0xc8}},
		{bitarray.Not(bitarray.MakeZeroBitArray(64)),
			[]byte{0x3a,
				0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // word 0
				0, 0xc8}},
		{bitarray.MakeZeroBitArray(65),
			[]byte{0x3a,
				0x88, // word 0
				0x88, // word 1
				0, 0x89}},
		{bitarray.Not(bitarray.MakeZeroBitArray(65)),
			[]byte{0x3a,
				0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // word 0
				0xfd, 0x80, 0, 0, 0, 0, 0, 0, 0, // word 1
				0, 0x89}},
		{bitarray.MakeZeroBitArray(66),
			[]byte{0x3a,
				0x88, // word 0
				0x88, // word 1
				0, 0x8a}},
		{bitarray.MakeZeroBitArray(128),
			[]byte{0x3a,
				0x88, // word 0
				0x88, // word 1
				0, 0xc8}},
		{bitarray.Not(bitarray.MakeZeroBitArray(128)),
			[]byte{0x3a,
				0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // word 0
				0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // word 1
				0, 0xc8}},
	}

	rng, _ := randutil.NewTestRand()

	var lastEncoded []byte
	dirNames := []string{"", "asc", "desc"}
	for _, dir := range []Direction{Ascending, Descending} {
		for _, tmp := range [][]byte{nil, make([]byte, 0, 100)} {
			for i, c := range testCases {
				t.Run(fmt.Sprintf("%s/%d/%d/%s", dirNames[dir], cap(tmp), i, c.Value), func(t *testing.T) {
					enc := encodeBitArrayWithDir(dir, nil, c.Value)
					_, dec := decodeBitArrayWithDir(t, dir, enc, tmp)
					if dir == Ascending && !bytes.Equal(enc, c.Encoding) {
						t.Errorf("unexpected mismatch for %s. expected [% x], got [% x]",
							c.Value, c.Encoding, enc)
					}
					if i > 0 {
						valCompare := bitarray.Compare(testCases[i-1].Value, testCases[i].Value)
						expCompare := valCompare
						if dir == Descending {
							expCompare = -expCompare
						}
						const compareSigns = "<=>"
						gotCompare := bytes.Compare(lastEncoded, enc)
						if gotCompare != expCompare {
							t.Errorf("%q %c %q, however their %s encodings are %v %c %v",
								testCases[i-1].Value, compareSigns[valCompare+1], testCases[i].Value,
								dirNames[dir],
								lastEncoded, compareSigns[gotCompare+1], enc)
						}
					}
					testPeekLength(t, enc)
					if bitarray.Compare(dec, c.Value) != 0 {
						t.Errorf("%d unexpected mismatch for %v. got %v", i, c.Value, dec)
					}
					lastEncoded = enc

					// Test that appending the bitarray to an existing buffer works. It
					// is important to test with various values, slice lengths, and
					// capacities because the various encoding paths try to use any
					// spare capacity to avoid allocations.
					for trials := 0; trials < 5; trials++ {
						orig := randBuf(rng, 30)
						origLen := len(orig)

						bufCap := origLen + rng.Intn(30)
						buf := make([]byte, origLen, bufCap)
						copy(buf, orig)

						enc := encodeBitArrayWithDir(dir, buf, c.Value)
						// Append some random bytes
						enc = append(enc, randBuf(rng, 20)...)
						_, dec := decodeBitArrayWithDir(t, dir, enc[origLen:], tmp)

						if bitarray.Compare(dec, c.Value) != 0 {
							t.Errorf("unexpected mismatch for %v. got %v", c.Value, dec)
						}
						// Verify the existing values weren't modified.
						for i := range orig {
							if enc[i] != orig[i] {
								t.Errorf("existing byte %d changed after encoding (from %d to %d)",
									i, orig[i], enc[i])
							}
						}
					}
				})
			}
		}
	}
}

func TestKeyEncodeDecodeBitArrayRand(t *testing.T) {
	rng, seed := randutil.NewTestRand()
	rd := randData{rng}
	tests := make([]bitarray.BitArray, 1000)
	for i := range tests {
		tests[i] = rd.bitArray()
	}
	for i, test := range tests {
		for _, dir := range []Direction{Ascending, Descending} {
			var remainder, buf []byte
			var err error
			var x bitarray.BitArray
			if dir == Ascending {
				buf = EncodeBitArrayAscending(nil, test)
				remainder, x, err = DecodeBitArrayAscending(buf)
			} else {
				buf = EncodeBitArrayAscending(nil, test)
				remainder, x, err = DecodeBitArrayAscending(buf)
			}
			if err != nil {
				t.Fatalf("%+v", err)
			}
			if bitarray.Compare(x, tests[i]) != 0 {
				t.Errorf("seed %d: expected %v got %v (buf: %+v)", seed, &tests[i], &x, buf)
			}
			if len(remainder) > 0 {
				t.Errorf("seed %d: decoding %v tailing bytes: %+v", seed, &tests[i], remainder)
			}
		}
	}
}

func TestPrettyPrintValue(t *testing.T) {
	ba := bitarray.MakeBitArrayFromInt64(8, 58, 7)

	testData := []struct {
		dir Direction
		key []byte
		exp string
	}{
		{Ascending, EncodeFloatAscending(nil, float64(233.221112)), "/233.221112"},
		{Descending, EncodeFloatDescending(nil, float64(233.221112)), "/233.221112"},
		{Ascending, EncodeBitArrayAscending(nil, ba), "/B00111010"},
		{Descending, EncodeBitArrayDescending(nil, ba), "/B00111010"},
	}

	for _, test := range testData {
		dirStr := "Asc"
		if test.dir != Ascending {
			dirStr = "Desc"
		}
		t.Run(test.exp[1:]+"/"+dirStr, func(t *testing.T) {
			got := PrettyPrintValue([]Direction{test.dir}, test.key, "/")
			if got != test.exp {
				t.Errorf("expected %q, got %q", test.exp, got)
			}
		})
	}
}

func TestEncodeDecodeUnsafeStringDescending(t *testing.T) {
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
		remainder, dec, err := DecodeUnsafeStringDescending(enc, nil)
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

		testPeekLength(t, enc)

		enc = append(enc, "remainder"...)
		remainder, _, err = DecodeUnsafeStringDescending(enc, nil)
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
	zeroTime := timeutil.Unix(0, 0)

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
				testPeekLength(t, b)
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

func TestEncodeDecodeTimeTZ(t *testing.T) {
	// Test cases are in ascending order for TimeTZ, which means:
	// * UTC timestamp first preference
	// * Negative Zone Offset second preference
	//
	// Tests these UTC times:
	//  05:06:07
	//  10:11:12
	//  15:16:17
	//  20:21:22
	// Over time zones (if not overflowing):
	//  -12
	//  -08
	//  -04
	//  0
	//  +04
	//  +08
	//  +12
	testCases := []string{
		"00:00:00+1559", // minimum

		"17:06:07+12",
		"13:06:07+8",
		"09:06:07+4",
		"05:06:07+0",
		"01:06:07-4",

		"22:11:12+12",
		"18:11:12+8",
		"14:11:12+4",
		"10:41:12+0030", // special check of .5 hour offsets
		"10:11:12+0",
		"06:11:12-4",
		"04:11:12-8",

		"23:16:17+8",
		"19:16:17+4",
		"15:16:17+0",
		"11:16:17-4",
		"09:16:17-8",
		"05:16:17-12",

		"20:21:22+0",
		"16:21:22-4",
		"12:21:22-8",
		"08:21:22-12",

		"24:00:00-1559", // maximum
	}

	var lastEncoded []byte
	for _, dir := range []Direction{Ascending, Descending} {
		t.Run(fmt.Sprintf("dir:%d", dir), func(t *testing.T) {
			for i := range testCases {
				t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
					current, _, err := timetz.ParseTimeTZ(timeutil.Now(), pgdate.DefaultDateStyle(), testCases[i], time.Microsecond)
					assert.NoError(t, err)

					var b []byte
					var decodedCurrent timetz.TimeTZ
					if dir == Ascending {
						b = EncodeTimeTZAscending(b, current)
						_, decodedCurrent, err = DecodeTimeTZAscending(b)
					} else {
						b = EncodeTimeTZDescending(b, current)
						_, decodedCurrent, err = DecodeTimeTZDescending(b)
					}
					assert.NoError(t, err)
					assert.Equal(t, current, decodedCurrent)
					testPeekLength(t, b)
					if i > 0 {
						if dir == Ascending {
							assert.True(t, bytes.Compare(lastEncoded, b) < 0, "encodings %s, %s not increasing", testCases[i-1], testCases[i])
						} else {
							assert.True(t, bytes.Compare(lastEncoded, b) > 0, "encodings %s, %s not decreasing", testCases[i-1], testCases[i])
						}
					}
					lastEncoded = b
				})
			}
		})
	}
}

func TestEncodeDecodeBox2D(t *testing.T) {
	testCases := []struct {
		ordered []geopb.BoundingBox
	}{
		{
			ordered: []geopb.BoundingBox{
				{LoX: -100, HiX: 99, LoY: -100, HiY: 100},
				{LoX: -100, HiX: 100, LoY: -100, HiY: 100},
				{LoX: -50, HiX: 100, LoY: -100, HiY: 100},
				{LoX: 0, HiX: 100, LoY: 0, HiY: 100},
				{LoX: 0, HiX: 100, LoY: 50, HiY: 100},
				{LoX: 10, HiX: 100, LoY: -100, HiY: 100},
				{LoX: 10, HiX: 100, LoY: -10, HiY: 50},
				{LoX: 10, HiX: 100, LoY: -10, HiY: 100},
			},
		},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			for _, dir := range []Direction{Ascending, Descending} {
				t.Run(fmt.Sprintf("dir:%d", dir), func(t *testing.T) {
					var lastEncoded []byte
					for j := range tc.ordered {
						var b []byte
						var err error
						var decoded geopb.BoundingBox

						if dir == Ascending {
							b, err = EncodeBox2DAscending(b, tc.ordered[j])
							require.NoError(t, err)
							_, decoded, err = DecodeBox2DAscending(b)
							require.NoError(t, err)
						} else {
							b, err = EncodeBox2DDescending(b, tc.ordered[j])
							require.NoError(t, err)
							_, decoded, err = DecodeBox2DDescending(b)
							require.NoError(t, err)
						}
						require.Equal(t, tc.ordered[j], decoded)
						testPeekLength(t, b)

						if j > 0 {
							if dir == Ascending {
								assert.Truef(t, bytes.Compare(b, lastEncoded) > 0, "expected %s > %s", tc.ordered[j], tc.ordered[j-1])
							} else {
								assert.Truef(t, bytes.Compare(b, lastEncoded) < 0, "expected %s < %s", tc.ordered[j], tc.ordered[j-1])
							}
						}

						lastEncoded = b
					}
				})
			}
		})
	}
}

func TestEncodeDecodeGeometry(t *testing.T) {
	testCases := []struct {
		orderedWKTs []string
	}{
		{
			orderedWKTs: []string{
				"SRID=4326;POLYGON EMPTY",
				"SRID=4326;POINT EMPTY",
				"SRID=4326;LINESTRING(0 0, -90 -80)",
				"SRID=4326;POINT(-80 80)",
				"SRID=4326;POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
			},
		},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			for _, dir := range []Direction{Ascending, Descending} {
				t.Run(fmt.Sprintf("dir:%d", dir), func(t *testing.T) {
					var lastEncoded []byte
					for j, wkt := range tc.orderedWKTs {
						parsed, err := geo.ParseGeometry(wkt)
						require.NoError(t, err)
						spatialObject := parsed.SpatialObject()

						var b []byte
						var decoded geopb.SpatialObject
						spaceCurveIndex, err := parsed.SpaceCurveIndex()
						require.NoError(t, err)
						if dir == Ascending {
							b, err = EncodeGeoAscending(b, spaceCurveIndex, &spatialObject)
							require.NoError(t, err)
							_, err = DecodeGeoAscending(b, &decoded)
							require.NoError(t, err)
						} else {
							b, err = EncodeGeoDescending(b, spaceCurveIndex, &spatialObject)
							require.NoError(t, err)
							_, err = DecodeGeoDescending(b, &decoded)
							require.NoError(t, err)
						}
						require.Equal(t, spatialObject, decoded)
						testPeekLength(t, b)

						if j > 0 {
							if dir == Ascending {
								assert.Truef(t, bytes.Compare(b, lastEncoded) > 0, "expected %s > %s", tc.orderedWKTs[j], tc.orderedWKTs[j-1])
							} else {
								assert.Truef(t, bytes.Compare(b, lastEncoded) < 0, "expected %s < %s", tc.orderedWKTs[j], tc.orderedWKTs[j-1])
							}
						}

						lastEncoded = b
					}
				})
			}
		})
	}
}

func TestEncodeDecodeGeography(t *testing.T) {
	testCases := []struct {
		orderedWKTs []string
	}{
		{
			orderedWKTs: []string{
				"SRID=4326;POLYGON EMPTY",
				"SRID=4326;POINT EMPTY",
				"SRID=4326;POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
				"SRID=4326;POINT(-80 80)",
				"SRID=4326;LINESTRING(0 0, -90 -80)",
			},
		},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			for _, dir := range []Direction{Ascending, Descending} {
				t.Run(fmt.Sprintf("dir:%d", dir), func(t *testing.T) {
					var lastEncoded []byte
					for j, wkt := range tc.orderedWKTs {
						parsed, err := geo.ParseGeography(wkt)
						require.NoError(t, err)
						spatialObject := parsed.SpatialObject()

						var b []byte
						var decoded geopb.SpatialObject
						if dir == Ascending {
							b, err = EncodeGeoAscending(b, parsed.SpaceCurveIndex(), &spatialObject)
							require.NoError(t, err)
							_, err = DecodeGeoAscending(b, &decoded)
							require.NoError(t, err)
						} else {
							b, err = EncodeGeoDescending(b, parsed.SpaceCurveIndex(), &spatialObject)
							require.NoError(t, err)
							_, err = DecodeGeoDescending(b, &decoded)
							require.NoError(t, err)
						}
						require.Equal(t, spatialObject, decoded)
						testPeekLength(t, b)

						if j > 0 {
							if dir == Ascending {
								assert.Truef(t, bytes.Compare(b, lastEncoded) > 0, "expected %s > %s", tc.orderedWKTs[j], tc.orderedWKTs[j-1])
							} else {
								assert.Truef(t, bytes.Compare(b, lastEncoded) < 0, "expected %s < %s", tc.orderedWKTs[j], tc.orderedWKTs[j-1])
							}
						}

						lastEncoded = b
					}
				})
			}
		})
	}
}

func TestEncodeDecodeGeoInvertedIndex(t *testing.T) {
	testCases := []struct {
		shape          string
		cellID         uint64
		expectedLength int
	}{
		{
			shape:          "SRID=4326;LINESTRING(0 0, -90 -80)",
			cellID:         0,
			expectedLength: 35,
		},
		{
			shape:          "SRID=4326;LINESTRING(0 0, -90 -80)",
			cellID:         math.MaxUint64,
			expectedLength: 43,
		},
		{
			shape:          "SRID=4326;POINT(-80 80)",
			cellID:         0,
			expectedLength: 19,
		},
		{
			shape:          "SRID=4326;POINT(-80 80)",
			cellID:         math.MaxUint64,
			expectedLength: 27,
		},
		{
			shape:          "SRID=4326;POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
			cellID:         10000,
			expectedLength: 37,
		},
		{
			shape:          "SRID=4326;POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
			cellID:         math.MaxUint32,
			expectedLength: 39,
		},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			parsed, err := geo.ParseGeometry(tc.shape)
			require.NoError(t, err)
			var b []byte
			b = EncodeGeoInvertedAscending(b)
			b = EncodeUvarintAscending(b, tc.cellID)
			bbox := parsed.BoundingBoxRef()
			require.NotNil(t, bbox)
			b = EncodeGeoInvertedBBox(b, bbox.LoX, bbox.LoY, bbox.HiX, bbox.HiY)
			require.Equal(t, tc.expectedLength, len(b))
			var dBBox geopb.BoundingBox
			dBBox.LoX, dBBox.LoY, dBBox.HiX, dBBox.HiY, b, err = DecodeGeoInvertedKey(b)
			require.NoError(t, err)
			require.Equal(t, *bbox, dBBox)
			require.Equal(t, 0, len(b))
		})
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
		testPeekLength(t, enc)
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
		if !bytes.Equal(enc, test.expEnc) {
			t.Errorf("%d expected [% x]; got [% x] (value: %d)", i, test.expEnc, enc, test.value)
		}
		_, decoded, err := decFunc(enc)
		if err != nil {
			t.Fatal(err)
		}
		if test.value != decoded {
			t.Errorf("%d duration changed during roundtrip [%s] vs [%s]", i, test.value, decoded)
		}
		testPeekLength(t, enc)
	}
}

func TestEncodeDecodeDuration(t *testing.T) {
	testCases := []testCaseDuration{
		{duration.DecodeDuration(0, 0, 0), []byte{0x16, 0x88, 0x88, 0x88}},
		{duration.DecodeDuration(0, 0, 1), []byte{0x16, 0x89, 0x88, 0x88}},
		{duration.DecodeDuration(0, 1, 0), []byte{0x16, 0xfb, 0x4e, 0x94, 0x91, 0x4f, 0x00, 0x00, 0x88, 0x89}},
		{duration.DecodeDuration(1, 0, 0), []byte{0x16, 0xfc, 0x09, 0x35, 0x69, 0x07, 0x42, 0x00, 0x00, 0x89, 0x88}},
		{duration.DecodeDuration(0, 40, 0), []byte{0x16, 0xfc, 0x0c, 0x47, 0x36, 0xb4, 0x58, 0x00, 0x00, 0x88, 0xb0}},
	}
	testBasicEncodeDuration(testCases, EncodeDurationAscending, t)
	testCustomEncodeDuration(testCases, EncodeDurationAscending, DecodeDurationAscending, t)
}

func TestEncodeDecodeDescending(t *testing.T) {
	testCases := []testCaseDuration{
		{duration.DecodeDuration(0, 40, 0), []byte{0x16, 0x81, 0xf3, 0xb8, 0xc9, 0x4b, 0xa7, 0xff, 0xff, 0x87, 0xff, 0x87, 0xd7}},
		{duration.DecodeDuration(1, 0, 0), []byte{0x16, 0x81, 0xf6, 0xca, 0x96, 0xf8, 0xbd, 0xff, 0xff, 0x87, 0xfe, 0x87, 0xff}},
		{duration.DecodeDuration(0, 1, 0), []byte{0x16, 0x82, 0xb1, 0x6b, 0x6e, 0xb0, 0xff, 0xff, 0x87, 0xff, 0x87, 0xfe}},
		{duration.DecodeDuration(0, 0, 1), []byte{0x16, 0x87, 0xfe, 0x87, 0xff, 0x87, 0xff}},
		{duration.DecodeDuration(0, 0, 0), []byte{0x16, 0x87, 0xff, 0x87, 0xff, 0x87, 0xff}},
	}
	testBasicEncodeDuration(testCases, EncodeDurationDescending, t)
	testCustomEncodeDuration(testCases, EncodeDurationDescending, DecodeDurationDescending, t)
}

func TestPeekType(t *testing.T) {
	encodedDurationAscending, err := EncodeDurationAscending(nil, duration.Duration{})
	require.NoError(t, err)
	encodedDurationDescending, err := EncodeDurationDescending(nil, duration.Duration{})
	require.NoError(t, err)
	encodedGeoAscending, err := EncodeGeoAscending(nil, 0, &geopb.SpatialObject{})
	require.NoError(t, err)
	encodedGeoDescending, err := EncodeGeoDescending(nil, 0, &geopb.SpatialObject{})
	require.NoError(t, err)
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
		{EncodeDecimalAscending(nil, apd.New(0, 0)), Decimal},
		{EncodeDecimalDescending(nil, apd.New(0, 0)), Decimal},
		{EncodeBytesAscending(nil, []byte("")), Bytes},
		{EncodeBytesDescending(nil, []byte("")), BytesDesc},
		{EncodeTimeAscending(nil, timeutil.Now()), Time},
		{EncodeTimeDescending(nil, timeutil.Now()), Time},
		{EncodeTimeTZAscending(nil, timetz.Now()), TimeTZ},
		{EncodeTimeTZDescending(nil, timetz.Now()), TimeTZ},
		{encodedGeoAscending, Geo},
		{encodedGeoDescending, GeoDesc},
		{encodedDurationAscending, Duration},
		{encodedDurationDescending, Duration},
		{EncodeBitArrayAscending(nil, bitarray.BitArray{}), BitArray},
		{EncodeBitArrayDescending(nil, bitarray.BitArray{}), BitArrayDesc},
	}
	for i, c := range testCases {
		typ := PeekType(c.enc)
		if c.typ != typ {
			t.Fatalf("%d: expected %d, but found %d", i, c.typ, typ)
		}
	}
}

var sink string

func BenchmarkPeekType(b *testing.B) {
	buf := EncodeVarintAscending(nil, 0)
	var typ Type
	for i := 0; i < b.N; i++ {
		typ = PeekType(buf)
	}
	sink = fmt.Sprint(typ)
}

type randData struct {
	*rand.Rand
}

func (rd randData) bool() bool {
	return rd.Intn(2) == 1
}

func (rd randData) decimal() *apd.Decimal {
	return apd.New(rd.Int63(), int32(rd.Intn(40)-20))
}

func (rd randData) time() time.Time {
	return timeutil.Unix(rd.Int63n(1000000), rd.Int63n(1000000))
}

func (rd randData) timetz() timetz.TimeTZ {
	return timetz.MakeTimeTZ(
		timeofday.FromInt(rd.Int63n(int64(timeofday.Max))),
		rd.Int31n(timetz.MaxTimeTZOffsetSecs*2)-timetz.MaxTimeTZOffsetSecs,
	)
}

func (rd randData) bitArray() bitarray.BitArray {
	return bitarray.Rand(rd.Rand, uint(rd.Int31n(140)))
}

func (rd randData) duration() duration.Duration {
	return duration.DecodeDuration(
		rd.Int63n(1000),
		rd.Int63n(1000),
		rd.Int63n(1000000),
	)
}

func (rd randData) ipAddr() ipaddr.IPAddr {
	return ipaddr.RandIPAddr(rd.Rand)
}

func BenchmarkEncodeUint32(b *testing.B) {
	rng, _ := randutil.NewTestRand()

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
	rng, _ := randutil.NewTestRand()

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
	rng, _ := randutil.NewTestRand()

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
	rng, _ := randutil.NewTestRand()

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
	rng, _ := randutil.NewTestRand()

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
	rng, _ := randutil.NewTestRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeVarintAscending(nil, rng.Int63())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeVarintAscending(vals[i%len(vals)])
	}
}

func BenchmarkPeekLengthVarint(b *testing.B) {
	rng, _ := randutil.NewTestRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeVarintAscending(nil, rng.Int63())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = PeekLength(vals[i%len(vals)])
	}
}

func BenchmarkEncodeUvarint(b *testing.B) {
	rng, _ := randutil.NewTestRand()

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
	rng, _ := randutil.NewTestRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUvarintAscending(nil, uint64(rng.Int63()))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeUvarintAscending(vals[i%len(vals)])
	}
}

func BenchmarkPeekLengthUvarint(b *testing.B) {
	rng, _ := randutil.NewTestRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeUvarintAscending(nil, uint64(rng.Int63()))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = PeekLength(vals[i%len(vals)])
	}
}

func BenchmarkEncodeBytes(b *testing.B) {
	rng, _ := randutil.NewTestRand()

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
	rng, _ := randutil.NewTestRand()

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
	rng, _ := randutil.NewTestRand()

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

func BenchmarkPeekLengthBytes(b *testing.B) {
	rng, _ := randutil.NewTestRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeBytesAscending(nil, randutil.RandBytes(rng, 100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = PeekLength(vals[i%len(vals)])
	}
}

func BenchmarkDecodeBytesDescending(b *testing.B) {
	rng, _ := randutil.NewTestRand()

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

func BenchmarkPeekLengthBytesDescending(b *testing.B) {
	rng, _ := randutil.NewTestRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeBytesDescending(nil, randutil.RandBytes(rng, 100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = PeekLength(vals[i%len(vals)])
	}
}

func BenchmarkEncodeString(b *testing.B) {
	rng, _ := randutil.NewTestRand()

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
	rng, _ := randutil.NewTestRand()

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

func BenchmarkDecodeUnsafeString(b *testing.B) {
	rng, _ := randutil.NewTestRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeStringAscending(nil, string(randutil.RandBytes(rng, 100)))
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeUnsafeStringAscending(vals[i%len(vals)], buf)
	}
}

func BenchmarkDecodeUnsafeStringDescending(b *testing.B) {
	rng, _ := randutil.NewTestRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeStringDescending(nil, string(randutil.RandBytes(rng, 100)))
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeUnsafeStringDescending(vals[i%len(vals)], buf)
	}
}

func BenchmarkEncodeDuration(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	rd := randData{rng}

	vals := make([]duration.Duration, 10000)
	for i := range vals {
		vals[i] = rd.duration()
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeDurationAscending(buf, vals[i%len(vals)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeDuration(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	rd := randData{rng}

	vals := make([][]byte, 10000)
	for i := range vals {
		var err error
		if vals[i], err = EncodeDurationAscending(nil, rd.duration()); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := DecodeDurationAscending(vals[i%len(vals)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPeekLengthDuration(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	rd := randData{rng}

	vals := make([][]byte, 10000)
	for i := range vals {
		d := rd.duration()
		var err error
		vals[i], err = EncodeDurationAscending(nil, d)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = PeekLength(vals[i%len(vals)])
	}
}

func TestValueEncodeDecodeBool(t *testing.T) {
	tests := []bool{true, false}
	for _, test := range tests {
		buf := EncodeBoolValue(nil, NoColumnID, test)
		_, x, err := DecodeBoolValue(buf)
		if err != nil {
			t.Fatal(err)
		}
		if x != test {
			t.Errorf("expected %v got %v", test, x)
		}
	}
}

func TestValueEncodeDecodeInt(t *testing.T) {
	rng, seed := randutil.NewTestRand()
	tests := append(int64TestCases[0:], randPowDistributedInt63s(rng, 1000)...)
	for _, test := range tests {
		buf := EncodeIntValue(nil, NoColumnID, test)
		_, x, err := DecodeIntValue(buf)
		if err != nil {
			t.Fatal(err)
		}
		if x != test {
			t.Errorf("seed %d: expected %v got %v", seed, test, x)
		}
	}
}

func TestValueEncodeDecodeFloat(t *testing.T) {
	rng, seed := randutil.NewTestRand()
	tests := make([]float64, 1000)
	for i := range tests {
		tests[i] = rng.NormFloat64()
	}
	for _, test := range tests {
		buf := EncodeFloatValue(nil, NoColumnID, test)
		_, x, err := DecodeFloatValue(buf)
		if err != nil {
			t.Fatal(err)
		}
		if x != test {
			t.Errorf("seed %d: expected %v got %v", seed, test, x)
		}
	}
}

func TestValueEncodeDecodeBytes(t *testing.T) {
	rng, seed := randutil.NewTestRand()
	tests := make([][]byte, 1000)
	for i := range tests {
		tests[i] = randutil.RandBytes(rng, 100)
	}
	for _, test := range tests {
		buf := EncodeBytesValue(nil, NoColumnID, test)
		_, x, err := DecodeBytesValue(buf)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(x, test) {
			t.Errorf("seed %d: expected %v got %v", seed, test, x)
		}
	}
}

func TestValueEncodeDecodeDecimal(t *testing.T) {
	rng, seed := randutil.NewTestRand()
	rd := randData{rng}
	tests := make([]*apd.Decimal, 1000)
	for i := range tests {
		tests[i] = rd.decimal()
	}
	for _, test := range tests {
		buf := EncodeDecimalValue(nil, NoColumnID, test)
		_, x, err := DecodeDecimalValue(buf)
		if err != nil {
			t.Fatal(err)
		}
		if x.Cmp(test) != 0 {
			t.Errorf("seed %d: expected %v got %v", seed, test, x)
		}
	}
}

func TestValueEncodeDecodeTime(t *testing.T) {
	rng, seed := randutil.NewTestRand()
	rd := randData{rng}
	tests := make([]time.Time, 1000)
	for i := range tests {
		tests[i] = rd.time()
	}
	for _, test := range tests {
		buf := EncodeTimeValue(nil, NoColumnID, test)
		_, x, err := DecodeTimeValue(buf)
		if err != nil {
			t.Fatal(err)
		}
		if x != test {
			t.Errorf("seed %d: expected %v got %v", seed, test, x)
		}
	}
}

func TestValueEncodeDecodeTimeTZ(t *testing.T) {
	rng, seed := randutil.NewTestRand()
	rd := randData{rng}
	tests := make([]timetz.TimeTZ, 1000)
	for i := range tests {
		tests[i] = rd.timetz()
	}
	for _, test := range tests {
		buf := EncodeTimeTZValue(nil, NoColumnID, test)
		_, x, err := DecodeTimeTZValue(buf)
		if err != nil {
			t.Fatal(err)
		}
		if x != test {
			t.Errorf("seed %d: expected %v got %v", seed, test, x)
		}
	}
}

func TestValueEncodeDecodeBitArray(t *testing.T) {
	rng, seed := randutil.NewTestRand()
	rd := randData{rng}
	tests := make([]bitarray.BitArray, 1000)
	for i := range tests {
		tests[i] = rd.bitArray()
	}
	for i, test := range tests {
		buf := EncodeBitArrayValue(nil, NoColumnID, test)
		remainder, x, err := DecodeBitArrayValue(buf)
		if err != nil {
			t.Fatal(err)
		}
		if bitarray.Compare(x, tests[i]) != 0 {
			t.Errorf("seed %d: expected %v got %v (buf: %+v)", seed, &tests[i], &x, buf)
		}
		if len(remainder) > 0 {
			t.Errorf("seed %d: decoding %v tailing bytes: %+v", seed, &tests[i], remainder)
		}
	}
}

func TestValueEncodeDecodeDuration(t *testing.T) {
	rng, seed := randutil.NewTestRand()
	rd := randData{rng}
	tests := make([]duration.Duration, 1000)
	for i := range tests {
		tests[i] = rd.duration()
	}
	for _, test := range tests {
		buf := EncodeDurationValue(nil, NoColumnID, test)
		_, x, err := DecodeDurationValue(buf)
		if err != nil {
			t.Fatal(err)
		}
		if x != test {
			t.Errorf("seed %d: expected %v got %v", seed, test, x)
		}
	}
}

func BenchmarkEncodeNonsortingVarint(b *testing.B) {
	bytes := make([]byte, 0, b.N*MaxNonsortingVarintLen)
	rng, _ := randutil.NewTestRand()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bytes = EncodeNonsortingStdlibVarint(bytes, rng.Int63())
	}
}

func BenchmarkDecodeNonsortingVarint(b *testing.B) {
	buf := make([]byte, 0, b.N*MaxNonsortingVarintLen)
	rng, _ := randutil.NewTestRand()
	for i := 0; i < b.N; i++ {
		buf = EncodeNonsortingStdlibVarint(buf, rng.Int63())
	}
	var err error
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _, _, err = DecodeNonsortingStdlibVarint(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// edgeCaseUint64s returns some uint64 edge cases for encodings. Currently:
// - every power of two
// - every power of two -1 and +1
func edgeCaseUint64s() []uint64 {
	values := []uint64{0, 1, 2}
	for i := uint(2); i < 64; i++ {
		x := uint64(1) << i
		values = append(values, x-1, x, x+1)
	}
	values = append(values, math.MaxUint64)
	return values
}

// randPowDistributedInt63s returns the requested number of int63s such that the
// logarithm of the results is evenly distributed.
func randPowDistributedInt63s(rng *rand.Rand, count int) []int64 {
	values := make([]int64, count)
	for i := range values {
		// 1 << 62 is the largest number that fits in an int63 and 0 digits is
		// not meaningful.
		digits := uint(rng.Intn(61)) + 1
		x := rng.Int63n(int64(1) << digits)
		for x>>(digits-1) == 0 {
			// If shifting off digits-1 digits is 0, then we didn't get a big enough
			// number.
			x = rng.Int63n(1 << digits)
		}
		values[i] = x
	}
	return values
}

func testNonsortingUvarint(t *testing.T, i uint64) {
	buf := EncodeNonsortingUvarint(nil, i)
	rem, n, x, err := DecodeNonsortingUvarint(buf)
	if err != nil {
		t.Fatal(err)
	}
	if x != i {
		t.Fatalf("expected %d got %d", i, x)
	}
	if n != len(buf) {
		t.Fatalf("expected length %d got %d", len(buf), n)
	}
	if len(rem) != 0 {
		t.Fatalf("expected no remaining bytes got %d", len(rem))
	}
}

func TestNonsortingUVarint(t *testing.T) {
	rng, _ := randutil.NewTestRand()

	for _, test := range edgeCaseUint64s() {
		testNonsortingUvarint(t, test)
	}
	for _, test := range randPowDistributedInt63s(rng, 1000) {
		testNonsortingUvarint(t, uint64(test))
	}
}

func TestPeekLengthNonsortingUVarint(t *testing.T) {
	rng, seed := randutil.NewTestRand()

	var buf []byte
	var lengths []int
	for _, test := range edgeCaseUint64s() {
		length := len(buf)
		buf = EncodeNonsortingUvarint(buf, test)
		lengths = append(lengths, len(buf)-length)
	}
	for _, test := range randPowDistributedInt63s(rng, 1000) {
		length := len(buf)
		buf = EncodeNonsortingUvarint(buf, uint64(test))
		lengths = append(lengths, len(buf)-length)
	}

	for _, length := range lengths {
		l := PeekLengthNonsortingUvarint(buf)
		if l != length {
			t.Fatalf("seed %d: got %d expected %d: %x", seed, l, length, buf[:length])
		}
		buf = buf[l:]
	}
	if l := PeekLengthNonsortingUvarint(buf); l != 0 {
		t.Fatalf("expected 0 for empty buffer got %d", l)
	}
}

func BenchmarkEncodeNonsortingUvarint(b *testing.B) {
	buf := make([]byte, 0, b.N*MaxNonsortingUvarintLen)
	rng, _ := randutil.NewTestRand()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = EncodeNonsortingUvarint(buf, uint64(rng.Int63()))
	}
}

func BenchmarkDecodeNonsortingUvarint(b *testing.B) {
	buf := make([]byte, 0, b.N*MaxNonsortingUvarintLen)
	rng, _ := randutil.NewTestRand()
	for i := 0; i < b.N; i++ {
		buf = EncodeNonsortingUvarint(buf, uint64(rng.Int63()))
	}
	var err error
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _, _, err = DecodeNonsortingUvarint(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeOneByteNonsortingUvarint(b *testing.B) {
	buf := make([]byte, 0, b.N*MaxNonsortingUvarintLen)
	rng, _ := randutil.NewTestRand()
	for i := 0; i < b.N; i++ {
		buf = EncodeNonsortingUvarint(buf, uint64(rng.Int63()%(1<<7)))
	}
	var err error
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _, _, err = DecodeNonsortingUvarint(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPeekLengthNonsortingUvarint(b *testing.B) {
	buf := make([]byte, 0, b.N*MaxNonsortingUvarintLen)
	rng, _ := randutil.NewTestRand()
	for i := 0; i < b.N; i++ {
		buf = EncodeNonsortingUvarint(buf, uint64(rng.Int63()))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l := PeekLengthNonsortingUvarint(buf)
		buf = buf[l:]
	}
}

// randValueEncode "value" encodes a random value for the specified Type into
// buf. It returns true if there was a matching encode method and false if there
// wasn't, in which case buf is left unchanged.
func randValueEncode(rd randData, buf []byte, colID uint32, typ Type) ([]byte, interface{}, bool) {
	switch typ {
	case Null:
		return EncodeNullValue(buf, colID), nil, true
	case True:
		return EncodeBoolValue(buf, colID, true), true, true
	case False:
		return EncodeBoolValue(buf, colID, false), false, true
	case Int:
		x := rd.Int63()
		return EncodeIntValue(buf, colID, x), x, true
	case Float:
		x := rd.NormFloat64()
		return EncodeFloatValue(buf, colID, x), x, true
	case Decimal:
		x := rd.decimal()
		return EncodeDecimalValue(buf, colID, x), *x, true
	case Bytes:
		x := randutil.RandBytes(rd.Rand, 100)
		return EncodeBytesValue(buf, colID, x), x, true
	case Time:
		x := rd.time()
		return EncodeTimeValue(buf, colID, x), x, true
	case TimeTZ:
		x := rd.timetz()
		return EncodeTimeTZValue(buf, colID, x), x, true
	case Duration:
		x := rd.duration()
		return EncodeDurationValue(buf, colID, x), x, true
	case BitArray:
		x := rd.bitArray()
		return EncodeBitArrayValue(buf, colID, x), x, true
	case IPAddr:
		x := rd.ipAddr()
		return EncodeIPAddrValue(buf, colID, x), x, true
	default:
		return buf, nil, false
	}
}

func TestValueEncodingPeekLength(t *testing.T) {
	rng, seed := randutil.NewTestRand()
	rd := randData{rng}

	var buf []byte
	var lengths []int
	for i := 0; i < 1000; {
		lastLen := len(buf)
		var ok bool
		buf, _, ok = randValueEncode(rd, buf, uint32(rng.Int63()), Type(rng.Intn(int(SentinelType))))
		if ok {
			lengths = append(lengths, len(buf)-lastLen)
			i++
		}
	}
	for _, length := range lengths {
		typeOffset, _, _, _, err := DecodeValueTag(buf)
		if err != nil {
			t.Fatal(err)
		}

		_, l, err := PeekValueLength(buf)
		if err != nil {
			t.Fatal(err)
		}
		if l != length {
			t.Fatalf("seed %d: got %d expected %d: %x", seed, l, length, buf[:length])
		}

		// Check that typeOffset bytes can be dropped from the beginning and
		// PeekValueLength still works.
		_, l, err = PeekValueLength(buf[typeOffset:])
		l += typeOffset
		if err != nil {
			t.Fatal(err)
		}
		if l != length {
			t.Fatalf("seed %d: got %d expected %d: %x", seed, l, length, buf[:length])
		}

		buf = buf[l:]
	}
	_, l, err := PeekValueLength(buf)
	if err != nil {
		t.Fatal(err)
	}
	if l != 0 {
		t.Fatalf("expected 0 for empty buffer got %d", l)
	}
}

func TestValueEncodingTags(t *testing.T) {
	rng, seed := randutil.NewTestRand()

	tests := make([]struct {
		colID  uint32
		typ    Type
		length int
	}, 10)

	var buf []byte
	var lastLen int
	for i := 0; i < len(tests); i++ {
		tests[i].colID = uint32(rng.Int63())
		tests[i].typ = Type(rng.Intn(1000))
		buf = EncodeValueTag(buf, tests[i].colID, tests[i].typ)
		tests[i].length = len(buf) - lastLen
		lastLen = len(buf)
	}

	for i, test := range tests {
		typeOffset, dataOffset, colID, typ, err := DecodeValueTag(buf)
		if err != nil {
			t.Fatal(err)
		}
		if colID != test.colID {
			t.Fatalf("%d seed %d: expected colID %d got %d", i, seed, test.colID, colID)
		}
		if typ != test.typ {
			t.Fatalf("%d seed %d: expected type %s got %s", i, seed, test.typ, typ)
		}
		if dataOffset != test.length {
			t.Fatalf("%d seed %d: expected length %d got %d", i, seed, test.length, dataOffset)
		}

		// Check that typeOffset bytes can be dropped from the beginning and
		// everything but colID still works.
		_, dataOffset, _, typ, err = DecodeValueTag(buf[typeOffset:])
		dataOffset += typeOffset
		if err != nil {
			t.Fatal(err)
		}
		if typ != test.typ {
			t.Fatalf("%d seed %d: expected type %s got %s", i, seed, test.typ, typ)
		}
		if dataOffset != test.length {
			t.Fatalf("%d seed %d: expected length %d got %d", i, seed, test.length, dataOffset)
		}

		buf = buf[dataOffset:]
	}
}

func TestValueEncodingRand(t *testing.T) {
	rng, seed := randutil.NewTestRand()
	rd := randData{rng}

	var buf []byte
	var values []interface{}
	for i := 0; i < 1000; {
		var value interface{}
		var ok bool
		buf, value, ok = randValueEncode(rd, buf, uint32(rng.Int63()), Type(rng.Intn(int(SentinelType))))
		if ok {
			values = append(values, value)
			i++
		}
	}
	for _, value := range values {
		_, dataOffset, _, typ, err := DecodeValueTag(buf)
		if err != nil {
			t.Fatal(err)
		}

		var decoded interface{}
		switch typ {
		case Null:
			buf = buf[dataOffset:]
		case True:
			buf, decoded, err = DecodeBoolValue(buf)
		case False:
			buf, decoded, err = DecodeBoolValue(buf)
		case Int:
			buf, decoded, err = DecodeIntValue(buf)
		case Float:
			buf, decoded, err = DecodeFloatValue(buf)
		case Decimal:
			buf, decoded, err = DecodeDecimalValue(buf)
		case Bytes:
			buf, decoded, err = DecodeBytesValue(buf)
		case Time:
			buf, decoded, err = DecodeTimeValue(buf)
		case TimeTZ:
			buf, decoded, err = DecodeTimeTZValue(buf)
		case Duration:
			buf, decoded, err = DecodeDurationValue(buf)
		case BitArray:
			buf, decoded, err = DecodeBitArrayValue(buf)
		case IPAddr:
			buf, decoded, err = DecodeIPAddrValue(buf)
		default:
			err = errors.Errorf("unknown type %s", typ)
		}
		if err != nil {
			t.Fatal(err)
		}

		switch typ {
		case Bytes:
			if !bytes.Equal(decoded.([]byte), value.([]byte)) {
				t.Fatalf("seed %d: %s got %x expected %x", seed, typ, decoded.([]byte), value.([]byte))
			}
		case Decimal:
			d := decoded.(apd.Decimal)
			val := value.(apd.Decimal)
			if d.Cmp(&val) != 0 {
				t.Fatalf("seed %d: %s got %v expected %v", seed, typ, decoded, value)
			}
		case IPAddr:
			d := decoded.(ipaddr.IPAddr)
			val := value.(ipaddr.IPAddr)
			if !d.Equal(&val) {
				t.Fatalf("seed %d: %s got %v expected %v", seed, typ, decoded, value)
			}
		case BitArray:
			d := decoded.(bitarray.BitArray)
			val := value.(bitarray.BitArray)
			if bitarray.Compare(d, val) != 0 {
				t.Fatalf("seed %d: %s got %v expected %v", seed, typ, decoded, value)
			}
		default:
			if decoded != value {
				t.Fatalf("seed %d: %s got %v expected %v", seed, typ, decoded, value)
			}
		}
	}
}

func TestPrettyPrintValueEncoded(t *testing.T) {
	uuidStr := "63616665-6630-3064-6465-616462656562"
	u, err := uuid.FromString(uuidStr)
	if err != nil {
		t.Fatalf("Bad test case. Attempted uuid.FromString(%q) got err: %d", uuidStr, err)
	}
	ip := "192.168.0.1/10"
	var ipAddr ipaddr.IPAddr
	err = ipaddr.ParseINet(ip, &ipAddr)
	if err != nil {
		t.Fatalf("Bad test case. Attempted ipaddr.ParseINet(%q) got err: %d", ip, err)
	}
	ba := bitarray.MakeBitArrayFromInt64(6, 9, 5)
	tests := []struct {
		buf      []byte
		expected string
	}{
		{EncodeNullValue(nil, NoColumnID), "NULL"},
		{EncodeBoolValue(nil, NoColumnID, true), "true"},
		{EncodeBoolValue(nil, NoColumnID, false), "false"},
		{EncodeIntValue(nil, NoColumnID, 7), "7"},
		{EncodeFloatValue(nil, NoColumnID, 6.28), "6.28"},
		{EncodeDecimalValue(nil, NoColumnID, apd.New(628, -2)), "6.28"},
		{EncodeTimeValue(nil, NoColumnID,
			time.Date(2016, 6, 29, 16, 2, 50, 5, time.UTC)), "2016-06-29T16:02:50.000000005Z"},
		{EncodeTimeTZValue(nil, NoColumnID,
			timetz.MakeTimeTZ(timeofday.New(10, 11, 12, 0), 5*60*60+24)), "10:11:12-05:00:24"},
		{EncodeDurationValue(nil, NoColumnID,
			duration.DecodeDuration(1, 2, 3)), "1 mon 2 days 00:00:00+3ns"},
		{EncodeBytesValue(nil, NoColumnID, []byte{0x1, 0x2, 0xF, 0xFF}), "0x01020fff"},
		{EncodeBytesValue(nil, NoColumnID, []byte("foo")), "foo"}, // printable bytes
		{EncodeBytesValue(nil, NoColumnID, []byte{0x89}), "0x89"}, // non-printable bytes
		{EncodeIPAddrValue(nil, NoColumnID, ipAddr), ip},
		{EncodeUUIDValue(nil, NoColumnID, u), uuidStr},
		{EncodeBitArrayValue(nil, NoColumnID, ba), "B001001"},
	}
	for i, test := range tests {
		remaining, str, err := PrettyPrintValueEncoded(test.buf)
		if err != nil {
			t.Fatal(err)
		}
		if len(remaining) != 0 {
			t.Errorf("%d: expected all bytes to be consumed but was left with %s", i, remaining)
		}
		if str != test.expected {
			t.Errorf("%d: got %q expected %q", i, str, test.expected)
		}
	}
}

func BenchmarkEncodeBoolValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	rd := randData{rng}

	vals := make([]bool, 10000)
	for i := range vals {
		vals[i] = rd.bool()
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeBoolValue(buf, NoColumnID, vals[i%len(vals)])
	}
}

func BenchmarkDecodeBoolValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	rd := randData{rng}

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeBoolValue(nil, uint32(rng.Intn(100)), rd.bool())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := DecodeBoolValue(vals[i%len(vals)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeIntValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()

	vals := make([]int64, 10000)
	for i := range vals {
		vals[i] = rng.Int63()
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeIntValue(buf, NoColumnID, vals[i%len(vals)])
	}
}

func BenchmarkDecodeIntValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeIntValue(nil, uint32(rng.Intn(100)), rng.Int63())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := DecodeIntValue(vals[i%len(vals)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeFloatValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()

	vals := make([]float64, 10000)
	for i := range vals {
		vals[i] = rng.NormFloat64()
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeFloatValue(buf, NoColumnID, vals[i%len(vals)])
	}
}

func BenchmarkDecodeFloatValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeFloatValue(nil, uint32(rng.Intn(100)), rng.NormFloat64())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := DecodeFloatValue(vals[i%len(vals)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeBytesValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = randutil.RandBytes(rng, 100)
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeBytesValue(buf, NoColumnID, vals[i%len(vals)])
	}
}

func BenchmarkDecodeBytesValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeBytesValue(nil, uint32(rng.Intn(100)), randutil.RandBytes(rng, 100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := DecodeBytesValue(vals[i%len(vals)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeTimeValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	rd := randData{rng}

	vals := make([]time.Time, 10000)
	for i := range vals {
		vals[i] = rd.time()
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeTimeValue(buf, NoColumnID, vals[i%len(vals)])
	}
}

func BenchmarkDecodeTimeValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	rd := randData{rng}

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeTimeValue(nil, uint32(rng.Intn(100)), rd.time())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := DecodeTimeValue(vals[i%len(vals)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeTimeTZValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	rd := randData{rng}

	vals := make([]timetz.TimeTZ, 10000)
	for i := range vals {
		vals[i] = rd.timetz()
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeTimeTZValue(buf, NoColumnID, vals[i%len(vals)])
	}
}

func BenchmarkDecodeTimeTZValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	rd := randData{rng}

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeTimeTZValue(nil, uint32(rng.Intn(100)), rd.timetz())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := DecodeTimeTZValue(vals[i%len(vals)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeIPAddrValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	rd := randData{rng}

	vals := make([]ipaddr.IPAddr, 10000)
	for i := range vals {
		vals[i] = rd.ipAddr()
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeIPAddrValue(buf, NoColumnID, vals[i%len(vals)])
	}
}

func BenchmarkDecodeIPAddrValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	rd := randData{rng}

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeIPAddrValue(nil, uint32(rng.Intn(100)), rd.ipAddr())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := DecodeIPAddrValue(vals[i%len(vals)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeDecimalValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	rd := randData{rng}

	vals := make([]*apd.Decimal, 10000)
	for i := range vals {
		vals[i] = rd.decimal()
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeDecimalValue(buf, NoColumnID, vals[i%len(vals)])
	}
}

func BenchmarkDecodeDecimalValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	rd := randData{rng}

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeDecimalValue(nil, uint32(rng.Intn(100)), rd.decimal())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := DecodeDecimalValue(vals[i%len(vals)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeDurationValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	rd := randData{rng}

	vals := make([]duration.Duration, 10000)
	for i := range vals {
		vals[i] = rd.duration()
	}

	buf := make([]byte, 0, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeDurationValue(buf, NoColumnID, vals[i%len(vals)])
	}
}

func BenchmarkDecodeDurationValue(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	rd := randData{rng}

	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeDurationValue(nil, uint32(rng.Intn(100)), rd.duration())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := DecodeDurationValue(vals[i%len(vals)]); err != nil {
			b.Fatal(err)
		}
	}
}
