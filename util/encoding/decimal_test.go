// Copyright 2016 The Cockroach Authors.
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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package encoding

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/util/decimal"
	"github.com/cockroachdb/cockroach/util/randutil"
)

func TestDecimalMandE(t *testing.T) {
	testCases := []struct {
		Value string
		E     int
		M     []byte
	}{
		{"1.0", 1, []byte{0x02}},
		{"10.0", 1, []byte{0x14}},
		{"99.0", 1, []byte{0xc6}},
		{"99.01", 1, []byte{0xc7, 0x02}},
		{"99.0001", 1, []byte{0xc7, 0x01, 0x02}},
		{"100.0", 2, []byte{0x02}},
		{"100.01", 2, []byte{0x03, 0x01, 0x02}},
		{"100.1", 2, []byte{0x03, 0x01, 0x14}},
		{"1234", 2, []byte{0x19, 0x44}},
		{"9999", 2, []byte{0xc7, 0xc6}},
		{"9999.000001", 2, []byte{0xc7, 0xc7, 0x01, 0x01, 0x02}},
		{"9999.000009", 2, []byte{0xc7, 0xc7, 0x01, 0x01, 0x12}},
		{"9999.00001", 2, []byte{0xc7, 0xc7, 0x01, 0x01, 0x14}},
		{"9999.00009", 2, []byte{0xc7, 0xc7, 0x01, 0x01, 0xb4}},
		{"9999.000099", 2, []byte{0xc7, 0xc7, 0x01, 0x01, 0xc6}},
		{"9999.0001", 2, []byte{0xc7, 0xc7, 0x01, 0x02}},
		{"9999.001", 2, []byte{0xc7, 0xc7, 0x01, 0x14}},
		{"9999.01", 2, []byte{0xc7, 0xc7, 0x02}},
		{"9999.1", 2, []byte{0xc7, 0xc7, 0x14}},
		{"10000", 3, []byte{0x02}},
		{"10001", 3, []byte{0x03, 0x01, 0x02}},
		{"12345", 3, []byte{0x03, 0x2f, 0x5a}},
		{"123450", 3, []byte{0x19, 0x45, 0x64}},
		{"1234.5", 2, []byte{0x19, 0x45, 0x64}},
		{"12.345", 1, []byte{0x19, 0x45, 0x64}},
		{"0.123", 0, []byte{0x19, 0x3c}},
		{"0.0123", 0, []byte{0x03, 0x2e}},
		{"0.00123", -1, []byte{0x19, 0x3c}},
		{"1e-307", -153, []byte{0x14}},
		{"1e308", 155, []byte{0x2}},
		{"9223372036854775807", 10, []byte{0x13, 0x2d, 0x43, 0x91, 0x07, 0x89, 0x6d, 0x9b, 0x75, 0x0e}},
	}
	for _, c := range testCases {
		// Deal with the issue that Dec.SetString can't handle exponent notation.
		scale := 0
		e := strings.IndexRune(c.Value, 'e')
		if e != -1 {
			var err error
			scale, err = strconv.Atoi(c.Value[e+1:])
			if err != nil {
				t.Fatalf("could not parse value's exponent: %v", err)
			}
			c.Value = c.Value[:e]
		}

		d := new(inf.Dec)
		if _, ok := d.SetString(c.Value); !ok {
			t.Fatalf("could not parse decimal from string %q", c.Value)
		}
		if scale != 0 {
			d.SetScale(d.Scale() - inf.Scale(scale))
		}

		if e, m := decimalMandE(d, nil); e != c.E || !bytes.Equal(m, c.M) {
			t.Errorf("unexpected mismatch in E/M for %v. expected E=%v | M=[% x], got E=%v | M=[% x]",
				c.Value, c.E, c.M, e, m)
		}
	}
}

func mustDecimal(s string) *inf.Dec {
	d, ok := new(inf.Dec).SetString(s)
	if !ok {
		panic(fmt.Sprintf("could not set string %q on decimal", s))
	}
	return d
}

func TestEncodeDecimal(t *testing.T) {
	testCases := []struct {
		Value    *inf.Dec
		Encoding []byte
	}{
		{inf.NewDec(-99122, -99999), []byte{0x1a, 0x86, 0x3c, 0xad, 0x38, 0xe6, 0xd7, 0x00}},
		// Three duplicates to make sure -13*10^1000 <= -130*10^999 <= -13*10^1000
		{inf.NewDec(-13, -1000), []byte{0x1a, 0x86, 0xfe, 0x0a, 0xe5, 0x00}},
		{inf.NewDec(-130, -999), []byte{0x1a, 0x86, 0xfe, 0x0a, 0xe5, 0x00}},
		{inf.NewDec(-13, -1000), []byte{0x1a, 0x86, 0xfe, 0x0a, 0xe5, 0x00}},
		{decimal.NewDecFromFloat(-math.MaxFloat64), []byte{0x1a, 0x87, 0x64, 0xfc, 0x60, 0x66, 0x44, 0xe4, 0x9e, 0x82, 0xc0, 0x8d, 0x00}},
		{inf.NewDec(-130, -100), []byte{0x1a, 0x87, 0xcb, 0xfc, 0xc3, 0x00}},
		{inf.NewDec(-13, 0), []byte{0x24, 0xe5, 0x00}},
		{inf.NewDec(-11, 0), []byte{0x24, 0xe9, 0x00}},
		{mustDecimal("-10.123456789"), []byte{0x24, 0xea, 0xe6, 0xba, 0x8e, 0x62, 0x4b, 0x00}},
		{mustDecimal("-10"), []byte{0x24, 0xeb, 0x00}},
		{mustDecimal("-9.123456789"), []byte{0x24, 0xec, 0xe6, 0xba, 0x8e, 0x62, 0x4b, 0x00}},
		{mustDecimal("-9"), []byte{0x24, 0xed, 0x00}},
		{mustDecimal("-1.1"), []byte{0x24, 0xfc, 0xeb, 0x00}},
		{inf.NewDec(-1, 0), []byte{0x24, 0xfd, 0x00}},
		{inf.NewDec(-8, 1), []byte{0x25, 0x5f, 0x00}},
		{inf.NewDec(-1, 1), []byte{0x25, 0xeb, 0x00}},
		{mustDecimal("-.09"), []byte{0x25, 0xed, 0x00}},
		{mustDecimal("-.054321"), []byte{0x25, 0xf4, 0xa8, 0xd5, 0x00}},
		{mustDecimal("-.012"), []byte{0x25, 0xfc, 0xd7, 0x00}},
		{inf.NewDec(-11, 4), []byte{0x26, 0x89, 0xe9, 0x00}},
		{inf.NewDec(-11, 6), []byte{0x26, 0x8a, 0xe9, 0x00}},
		{decimal.NewDecFromFloat(-math.SmallestNonzeroFloat64), []byte{0x26, 0xf6, 0xa1, 0xf5, 0x00}},
		{inf.NewDec(-11, 66666), []byte{0x26, 0xf7, 0x82, 0x34, 0xe9, 0x00}},
		{inf.NewDec(0, 0), []byte{0x27}},
		{decimal.NewDecFromFloat(math.SmallestNonzeroFloat64), []byte{0x28, 0x87, 0x5e, 0x0a, 0x00}},
		{inf.NewDec(11, 6), []byte{0x28, 0x87, 0xfd, 0x16, 0x00}},
		{inf.NewDec(11, 4), []byte{0x28, 0x87, 0xfe, 0x16, 0x00}},
		{inf.NewDec(1, 1), []byte{0x29, 0x14, 0x00}},
		{inf.NewDec(8, 1), []byte{0x29, 0xa0, 0x00}},
		{inf.NewDec(1, 0), []byte{0x2a, 0x02, 0x00}},
		{mustDecimal("1.1"), []byte{0x2a, 0x03, 0x14, 0x00}},
		{inf.NewDec(11, 0), []byte{0x2a, 0x16, 0x00}},
		{inf.NewDec(13, 0), []byte{0x2a, 0x1a, 0x00}},
		{decimal.NewDecFromFloat(math.MaxFloat64), []byte{0x34, 0xf6, 0x9b, 0x03, 0x9f, 0x99, 0xbb, 0x1b, 0x61, 0x7d, 0x3f, 0x72, 0x00}},
		// Four duplicates to make sure 13*10^1000 <= 130*10^999 <= 1300*10^998 <= 13*10^1000
		{inf.NewDec(13, -1000), []byte{0x34, 0xf7, 0x01, 0xf5, 0x1a, 0x00}},
		{inf.NewDec(130, -999), []byte{0x34, 0xf7, 0x01, 0xf5, 0x1a, 0x00}},
		{inf.NewDec(1300, -998), []byte{0x34, 0xf7, 0x01, 0xf5, 0x1a, 0x00}},
		{inf.NewDec(13, -1000), []byte{0x34, 0xf7, 0x01, 0xf5, 0x1a, 0x00}},
		{inf.NewDec(99122, -99999), []byte{0x34, 0xf7, 0xc3, 0x52, 0xc7, 0x19, 0x28, 0x00}},
		{inf.NewDec(99122839898321208, -99999), []byte{0x34, 0xf7, 0xc3, 0x58, 0xc7, 0x19, 0x39, 0x4f, 0xb3, 0xa7, 0x2b, 0x29, 0xa0, 0x00}},
	}

	var lastEncoded []byte
	for _, dir := range []Direction{Ascending, Descending} {
		for _, tmp := range [][]byte{nil, make([]byte, 0, 100)} {
			for i, c := range testCases {
				var enc []byte
				var err error
				var dec *inf.Dec
				if dir == Ascending {
					enc = EncodeDecimalAscending(nil, c.Value)
					_, dec, err = DecodeDecimalAscending(enc, tmp)
				} else {
					enc = EncodeDecimalDescending(nil, c.Value)
					_, dec, err = DecodeDecimalDescending(enc, tmp)
				}
				if dir == Ascending && !bytes.Equal(enc, c.Encoding) {
					t.Errorf("unexpected mismatch for %s. expected [% x], got [% x]",
						c.Value, c.Encoding, enc)
				}
				if i > 0 {
					if (bytes.Compare(lastEncoded, enc) > 0 && dir == Ascending) ||
						(bytes.Compare(lastEncoded, enc) < 0 && dir == Descending) {
						t.Errorf("%v: expected [% x] to be less than or equal to [% x]",
							c.Value, testCases[i-1].Encoding, enc)
					}
				}
				if err != nil {
					t.Error(err)
					continue
				}
				if dec.Cmp(c.Value) != 0 {
					t.Errorf("%d unexpected mismatch for %v. got %v", i, c.Value, dec)
				}
				lastEncoded = enc
			}

			// Test that appending the decimal to an existing buffer works.
			var enc []byte
			var dec *inf.Dec
			other := inf.NewDec(123, 2)
			if dir == Ascending {
				enc = EncodeDecimalAscending([]byte("hello"), other)
				_, dec, _ = DecodeDecimalAscending(enc[5:], tmp)
			} else {
				enc = EncodeDecimalDescending([]byte("hello"), other)
				_, dec, _ = DecodeDecimalDescending(enc[5:], tmp)
			}
			if dec.Cmp(other) != 0 {
				t.Errorf("unexpected mismatch for %v. got %v", 1.23, other)
			}
		}
	}
}

func TestEncodeDecimalRand(t *testing.T) {
	// Test both directions.
	for _, dir := range []Direction{Ascending, Descending} {
		var prev *inf.Dec
		var prevEnc []byte
		// Test with and without tmp buffer.
		for _, tmp := range [][]byte{nil, make([]byte, 0, 100)} {
			// Test with and without appending buffer.
			for _, append := range [][]byte{nil, []byte("hello")} {
				const randomTrials = 50000
				for i := 0; i < randomTrials; i++ {
					cur := decimal.NewDecFromFloat(rand.Float64())

					var enc []byte
					var res *inf.Dec
					var err error
					if dir == Ascending {
						enc = EncodeDecimalAscending(append, cur)
						enc = enc[len(append):]
						_, res, err = DecodeDecimalAscending(enc, tmp)
					} else {
						enc = EncodeDecimalDescending(append, cur)
						enc = enc[len(append):]
						_, res, err = DecodeDecimalDescending(enc, tmp)
					}
					if err != nil {
						t.Fatal(err)
					}

					// Make sure we decode the same value we encoded.
					if cur.Cmp(res) != 0 {
						t.Fatalf("unexpected mismatch for %v, got %v", cur, res)
					}

					// Make sure lexicographical sorting is consistent.
					if prev != nil {
						bytesCmp := bytes.Compare(prevEnc, enc)
						cmpType := "same"
						if dir == Descending {
							bytesCmp *= -1
							cmpType = "inverse"
						}
						if decCmp := prev.Cmp(cur); decCmp != bytesCmp {
							t.Fatalf("expected [% x] to compare to [% x] the %s way that %v compares to %v",
								prevEnc, enc, cmpType, prev, cur)
						}
					}
					prev = cur
					prevEnc = enc
				}
			}
		}
	}
}

func TestNonsortingEncodeDecimal(t *testing.T) {
	testCases := []struct {
		Value    *inf.Dec
		Encoding []byte
	}{
		{inf.NewDec(-99122, -99999), []byte{0x1a, 0xf8, 0x01, 0x86, 0xa4, 0x01, 0x83, 0x32}},
		// Three duplicates to make sure -13*10^1000 <= -130*10^999 <= -13*10^1000
		{inf.NewDec(-13, -1000), []byte{0x1a, 0xf7, 0x03, 0xea, 0x0d}},
		{inf.NewDec(-130, -999), []byte{0x1a, 0xf7, 0x03, 0xea, 0x0d}},
		{inf.NewDec(-13, -1000), []byte{0x1a, 0xf7, 0x03, 0xea, 0x0d}},
		{decimal.NewDecFromFloat(-math.MaxFloat64), []byte{0x1a, 0xf7, 0x01, 0x35, 0x3f, 0xdd, 0xec, 0x7f, 0x2f, 0xaf, 0x35}},
		{inf.NewDec(-130, -100), []byte{0x1a, 0xef, 0x0d}},
		{inf.NewDec(-13, 0), []byte{0x1a, 0x8a, 0x0d}},
		{inf.NewDec(-11, 0), []byte{0x1a, 0x8a, 0x0b}},
		{inf.NewDec(-1, 0), []byte{0x1a, 0x89, 0x01}},
		{inf.NewDec(-8, 1), []byte{0x25, 0x08}},
		{inf.NewDec(-1, 1), []byte{0x25, 0x01}},
		{inf.NewDec(-11, 4), []byte{0x26, 0x8a, 0x0b}},
		{inf.NewDec(-11, 6), []byte{0x26, 0x8c, 0x0b}},
		{decimal.NewDecFromFloat(-math.SmallestNonzeroFloat64), []byte{0x26, 0xf7, 0x01, 0x43, 0x05}},
		{inf.NewDec(-11, 66666), []byte{0x26, 0xf8, 0x01, 0x04, 0x68, 0x0b}},
		{inf.NewDec(0, 0), []byte{0x27}},
		{decimal.NewDecFromFloat(math.SmallestNonzeroFloat64), []byte{0x28, 0xf7, 0x01, 0x43, 0x05}},
		{inf.NewDec(11, 6), []byte{0x28, 0x8c, 0x0b}},
		{inf.NewDec(11, 4), []byte{0x28, 0x8a, 0x0b}},
		{inf.NewDec(1, 1), []byte{0x29, 0x01}},
		{inf.NewDec(12345, 5), []byte{0x29, 0x30, 0x39}},
		{inf.NewDec(8, 1), []byte{0x29, 0x08}},
		{inf.NewDec(1, 0), []byte{0x34, 0x89, 0x01}},
		{inf.NewDec(11, 0), []byte{0x34, 0x8a, 0x0b}},
		{inf.NewDec(13, 0), []byte{0x34, 0x8a, 0x0d}},
		// Note that this does not sort correctly!
		{inf.NewDec(255, 0), []byte{0x34, 0x8b, 0xff}},
		{inf.NewDec(256, 0), []byte{0x34, 0x8b, 0x01, 0x00}},
		{decimal.NewDecFromFloat(math.MaxFloat64), []byte{0x34, 0xf7, 0x01, 0x35, 0x3f, 0xdd, 0xec, 0x7f, 0x2f, 0xaf, 0x35}},
		// Four duplicates to make sure 13*10^1000 <= 130*10^999 <= 1300*10^998 <= 13*10^1000
		{inf.NewDec(13, -1000), []byte{0x34, 0xf7, 0x03, 0xea, 0x0d}},
		{inf.NewDec(130, -999), []byte{0x34, 0xf7, 0x03, 0xea, 0x0d}},
		{inf.NewDec(1300, -998), []byte{0x34, 0xf7, 0x03, 0xea, 0x0d}},
		{inf.NewDec(13, -1000), []byte{0x34, 0xf7, 0x03, 0xea, 0x0d}},
		{inf.NewDec(99122, -99999), []byte{0x34, 0xf8, 0x01, 0x86, 0xa4, 0x01, 0x83, 0x32}},
		{inf.NewDec(99122839898321208, -99999), []byte{0x34, 0xf8, 0x01, 0x86, 0xb0, 0x01, 0x60, 0x27, 0xb2, 0x9d, 0x44, 0x71, 0x38}},
	}

	for _, tmp := range [][]byte{nil, make([]byte, 0, 100)} {
		for i, c := range testCases {
			enc := EncodeNonsortingDecimal(nil, c.Value)
			dec, err := DecodeNonsortingDecimal(enc, tmp)
			if err != nil {
				t.Error(err)
				continue
			}
			if !bytes.Equal(enc, c.Encoding) {
				t.Errorf("unexpected mismatch for %s. expected [% x], got [% x]",
					c.Value, c.Encoding, enc)
			}
			if dec.Cmp(c.Value) != 0 {
				t.Errorf("%d unexpected mismatch for %v. got %v", i, c.Value, dec)
			}
		}

		// Test that appending the decimal to an existing buffer works.
		other := inf.NewDec(123, 2)
		enc := EncodeNonsortingDecimal([]byte("hello"), other)
		dec, err := DecodeNonsortingDecimal(enc[5:], tmp)
		if err != nil {
			t.Error(err)
			continue
		}
		if dec.Cmp(other) != 0 {
			t.Errorf("unexpected mismatch for %v. got %v", 1.23, other)
		}
	}
}

func TestNonsortingEncodeDecimalRand(t *testing.T) {
	// Test with and without tmp buffer.
	for _, tmp := range [][]byte{nil, make([]byte, 0, 100)} {
		// Test with and without appending buffer.
		for _, append := range [][]byte{nil, []byte("hello")} {
			const randomTrials = 200000
			for i := 0; i < randomTrials; i++ {
				cur := decimal.NewDecFromFloat(rand.Float64())

				enc := EncodeNonsortingDecimal(append, cur)
				enc = enc[len(append):]
				res, err := DecodeNonsortingDecimal(enc, tmp)
				if err != nil {
					t.Fatal(err)
				}

				// Make sure we decode the same value we encoded.
				if cur.Cmp(res) != 0 {
					t.Fatalf("unexpected mismatch for %v, got %v", cur, res)
				}

				// Make sure we would have overestimated the value.
				if est := UpperBoundNonsortingDecimalSize(cur); est < len(enc) {
					t.Fatalf("expected estimate of %d for %v to be greater than or equal to the encoded length, found [% x]", est, cur, enc)
				}
			}
		}
	}
}

func TestDigitsLookupTable(t *testing.T) {
	// Make sure all elements in table make sense.
	min := new(big.Int)
	prevBorder := big.NewInt(0)
	for i := 1; i <= tableSize; i++ {
		elem := digitsLookupTable[i]

		min.SetInt64(2)
		min.Exp(min, big.NewInt(int64(i-1)), nil)
		if minLen := len(min.String()); minLen != elem.digits {
			t.Errorf("expected 2^%d to have %d digits, found %d", i, elem.digits, minLen)
		}

		if zeros := strings.Count(elem.border.String(), "0"); zeros != elem.digits {
			t.Errorf("the %d digits for digitsLookupTable[%d] does not agree with the border %v", elem.digits, i, &elem.border)
		}

		if min.Cmp(&elem.border) >= 0 {
			t.Errorf("expected 2^%d = %v to be less than the border, found %v", i-1, min, &elem.border)
		}

		if elem.border.Cmp(prevBorder) > 0 {
			if min.Cmp(prevBorder) <= 0 {
				t.Errorf("expected 2^%d = %v to be greater than or equal to the border, found %v", i-1, min, prevBorder)
			}
			prevBorder = &elem.border
		}
	}

	// Throw random big.Ints at the table and make sure the
	// digit lengths line up.
	const randomTrials = 100
	for i := 0; i < randomTrials; i++ {
		a := big.NewInt(rand.Int63())
		b := big.NewInt(rand.Int63())
		a.Mul(a, b)

		tableDigits, _ := numDigits(a, nil)
		if actualDigits := len(a.String()); actualDigits != tableDigits {
			t.Errorf("expected %d digits for %v, found %d", tableDigits, a, actualDigits)
		}
	}
}

func BenchmarkEncodeDecimal(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]*inf.Dec, 10000)
	for i := range vals {
		vals[i] = decimal.NewDecFromFloat(rng.Float64())
	}

	buf := make([]byte, 0, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeDecimalAscending(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeDecimal(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		d := decimal.NewDecFromFloat(rng.Float64())
		vals[i] = EncodeDecimalAscending(nil, d)
	}

	buf := make([]byte, 0, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeDecimalAscending(vals[i%len(vals)], buf)
	}
}

func BenchmarkNonsortingEncodeDecimal(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]*inf.Dec, 10000)
	for i := range vals {
		vals[i] = decimal.NewDecFromFloat(rng.Float64())
	}

	buf := make([]byte, 0, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeNonsortingDecimal(buf, vals[i%len(vals)])
	}
}

func BenchmarkNonsortingDecodeDecimal(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([][]byte, 10000)
	for i := range vals {
		d := decimal.NewDecFromFloat(rng.Float64())
		vals[i] = EncodeNonsortingDecimal(nil, d)
	}

	buf := make([]byte, 0, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeNonsortingDecimal(vals[i%len(vals)], buf)
	}
}
