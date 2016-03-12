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
	"math"
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

func TestEncodeDecimal(t *testing.T) {
	testCases := []struct {
		Value    *inf.Dec
		Encoding []byte
	}{
		{decimal.NewDecFromFloat(-math.MaxFloat64), []byte{0x04, 0x64, 0xfc, 0x60, 0x66, 0x44, 0xe4, 0x9e, 0x82, 0xc0, 0x8d, 0x0}},
		{inf.NewDec(-1, -308), []byte{0x04, 0x64, 0xfd, 0x0}},
		// Four duplicates to make sure -1*10^4 <= -10*10^3 <= -100*10^2 <= -1*10^4
		{inf.NewDec(-1, -4), []byte{0x0c, 0xfd, 0x0}},
		{inf.NewDec(-10, -3), []byte{0x0c, 0xfd, 0x0}},
		{inf.NewDec(-100, -2), []byte{0x0c, 0xfd, 0x0}},
		{inf.NewDec(-1, -4), []byte{0x0c, 0xfd, 0x0}},
		{inf.NewDec(-9999, 0), []byte{0x0d, 0x38, 0x39, 0x00}},
		{inf.NewDec(-10, -1), []byte{0x0d, 0xfd, 0x00}},
		{inf.NewDec(-99, 0), []byte{0x0e, 0x39, 0x00}},
		{inf.NewDec(-1, 0), []byte{0x0e, 0xfd, 0x0}},
		{inf.NewDec(-123, 5), []byte{0x10, 0x1, 0xe6, 0xc3, 0x0}},
		{inf.NewDec(-1, 307), []byte{0x10, 0x99, 0xeb, 0x0}},
		{decimal.NewDecFromFloat(-math.SmallestNonzeroFloat64), []byte{0x10, 0xa1, 0xf5, 0x0}},
		{inf.NewDec(0, 0), []byte{0x11}},
		{decimal.NewDecFromFloat(math.SmallestNonzeroFloat64), []byte{0x12, 0x5e, 0xa, 0x0}},
		{inf.NewDec(1, 307), []byte{0x12, 0x66, 0x14, 0x0}},
		{inf.NewDec(123, 5), []byte{0x12, 0xfe, 0x19, 0x3c, 0x0}},
		{inf.NewDec(123, 4), []byte{0x13, 0x03, 0x2e, 0x0}},
		{inf.NewDec(123, 3), []byte{0x13, 0x19, 0x3c, 0x0}},
		{inf.NewDec(1, 0), []byte{0x14, 0x02, 0x0}},
		{inf.NewDec(1, -1), []byte{0x14, 0x14, 0x0}},
		{inf.NewDec(12345, 3), []byte{0x14, 0x19, 0x45, 0x64, 0x0}},
		{inf.NewDec(990, 1), []byte{0x14, 0xc6, 0x0}},
		{inf.NewDec(990001, 4), []byte{0x14, 0xc7, 0x01, 0x02, 0x0}},
		{inf.NewDec(9901, 2), []byte{0x14, 0xc7, 0x02, 0x0}},
		{inf.NewDec(10, -1), []byte{0x15, 0x02, 0x0}},
		{inf.NewDec(10001, 2), []byte{0x15, 0x03, 0x01, 0x02, 0x0}},
		{inf.NewDec(1001, 1), []byte{0x15, 0x03, 0x01, 0x14, 0x0}},
		{inf.NewDec(1234, 0), []byte{0x15, 0x19, 0x44, 0x0}},
		{inf.NewDec(12345, 1), []byte{0x15, 0x19, 0x45, 0x64, 0x0}},
		{inf.NewDec(9999, 0), []byte{0x15, 0xc7, 0xc6, 0x0}},
		{inf.NewDec(9999000001, 6), []byte{0x15, 0xc7, 0xc7, 0x01, 0x01, 0x02, 0x0}},
		{inf.NewDec(9999000009, 6), []byte{0x15, 0xc7, 0xc7, 0x01, 0x01, 0x12, 0x0}},
		{inf.NewDec(9999000010, 6), []byte{0x15, 0xc7, 0xc7, 0x01, 0x01, 0x14, 0x0}},
		{inf.NewDec(9999000090, 6), []byte{0x15, 0xc7, 0xc7, 0x01, 0x01, 0xb4, 0x0}},
		{inf.NewDec(9999000099, 6), []byte{0x15, 0xc7, 0xc7, 0x01, 0x01, 0xc6, 0x0}},
		{inf.NewDec(99990001, 4), []byte{0x15, 0xc7, 0xc7, 0x01, 0x02, 0x0}},
		{inf.NewDec(9999001, 3), []byte{0x15, 0xc7, 0xc7, 0x01, 0x14, 0x0}},
		{inf.NewDec(999901, 2), []byte{0x15, 0xc7, 0xc7, 0x02, 0x0}},
		{inf.NewDec(99991, 1), []byte{0x15, 0xc7, 0xc7, 0x14, 0x0}},
		{inf.NewDec(10000, 0), []byte{0x16, 0x02, 0x0}},
		{inf.NewDec(10001, 0), []byte{0x16, 0x03, 0x01, 0x02, 0x0}},
		{inf.NewDec(12345, 0), []byte{0x16, 0x03, 0x2f, 0x5a, 0x0}},
		{inf.NewDec(123450, 0), []byte{0x16, 0x19, 0x45, 0x64, 0x0}},
		{inf.NewDec(1, -308), []byte{0x1e, 0x9b, 0x2, 0x0}},
		{decimal.NewDecFromFloat(math.MaxFloat64), []byte{0x1e, 0x9b, 0x3, 0x9f, 0x99, 0xbb, 0x1b, 0x61, 0x7d, 0x3f, 0x72, 0x0}},
	}

	var lastEncoded []byte
	for _, tmp := range [][]byte{nil, make([]byte, 0, 100)} {
		tmp = tmp[:0]
		for _, dir := range []Direction{Ascending, Descending} {
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
