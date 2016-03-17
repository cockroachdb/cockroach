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
	"testing"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/util/decimal"
	"github.com/cockroachdb/cockroach/util/randutil"
)

func TestEncodeDecimal(t *testing.T) {
	testCases := []struct {
		Value    *inf.Dec
		Encoding []byte
	}{
		{inf.NewDec(-99122, -99999), []byte{0x09, 0x05, 0xfe, 0x79, 0x5b, 0xfe, 0x7c, 0xcd, 0x00}},
		// Three duplicates to make sure -13*10^1000 <= -130*10^999 <= -13*10^1000
		{inf.NewDec(-13, -1000), []byte{0x09, 0x0c, 0x05, 0xf2, 0x00}},
		{inf.NewDec(-130, -999), []byte{0x09, 0x0c, 0x05, 0xf2, 0x00}},
		{inf.NewDec(-13, -1000), []byte{0x09, 0x0c, 0x05, 0xf2, 0x00}},
		{decimal.NewDecFromFloat(-math.MaxFloat64), []byte{0x09, 0x0e, 0xba, 0xc0, 0x22, 0x13, 0x80, 0xd0, 0x50, 0xca, 0x00}},
		{inf.NewDec(-130, -100), []byte{0x09, 0x98, 0xf2, 0x00}},
		{inf.NewDec(-13, 0), []byte{0x09, 0xfd, 0xf2, 0x00}},
		{inf.NewDec(-11, 0), []byte{0x09, 0xfd, 0xf4, 0x00}},
		{inf.NewDec(-1, 0), []byte{0x09, 0xfe, 0xfe, 0x00}},
		{inf.NewDec(-8, 1), []byte{0x0a, 0xf7, 0x00}},
		{inf.NewDec(-1, 1), []byte{0x0a, 0xfe, 0x00}},
		{inf.NewDec(-11, 4), []byte{0x0b, 0x02, 0xf4, 0x00}},
		{inf.NewDec(-11, 6), []byte{0x0b, 0x04, 0xf4, 0x00}},
		{decimal.NewDecFromFloat(-math.SmallestNonzeroFloat64), []byte{0x0b, 0xf1, 0x53, 0xfa, 0x00}},
		{inf.NewDec(-11, 66666), []byte{0x0b, 0xf9, 0xfb, 0x78, 0xf4, 0x00}},
		{inf.NewDec(0, 0), []byte{0x0c}},
		{decimal.NewDecFromFloat(math.SmallestNonzeroFloat64), []byte{0x0d, 0x0e, 0xac, 0x05, 0x00}},
		{inf.NewDec(11, 6), []byte{0x0d, 0xfb, 0x0b, 0x00}},
		{inf.NewDec(11, 4), []byte{0x0d, 0xfd, 0x0b, 0x00}},
		{inf.NewDec(1, 1), []byte{0x0e, 0x01, 0x00}},
		{inf.NewDec(8, 1), []byte{0x0e, 0x08, 0x00}},
		{inf.NewDec(1, 0), []byte{0x0f, 0x01, 0x01, 0x00}},
		{inf.NewDec(11, 0), []byte{0x0f, 0x02, 0x0b, 0x00}},
		{inf.NewDec(13, 0), []byte{0x0f, 0x02, 0x0d, 0x00}},
		{decimal.NewDecFromFloat(math.MaxFloat64), []byte{0x0f, 0xf1, 0x45, 0x3f, 0xdd, 0xec, 0x7f, 0x2f, 0xaf, 0x35, 0x00}},
		// Four duplicates to make sure 13*10^1000 <= 130*10^999 <= 1300*10^998 <= 13*10^1000
		{inf.NewDec(13, -1000), []byte{0x0f, 0xf3, 0xfa, 0x0d, 0x00}},
		{inf.NewDec(130, -999), []byte{0x0f, 0xf3, 0xfa, 0x0d, 0x00}},
		{inf.NewDec(1300, -998), []byte{0x0f, 0xf3, 0xfa, 0x0d, 0x00}},
		{inf.NewDec(13, -1000), []byte{0x0f, 0xf3, 0xfa, 0x0d, 0x00}},
		{inf.NewDec(99122, -99999), []byte{0x0f, 0xfa, 0x01, 0x86, 0xa4, 0x01, 0x83, 0x32, 0x00}},
		{inf.NewDec(99122839898321208, -99999), []byte{0x0f, 0xfa, 0x01, 0x86, 0xb0, 0x01, 0x60, 0x27, 0xb2, 0x9d, 0x44, 0x71, 0x38, 0x00}},
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
