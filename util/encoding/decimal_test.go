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
	"testing"

	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/decimal"
)

func TestEncodeDecimal(t *testing.T) {
	testCases := []struct {
		Value    decimal.Decimal
		Encoding []byte
	}{
		{decimal.New(-99122, 99999), []byte{0x23, 0x05, 0xfe, 0x79, 0x5b, 0xfe, 0x7c, 0xcd, 0x00}},
		// Three duplicates to make sure -13*10^1000 <= -130*10^999 <= -13*10^1000
		{decimal.New(-13, 1000), []byte{0x23, 0x0c, 0x05, 0xf2, 0x00}},
		{decimal.New(-130, 999), []byte{0x23, 0x0c, 0x05, 0xf2, 0x00}},
		{decimal.New(-13, 1000), []byte{0x23, 0x0c, 0x05, 0xf2, 0x00}},
		{decimal.New(-130, 100), []byte{0x23, 0x98, 0xf2, 0x00}},
		{decimal.New(-13, 0), []byte{0x23, 0xfd, 0xf2, 0x00}},
		{decimal.New(-11, 0), []byte{0x23, 0xfd, 0xf4, 0x00}},
		{decimal.New(-1, 0), []byte{0x23, 0xfe, 0xfe, 0x00}},
		{decimal.New(-8, -1), []byte{0x24, 0xf7, 0x00}},
		{decimal.New(-1, -1), []byte{0x24, 0xfe, 0x00}},
		{decimal.New(-11, -4), []byte{0x25, 0x02, 0xf4, 0x00}},
		{decimal.New(-11, -6), []byte{0x25, 0x04, 0xf4, 0x00}},
		{decimal.New(-11, -66666), []byte{0x25, 0xf9, 0xfb, 0x78, 0xf4, 0x00}},
		{decimal.New(0, 0), []byte{0x26}},
		{decimal.New(11, -6), []byte{0x27, 0xfb, 0x0b, 0x00}},
		{decimal.New(11, -4), []byte{0x27, 0xfd, 0x0b, 0x00}},
		{decimal.New(1, -1), []byte{0x28, 0x01, 0x00}},
		{decimal.New(8, -1), []byte{0x28, 0x08, 0x00}},
		{decimal.New(1, 0), []byte{0x29, 0x01, 0x01, 0x00}},
		{decimal.New(11, 0), []byte{0x29, 0x02, 0x0b, 0x00}},
		{decimal.New(13, 0), []byte{0x29, 0x02, 0x0d, 0x00}},
		// Three duplicates to make sure 13*10^1000 <= 130*10^999 <= 13*10^1000
		{decimal.New(13, 1000), []byte{0x29, 0xf3, 0xfa, 0x0d, 0x00}},
		{decimal.New(130, 999), []byte{0x29, 0xf3, 0xfa, 0x0d, 0x00}},
		{decimal.New(13, 1000), []byte{0x29, 0xf3, 0xfa, 0x0d, 0x00}},
		{decimal.New(99122, 99999), []byte{0x29, 0xfa, 0x01, 0x86, 0xa4, 0x01, 0x83, 0x32, 0x00}},
		{decimal.New(99122839898321208, 99999), []byte{0x29, 0xfa, 0x01, 0x86, 0xb0, 0x01, 0x60, 0x27, 0xb2, 0x9d, 0x44, 0x71, 0x38, 0x00}},
	}

	var lastEncoded []byte
	for _, dir := range []Direction{Ascending, Descending} {
		for i, c := range testCases {
			var enc []byte
			var err error
			var dec decimal.Decimal
			if dir == Ascending {
				enc = EncodeDecimalAscending(nil, c.Value)
				_, dec, err = DecodeDecimalAscending(enc, nil)
			} else {
				enc = EncodeDecimalDescending(nil, c.Value)
				_, dec, err = DecodeDecimalDescending(enc, nil)
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
			if !dec.Equals(c.Value) {
				t.Errorf("%d unexpected mismatch for %v. got %v", i, c.Value, dec)
			}
			lastEncoded = enc
		}

		// Test that appending the decimal to an existing buffer works.
		var enc []byte
		var dec decimal.Decimal
		other := decimal.NewFromFloat(1.23)
		if dir == Ascending {
			enc = EncodeDecimalAscending([]byte("hello"), other)
			_, dec, _ = DecodeDecimalAscending(enc[5:], nil)
		} else {
			enc = EncodeDecimalDescending([]byte("hello"), other)
			_, dec, _ = DecodeDecimalDescending(enc[5:], nil)
		}
		if !dec.Equals(other) {
			t.Errorf("unexpected mismatch for %v. got %v", 1.23, other)
		}
	}
}

func BenchmarkEncodeDecimal(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]decimal.Decimal, 10000)
	for i := range vals {
		vals[i] = decimal.NewFromFloat(rng.Float64())
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
		d := decimal.NewFromFloat(rng.Float64())
		vals[i] = EncodeDecimalAscending(nil, d)
	}

	buf := make([]byte, 0, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeDecimalAscending(vals[i%len(vals)], buf)
	}
}
