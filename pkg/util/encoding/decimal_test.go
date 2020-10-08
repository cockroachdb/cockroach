// Copyright 2016 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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
		d := new(apd.Decimal)
		if _, _, err := d.SetString(c.Value); err != nil {
			t.Fatalf("could not parse decimal from string %q", c.Value)
		}

		if e, m := decimalEandM(d, nil); e != c.E || !bytes.Equal(m, c.M) {
			t.Errorf("unexpected mismatch in E/M for %v. expected E=%v | M=[% x], got E=%v | M=[% x]",
				c.Value, c.E, c.M, e, m)
		}
	}
}

func mustDecimal(s string) *apd.Decimal {
	d, _, err := new(apd.Decimal).SetString(s)
	if err != nil {
		panic(fmt.Sprintf("could not set string %q on decimal", s))
	}
	return d
}

func randBuf(rng *rand.Rand, maxLen int) []byte {
	buf := make([]byte, rng.Intn(maxLen+1))
	_, _ = rng.Read(buf)
	return buf
}

func encodeDecimalWithDir(dir Direction, buf []byte, d *apd.Decimal) []byte {
	if dir == Ascending {
		return EncodeDecimalAscending(buf, d)
	}
	return EncodeDecimalDescending(buf, d)
}

func decodeDecimalWithDir(
	t *testing.T, dir Direction, buf []byte, tmp []byte,
) ([]byte, apd.Decimal) {
	var err error
	var resBuf []byte
	var res apd.Decimal
	if dir == Ascending {
		resBuf, res, err = DecodeDecimalAscending(buf, tmp)
	} else {
		resBuf, res, err = DecodeDecimalDescending(buf, tmp)
	}
	if err != nil {
		t.Fatal(err)
	}
	return resBuf, res
}

func mustDecimalFloat64(f float64) *apd.Decimal {
	d, err := new(apd.Decimal).SetFloat64(f)
	if err != nil {
		panic(err)
	}
	return d
}

func mustDecimalString(s string) *apd.Decimal {
	d, _, err := apd.NewFromString(s)
	if err != nil {
		panic(err)
	}
	return d
}

func TestEncodeDecimal(t *testing.T) {
	testCases := []struct {
		Value    *apd.Decimal
		Encoding []byte
	}{
		{&apd.Decimal{Form: apd.NaN}, []byte{0x18}},
		{&apd.Decimal{Form: apd.Infinite, Negative: true}, []byte{0x19}},
		{apd.New(-99122, 99999), []byte{0x1a, 0x86, 0x3c, 0xad, 0x38, 0xe6, 0xd7, 0x00}},
		// Three duplicates to make sure -13*10^1000 <= -130*10^999 <= -13*10^1000
		{apd.New(-13, 1000), []byte{0x1a, 0x86, 0xfe, 0x0a, 0xe5, 0x00}},
		{apd.New(-130, 999), []byte{0x1a, 0x86, 0xfe, 0x0a, 0xe5, 0x00}},
		{apd.New(-13, 1000), []byte{0x1a, 0x86, 0xfe, 0x0a, 0xe5, 0x00}},
		{mustDecimalFloat64(-math.MaxFloat64), []byte{0x1a, 0x87, 0x64, 0xfc, 0x60, 0x66, 0x44, 0xe4, 0x9e, 0x82, 0xc0, 0x8d, 0x00}},
		{apd.New(-130, 100), []byte{0x1a, 0x87, 0xcb, 0xfc, 0xc3, 0x00}},
		{apd.New(-13, 0), []byte{0x24, 0xe5, 0x00}},
		{apd.New(-11, 0), []byte{0x24, 0xe9, 0x00}},
		{mustDecimal("-10.123456789"), []byte{0x24, 0xea, 0xe6, 0xba, 0x8e, 0x62, 0x4b, 0x00}},
		{mustDecimal("-10"), []byte{0x24, 0xeb, 0x00}},
		{mustDecimal("-9.123456789"), []byte{0x24, 0xec, 0xe6, 0xba, 0x8e, 0x62, 0x4b, 0x00}},
		{mustDecimal("-9"), []byte{0x24, 0xed, 0x00}},
		{mustDecimal("-1.1"), []byte{0x24, 0xfc, 0xeb, 0x00}},
		{apd.New(-1, 0), []byte{0x24, 0xfd, 0x00}},
		{apd.New(-8, -1), []byte{0x25, 0x5f, 0x00}},
		{apd.New(-1, -1), []byte{0x25, 0xeb, 0x00}},
		{mustDecimal("-.09"), []byte{0x25, 0xed, 0x00}},
		{mustDecimal("-.054321"), []byte{0x25, 0xf4, 0xa8, 0xd5, 0x00}},
		{mustDecimal("-.012"), []byte{0x25, 0xfc, 0xd7, 0x00}},
		{apd.New(-11, -4), []byte{0x26, 0x89, 0xe9, 0x00}},
		{apd.New(-11, -6), []byte{0x26, 0x8a, 0xe9, 0x00}},
		{mustDecimalFloat64(-math.SmallestNonzeroFloat64), []byte{0x26, 0xf6, 0xa1, 0xf5, 0x00}},
		{apd.New(-11, -66666), []byte{0x26, 0xf7, 0x82, 0x34, 0xe9, 0x00}},
		{mustDecimal("-0"), []byte{0x27}},
		{apd.New(0, 0), []byte{0x27}},
		{mustDecimalFloat64(math.SmallestNonzeroFloat64), []byte{0x28, 0x87, 0x5e, 0x0a, 0x00}},
		{apd.New(11, -6), []byte{0x28, 0x87, 0xfd, 0x16, 0x00}},
		{apd.New(11, -4), []byte{0x28, 0x87, 0xfe, 0x16, 0x00}},
		{apd.New(1, -1), []byte{0x29, 0x14, 0x00}},
		{apd.New(8, -1), []byte{0x29, 0xa0, 0x00}},
		{apd.New(1, 0), []byte{0x2a, 0x02, 0x00}},
		{mustDecimal("1.1"), []byte{0x2a, 0x03, 0x14, 0x00}},
		{apd.New(11, 0), []byte{0x2a, 0x16, 0x00}},
		{apd.New(13, 0), []byte{0x2a, 0x1a, 0x00}},
		{mustDecimalFloat64(math.MaxFloat64), []byte{0x34, 0xf6, 0x9b, 0x03, 0x9f, 0x99, 0xbb, 0x1b, 0x61, 0x7d, 0x3f, 0x72, 0x00}},
		// Four duplicates to make sure 13*10^1000 <= 130*10^999 <= 1300*10^998 <= 13*10^1000
		{apd.New(13, 1000), []byte{0x34, 0xf7, 0x01, 0xf5, 0x1a, 0x00}},
		{apd.New(130, 999), []byte{0x34, 0xf7, 0x01, 0xf5, 0x1a, 0x00}},
		{apd.New(1300, 998), []byte{0x34, 0xf7, 0x01, 0xf5, 0x1a, 0x00}},
		{apd.New(13, 1000), []byte{0x34, 0xf7, 0x01, 0xf5, 0x1a, 0x00}},
		{apd.New(99122, 99999), []byte{0x34, 0xf7, 0xc3, 0x52, 0xc7, 0x19, 0x28, 0x00}},
		{apd.New(99122839898321208, 99999), []byte{0x34, 0xf7, 0xc3, 0x58, 0xc7, 0x19, 0x39, 0x4f, 0xb3, 0xa7, 0x2b, 0x29, 0xa0, 0x00}},
		{&apd.Decimal{Form: apd.Infinite}, []byte{0x35}},
	}

	rng, _ := randutil.NewPseudoRand()

	var lastEncoded []byte
	for _, dir := range []Direction{Ascending, Descending} {
		for _, tmp := range [][]byte{nil, make([]byte, 0, 100)} {
			for i, c := range testCases {
				t.Run(fmt.Sprintf("%v_%d_%d_%s", dir, cap(tmp), i, c.Value), func(t *testing.T) {
					enc := encodeDecimalWithDir(dir, nil, c.Value)
					_, dec := decodeDecimalWithDir(t, dir, enc, tmp)
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
					testPeekLength(t, enc)
					if dec.Cmp(c.Value) != 0 {
						t.Errorf("%d unexpected mismatch for %v. got %v", i, c.Value, dec)
					}
					lastEncoded = enc

					// Test that appending the decimal to an existing buffer works. It
					// is important to test with various values, slice lengths, and
					// capacities because the various encoding paths try to use any
					// spare capacity to avoid allocations.
					for trials := 0; trials < 5; trials++ {
						orig := randBuf(rng, 30)
						origLen := len(orig)

						bufCap := origLen + rng.Intn(30)
						buf := make([]byte, origLen, bufCap)
						copy(buf, orig)

						enc := encodeDecimalWithDir(dir, buf, c.Value)
						// Append some random bytes
						enc = append(enc, randBuf(rng, 20)...)
						_, dec := decodeDecimalWithDir(t, dir, enc[origLen:], tmp)

						if dec.Cmp(c.Value) != 0 {
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

func TestEncodeDecimalRand(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()
	// Test both directions.
	for _, dir := range []Direction{Ascending, Descending} {
		var prev *apd.Decimal
		var prevEnc []byte
		const randomTrials = 100000
		for i := 0; i < randomTrials; i++ {
			cur := randDecimal(rng, -20, 20)
			var tmp, appendTo []byte
			// Test with and without appending.
			if rng.Intn(2) == 1 {
				appendTo = randBuf(rng, 30)
				appendTo = appendTo[:rng.Intn(len(appendTo)+1)]
			}
			// Test with and without tmp buffer.
			if rng.Intn(2) == 1 {
				tmp = randBuf(rng, 100)
			}
			var enc []byte
			var res apd.Decimal
			var err error
			if dir == Ascending {
				enc = EncodeDecimalAscending(appendTo, cur)
				enc = enc[len(appendTo):]
				_, res, err = DecodeDecimalAscending(enc, tmp)
			} else {
				enc = EncodeDecimalDescending(appendTo, cur)
				enc = enc[len(appendTo):]
				_, res, err = DecodeDecimalDescending(enc, tmp)
			}
			if err != nil {
				t.Fatal(err)
			}

			testPeekLength(t, enc)

			// Make sure we decode the same value we encoded.
			if cur.Cmp(&res) != 0 {
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

func TestNonsortingEncodeDecimal(t *testing.T) {
	testCases := []struct {
		Value    *apd.Decimal
		Encoding []byte
	}{
		{&apd.Decimal{Form: apd.NaN}, []byte{0x18}},
		{&apd.Decimal{Form: apd.Infinite, Negative: true}, []byte{0x19}},
		{apd.New(-99122, 99999), []byte{0x1a, 0xf8, 0x01, 0x86, 0xa4, 0x01, 0x83, 0x32}},
		// Three duplicates to make sure -13*10^1000 <= -130*10^999 <= -13*10^1000
		{apd.New(-13, 1000), []byte{0x1a, 0xf7, 0x03, 0xea, 0x0d}},
		{apd.New(-130, 999), []byte{0x1a, 0xf7, 0x03, 0xea, 0x82}},
		{apd.New(-13, 1000), []byte{0x1a, 0xf7, 0x03, 0xea, 0x0d}},
		{mustDecimalFloat64(-math.MaxFloat64), []byte{0x1a, 0xf7, 0x01, 0x35, 0x3f, 0xdd, 0xec, 0x7f, 0x2f, 0xaf, 0x35}},
		{apd.New(-130, 100), []byte{0x1a, 0xef, 0x82}},
		{apd.New(-13, 0), []byte{0x1a, 0x8a, 0x0d}},
		{apd.New(-11, 0), []byte{0x1a, 0x8a, 0x0b}},
		{apd.New(-1, 0), []byte{0x1a, 0x89, 0x01}},
		{apd.New(-8, -1), []byte{0x25, 0x08}},
		{apd.New(-1, -1), []byte{0x25, 0x01}},
		{apd.New(-11, -4), []byte{0x26, 0x8a, 0x0b}},
		{apd.New(-11, -6), []byte{0x26, 0x8c, 0x0b}},
		{mustDecimalFloat64(-math.SmallestNonzeroFloat64), []byte{0x26, 0xf7, 0x01, 0x43, 0x05}},
		{apd.New(-11, -66666), []byte{0x26, 0xf8, 0x01, 0x04, 0x68, 0x0b}},
		{mustDecimal("-0"), []byte{0x1a, 0x89}},
		{apd.New(0, 0), []byte{0x27}},
		{mustDecimalFloat64(math.SmallestNonzeroFloat64), []byte{0x28, 0xf7, 0x01, 0x43, 0x05}},
		{apd.New(11, -6), []byte{0x28, 0x8c, 0x0b}},
		{apd.New(11, -4), []byte{0x28, 0x8a, 0x0b}},
		{apd.New(1, -1), []byte{0x29, 0x01}},
		{apd.New(12345, -5), []byte{0x29, 0x30, 0x39}},
		{apd.New(8, -1), []byte{0x29, 0x08}},
		{apd.New(1, 0), []byte{0x34, 0x89, 0x01}},
		{apd.New(11, 0), []byte{0x34, 0x8a, 0x0b}},
		{apd.New(13, 0), []byte{0x34, 0x8a, 0x0d}},
		// Note that this does not sort correctly!
		{apd.New(255, 0), []byte{0x34, 0x8b, 0xff}},
		{apd.New(256, 0), []byte{0x34, 0x8b, 0x01, 0x00}},
		{mustDecimalFloat64(math.MaxFloat64), []byte{0x34, 0xf7, 0x01, 0x35, 0x3f, 0xdd, 0xec, 0x7f, 0x2f, 0xaf, 0x35}},
		// Four duplicates to make sure 13*10^1000 <= 130*10^999 <= 1300*10^998 <= 13*10^1000
		{apd.New(13, 1000), []byte{0x34, 0xf7, 0x03, 0xea, 0x0d}},
		{apd.New(130, 999), []byte{0x34, 0xf7, 0x03, 0xea, 0x82}},
		{apd.New(1300, 998), []byte{0x34, 0xf7, 0x03, 0xea, 0x05, 0x14}},
		{apd.New(13, 1000), []byte{0x34, 0xf7, 0x03, 0xea, 0x0d}},
		{apd.New(99122, 99999), []byte{0x34, 0xf8, 0x01, 0x86, 0xa4, 0x01, 0x83, 0x32}},
		{apd.New(99122839898321208, 99999), []byte{0x34, 0xf8, 0x01, 0x86, 0xb0, 0x01, 0x60, 0x27, 0xb2, 0x9d, 0x44, 0x71, 0x38}},
		{&apd.Decimal{Form: apd.Infinite}, []byte{0x35}},
		{mustDecimalString("142378208485490985369999605144727062141206925976498256305323716858805588894693616552055968571135475510700810219028167653516982373238641332965927953273383572708760984694356069974208844865675206339235758647159337463780100273189720943242182911961627806424621091859596571173867825568394327041453823674373002756096"), []byte{0x34, 0xf7, 0x01, 0x35, 0xca, 0xc0, 0xd8, 0x34, 0x68, 0x5d, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}

	rng, _ := randutil.NewPseudoRand()

	for _, tmp := range [][]byte{nil, make([]byte, 0, 100)} {
		for i, c := range testCases {
			t.Run(fmt.Sprintf("%d_%d_%s", cap(tmp), i, c.Value), func(t *testing.T) {
				enc := EncodeNonsortingDecimal(nil, c.Value)
				dec, err := DecodeNonsortingDecimal(enc, tmp)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(enc, c.Encoding) {
					t.Errorf("unexpected mismatch for %s. expected [% x], got [% x]",
						c.Value, c.Encoding, enc)
				}
				if dec.CmpTotal(c.Value) != 0 {
					t.Errorf("%d unexpected mismatch for %v. got %v", i, c.Value, dec)
				}
				// Test that appending the decimal to an existing buffer works. It
				// is important to test with various values, slice lengths, and
				// capacities because the various encoding paths try to use any
				// spare capacity to avoid allocations.
				for trials := 0; trials < 5; trials++ {
					orig := randBuf(rng, 30)
					origLen := len(orig)

					bufCap := origLen + rng.Intn(30)
					buf := make([]byte, origLen, bufCap)
					copy(buf, orig)

					enc := EncodeNonsortingDecimal(buf, c.Value)
					dec, err := DecodeNonsortingDecimal(enc[origLen:], tmp)
					if err != nil {
						t.Fatal(err)
					}

					if dec.CmpTotal(c.Value) != 0 {
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

func TestNonsortingEncodeDecimalRand(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()
	const randomTrials = 200000
	for i := 0; i < randomTrials; i++ {
		var tmp, appendTo []byte
		// Test with and without appending.
		if rng.Intn(2) == 1 {
			appendTo = randBuf(rng, 30)
			appendTo = appendTo[:rng.Intn(len(appendTo)+1)]
		}
		// Test with and without tmp buffer.
		if rng.Intn(2) == 1 {
			tmp = randBuf(rng, 100)
		}
		cur := randDecimal(rng, -20, 20)

		enc := EncodeNonsortingDecimal(appendTo, cur)
		enc = enc[len(appendTo):]
		res, err := DecodeNonsortingDecimal(enc, tmp)
		if err != nil {
			t.Fatal(err)
		}

		// Make sure we decode the same value we encoded.
		if cur.Cmp(&res) != 0 {
			t.Fatalf("unexpected mismatch for %v, got %v", cur, res)
		}

		// Make sure we would have overestimated the value.
		if est := UpperBoundNonsortingDecimalSize(cur); est < len(enc) {
			t.Fatalf("expected estimate of %d for %v to be greater than or equal to the encoded length, found [% x]", est, cur, enc)
		}
	}
}

// TestNonsortingEncodeDecimalRoundtrip tests that decimals can round trip
// through EncodeNonsortingDecimal and DecodeNonsortingDecimal with an expected
// coefficient and exponent.
func TestNonsortingEncodeDecimalRoundtrip(t *testing.T) {
	tests := map[string]string{
		"0":         "0E+0",
		"0.0":       "0E-1",
		"0.00":      "0E-2",
		"0e-10":     "0E-10",
		"0.00e-10":  "0E-12",
		"00":        "0E+0",
		"-0":        "-0E+0",
		"-0.0":      "-0E-1",
		"-0.00":     "-0E-2",
		"-0e-10":    "-0E-10",
		"-0.00e-10": "-0E-12",
		"-00":       "-0E+0",
	}
	for tc, expect := range tests {
		t.Run(tc, func(t *testing.T) {
			d, _, err := apd.NewFromString(tc)
			if err != nil {
				t.Fatal(err)
			}
			enc := EncodeNonsortingDecimal(nil, d)
			res, err := DecodeNonsortingDecimal(enc, nil)
			if err != nil {
				t.Fatal(err)
			}
			s := res.Text('E')
			if expect != s {
				t.Fatalf("expected %s, got %s", expect, s)
			}
		})
	}
}

func TestDecodeMultipleDecimalsIntoNonsortingDecimal(t *testing.T) {
	tcs := []struct {
		value []string
	}{
		{
			[]string{"1.0", "5.0", "7.0"},
		},
		{
			[]string{"1.0", "-1.0", "0.0"},
		},
		{
			[]string{"1.0", "-1.0", "10.0"},
		},
		{
			[]string{"nan", "1.0", "-1.0"},
		},
		{
			[]string{"-1.0", "inf", "5.0"},
		},
	}

	for _, tc := range tcs {
		var actual apd.Decimal
		for _, num := range tc.value {
			expected, _, err := apd.NewFromString(num)
			if err != nil {
				t.Fatal(err)
			}
			enc := EncodeNonsortingDecimal(nil, expected)
			err = DecodeIntoNonsortingDecimal(&actual, enc, nil)
			if err != nil {
				t.Fatal(err)
			}
			if actual.Cmp(expected) != 0 {
				t.Errorf("unexpected mismatch for %v, got %v", expected, &actual)
			}
		}
	}
}

func TestUpperBoundNonsortingDecimalUnscaledSize(t *testing.T) {
	x := make([]byte, 100)
	d := new(apd.Decimal)
	for i := 0; i < len(x); i++ {
		d.Coeff.SetString(string(x[:i]), 10)
		reference := UpperBoundNonsortingDecimalSize(d)
		bound := upperBoundNonsortingDecimalUnscaledSize(i)
		if bound < reference || bound > reference+bigWordSize {
			t.Errorf("%d: got a bound of %d but expected between %d and %d", i, bound, reference, reference+bigWordSize)
		}
		x[i] = '1'
	}
}

// randDecimal generates a random decimal with exponent in the
// range [minExp, maxExp].
func randDecimal(rng *rand.Rand, minExp, maxExp int) *apd.Decimal {
	exp := randutil.RandIntInRange(rng, minExp, maxExp+1)
	// Transform random float in [0, 1) to [-1, 1) and multiply by 10^exp.
	floatVal := (rng.Float64()*2 - 1) * math.Pow10(exp)
	return mustDecimalFloat64(floatVal)
}

// makeDecimalVals creates decimal values with exponents in
// the range [minExp, maxExp].
func makeDecimalVals(minExp, maxExp int) []*apd.Decimal {
	rng, _ := randutil.NewPseudoRand()
	vals := make([]*apd.Decimal, 10000)
	for i := range vals {
		vals[i] = randDecimal(rng, minExp, maxExp)
	}
	return vals
}

func makeEncodedVals(minExp, maxExp int) [][]byte {
	rng, _ := randutil.NewPseudoRand()
	vals := make([][]byte, 10000)
	for i := range vals {
		vals[i] = EncodeDecimalAscending(nil, randDecimal(rng, minExp, maxExp))
	}
	return vals
}

func BenchmarkEncodeDecimalSmall(b *testing.B) {
	vals := makeDecimalVals(-40, -1)
	buf := make([]byte, 0, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeDecimalAscending(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeDecimalSmall(b *testing.B) {
	vals := makeEncodedVals(-40, -1)
	buf := make([]byte, 0, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeDecimalAscending(vals[i%len(vals)], buf)
	}
}

func BenchmarkEncodeDecimalMedium(b *testing.B) {
	vals := makeDecimalVals(0, 10)
	buf := make([]byte, 0, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeDecimalAscending(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeDecimalMedium(b *testing.B) {
	vals := makeEncodedVals(0, 10)
	buf := make([]byte, 0, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeDecimalAscending(vals[i%len(vals)], buf)
	}
}

func BenchmarkEncodeDecimalLarge(b *testing.B) {
	vals := makeDecimalVals(11, 40)
	buf := make([]byte, 0, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeDecimalAscending(buf, vals[i%len(vals)])
	}
}

func BenchmarkDecodeDecimalLarge(b *testing.B) {
	vals := makeEncodedVals(11, 40)
	buf := make([]byte, 0, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeDecimalAscending(vals[i%len(vals)], buf)
	}
}

func BenchmarkPeekLengthDecimal(b *testing.B) {
	vals := makeEncodedVals(-20, 20)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = PeekLength(vals[i%len(vals)])
	}
}

func BenchmarkNonsortingEncodeDecimal(b *testing.B) {
	vals := makeDecimalVals(-20, 20)
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
		d := randDecimal(rng, -20, 20)
		vals[i] = EncodeNonsortingDecimal(nil, d)
	}

	buf := make([]byte, 0, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeNonsortingDecimal(vals[i%len(vals)], buf)
	}
}
