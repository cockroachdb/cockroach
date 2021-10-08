// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bitarray

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestParseFormatBinary(t *testing.T) {
	testData := []struct {
		str string
		ba  BitArray
	}{
		{"", BitArray{words: nil, lastBitsUsed: 0}},
		{"0", BitArray{words: []word{0}, lastBitsUsed: 1}},
		{"1", BitArray{words: []word{0x8000000000000000}, lastBitsUsed: 1}},
		{"0000", BitArray{words: []word{0}, lastBitsUsed: 4}},
		{"1010", BitArray{words: []word{0xA000000000000000}, lastBitsUsed: 4}},
		{"10111010101111101111101011001110" + "11001010111111101011101010111110",
			BitArray{words: []word{0xBABEFACECAFEBABE}, lastBitsUsed: 64}},
		{"10111010101111101111101011001110" + "11001010111111101011101010111110" +
			"110010101101110100011110",
			BitArray{words: []word{0xBABEFACECAFEBABE, 0xCADD1E0000000000}, lastBitsUsed: 24}},
	}

	for _, test := range testData {
		t.Run(test.str, func(t *testing.T) {
			ba, err := Parse(test.str)
			if err != nil {
				t.Fatalf("error during parse: %v", err)
			}

			if !reflect.DeepEqual(ba, test.ba) {
				t.Fatalf("expected %+v, got %+v", test.ba, ba)
			}

			res := ba.String()
			if res != test.str {
				t.Fatalf("format expected %q, got %q", test.str, res)
			}
		})
	}
}

func TestParseFormatHex(t *testing.T) {
	testData := []struct {
		str string
		ba  BitArray
	}{
		{"x", BitArray{words: nil, lastBitsUsed: 0}},
		{"x0", BitArray{words: []word{0}, lastBitsUsed: 4}},
		{"x1", BitArray{words: []word{0x1000000000000000}, lastBitsUsed: 4}},
		{"X0", BitArray{words: []word{0}, lastBitsUsed: 4}},
		{"X1", BitArray{words: []word{0x1000000000000000}, lastBitsUsed: 4}},
		{"xA", BitArray{words: []word{0xA000000000000000}, lastBitsUsed: 4}},
		{"xF", BitArray{words: []word{0xF000000000000000}, lastBitsUsed: 4}},
		{"xa", BitArray{words: []word{0xA000000000000000}, lastBitsUsed: 4}},
		{"xf", BitArray{words: []word{0xF000000000000000}, lastBitsUsed: 4}},
		{"Xa", BitArray{words: []word{0xA000000000000000}, lastBitsUsed: 4}},
		{"Xf", BitArray{words: []word{0xF000000000000000}, lastBitsUsed: 4}},
		{"x0000", BitArray{words: []word{0}, lastBitsUsed: 16}},
		{"x1010", BitArray{words: []word{0x1010000000000000}, lastBitsUsed: 16}},
		{"xBaBE", BitArray{words: []word{0xBABE000000000000}, lastBitsUsed: 16}},
		{"xBaBEfAcE", BitArray{words: []word{0xBABEFACE00000000}, lastBitsUsed: 32}},
		{"xBaBEEE", BitArray{words: []word{0xBABEEE0000000000}, lastBitsUsed: 24}},
		{"XBABEFACECAFEBABE",
			BitArray{words: []word{0xBABEFACECAFEBABE}, lastBitsUsed: 64}},
		{"xBABEFACECAFEBABECADD1E",
			BitArray{words: []word{0xBABEFACECAFEBABE, 0xCADD1E0000000000}, lastBitsUsed: 24}},
	}

	for _, test := range testData {
		t.Run(test.str, func(t *testing.T) {
			ba, err := Parse(test.str)
			if err != nil {
				t.Fatalf("error during parse: %v", err)
			}

			if !reflect.DeepEqual(ba, test.ba) {
				t.Fatalf("expected %+v, got %+v", test.ba, ba)
			}
		})
	}
}

func TestFromEncodingParts(t *testing.T) {
	testData := []struct {
		words        []uint64
		lastBitsUsed uint64
		ba           BitArray
		err          string
	}{
		{nil, 0, BitArray{words: nil, lastBitsUsed: 0}, ""},
		{[]uint64{0}, 0, BitArray{words: []word{0}, lastBitsUsed: 0}, ""},
		{[]uint64{42}, 3, BitArray{words: []word{42}, lastBitsUsed: 3}, ""},
		{[]uint64{42}, 65, BitArray{}, "FromEncodingParts: lastBitsUsed must not exceed 64, got 65"},
	}

	for _, test := range testData {
		t.Run(fmt.Sprintf("{%v,%d}", test.words, test.lastBitsUsed), func(t *testing.T) {
			ba, err := FromEncodingParts(test.words, test.lastBitsUsed)
			if test.err != "" && (err == nil || test.err != err.Error()) {
				t.Errorf("expected %q error, but got: %+v", test.err, err)
			} else if test.err == "" && err != nil {
				t.Errorf("unexpected error: %s", err)
			} else if !reflect.DeepEqual(ba, test.ba) {
				t.Errorf("expected %s, got %s", test.ba.viz(), ba.viz())
			}
		})
	}
}

func (d BitArray) viz() string {
	var buf bytes.Buffer
	buf.WriteString("bitarray{[")
	comma := ""
	for _, w := range d.words {
		buf.WriteString(comma)
		for i := 60; i > 0; i -= 4 {
			if i < 60 {
				buf.WriteByte(' ')
			}
			c := strconv.FormatUint((w>>uint(i))&0xf, 2)
			if len(c) < 4 {
				fmt.Fprintf(&buf, "%0*d", 4-len(c), 0)
			}
			buf.WriteString(c)
		}
		comma = ","
	}
	fmt.Fprintf(&buf, "],%d}", d.lastBitsUsed)
	return buf.String()
}

func TestToWidth(t *testing.T) {
	testData := []struct {
		str     string
		toWidth uint
		ba      BitArray
	}{
		{"", 0, BitArray{words: nil, lastBitsUsed: 0}},
		{"", 1, BitArray{words: []word{0}, lastBitsUsed: 1}},
		{"", 64, BitArray{words: []word{0}, lastBitsUsed: 64}},
		{"", 100, BitArray{words: []word{0, 0}, lastBitsUsed: 36}},
		{"0", 0, BitArray{words: nil, lastBitsUsed: 0}},
		{"0", 1, BitArray{words: []word{0}, lastBitsUsed: 1}},
		{"0", 100, BitArray{words: []word{0, 0}, lastBitsUsed: 36}},
		{"1", 1, BitArray{words: []word{0x8000000000000000}, lastBitsUsed: 1}},
		{"1", 64, BitArray{words: []word{0x8000000000000000}, lastBitsUsed: 64}},
		{"1", 100, BitArray{words: []word{0x8000000000000000, 0}, lastBitsUsed: 36}},
		{"0000", 4, BitArray{words: []word{0}, lastBitsUsed: 4}},
		{"1010", 4, BitArray{words: []word{0xA000000000000000}, lastBitsUsed: 4}},
		{"0000", 0, BitArray{words: nil, lastBitsUsed: 0}},
		{"1010", 2, BitArray{words: []word{0x8000000000000000}, lastBitsUsed: 2}},
		{"10111010101111101111101011001110" + "11001010111111101011101010111110",
			64,
			BitArray{words: []word{0xBABEFACECAFEBABE}, lastBitsUsed: 64}},
		{"10111010101111101111101011001110" + "11001010111111101011101010111110",
			16,
			BitArray{words: []word{0xBABE000000000000}, lastBitsUsed: 16}},
		{"10111010101111101111101011001110" + "11001010111111101011101010111110" +
			"110010101101110100011110",
			60,
			BitArray{words: []word{0xBABEFACECAFEBAB0}, lastBitsUsed: 60}},
		{"10111010101111101111101011001110" + "11001010111111101011101010111110" +
			"110010101101110100011110",
			100,
			BitArray{words: []word{0xBABEFACECAFEBABE, 0xCADD1E0000000000}, lastBitsUsed: 36}},
	}

	for _, test := range testData {
		t.Run(fmt.Sprintf("%s/%d", test.str, test.toWidth), func(t *testing.T) {
			ba, err := Parse(test.str)
			if err != nil {
				t.Fatalf("error during parse: %v", err)
			}

			ba = ba.ToWidth(test.toWidth)

			if !reflect.DeepEqual(ba, test.ba) {
				t.Fatalf("expected %s, got %s", test.ba.viz(), ba.viz())
			}

			vs := ba.String()
			if uint(len(vs)) != test.toWidth {
				t.Fatalf("expected len %d in representation, got %d", test.toWidth, len(vs))
			}
			if uint(len(test.str)) < test.toWidth {
				exp := test.str + fmt.Sprintf("%0*d", int(test.toWidth)-len(test.str), 0)
				if vs != exp {
					t.Fatalf("expected %q, got %q", exp, vs)
				}
			} else {
				if test.str[:int(test.toWidth)] != vs {
					t.Fatalf("expected %q, got %q", test.str[:int(test.toWidth)], vs)
				}
			}
		})
	}
}

func TestToInt(t *testing.T) {
	testData := []struct {
		str     string
		nbits   uint
		exp     int64
		expbits string
	}{
		{"", 0, 0, "0"},
		{"", 1, 0, "0"},
		{"", 64, 0, "0"},
		{"0", 0, 0, "0"},
		{"0", 1, 0, "0"},
		{"0", 64, 0, "0"},
		{"1", 0, 0, "0"},
		{"1", 1, -1, "1111111111111111111111111111111111111111111111111111111111111111"},
		{"1", 64, 1, "1"},
		{"01", 0, 0, "0"},
		{"01", 1, -1, "1111111111111111111111111111111111111111111111111111111111111111"},
		{"01", 2, 1, "1"},
		{"01", 64, 1, "1"},
		{"10111010101111101111101011001110" + "11001010111111101011101010111110" +
			"110010101101110100011110", 6, 30, "11110"},
		{"10111010101111101111101011001110" + "11001010111111101011101000111110" +
			"110010101101110100011110", 32, 0x3ECADD1E,
			"111110" + "110010101101110100011110"},
	}

	for _, test := range testData {
		t.Run(fmt.Sprintf("%s/%d", test.str, test.nbits), func(t *testing.T) {
			expbits := strconv.FormatUint(uint64(test.exp), 2)
			if expbits != test.expbits {
				t.Fatalf("programming error: invalid expbits: got %q, expected %q", expbits, test.expbits)
			}
			ba, err := Parse(test.str)
			if err != nil {
				t.Fatal(err)
			}
			if ba.String() != test.str {
				t.Fatalf("expected %q, got %q", test.str, ba.String())
			}
			res := ba.AsInt64(test.nbits)
			if res != test.exp {
				t.Fatalf("expected %d (%b), got %d (%b)", test.exp, test.exp, res, uint64(res))
			}
		})
	}
}

func TestFromInt(t *testing.T) {
	largeVal := uint64(0xCAFEBABEDEADBEEF)
	sLarge := int64(largeVal)
	uLarge := int64(largeVal ^ (1 << 63))
	t.Logf("sLarge = %d (%#x)", sLarge, sLarge)
	t.Logf("uLarge = %d (%#x)", uLarge, uLarge)
	testData := []struct {
		bitlen   uint
		val      int64
		valWidth uint
		exp      string
	}{
		{0, 123, 0, ""},
		{0, 123, 64, ""},
		{1, 0x1, 0, "0"},
		{1, 0x1, 1, "1"},
		{2, 0x1, 1, "11" /* sign extend from 1 to 2 bits */},
		{64, 0x2, 2, "11111111111111111111111111111111" + "11111111111111111111111111111110"},
		{64, 0x2, 3, "00000000000000000000000000000000" + "00000000000000000000000000000010"},
		{67, 0x2, 2, "11111111111111111111111111111111" + "11111111111111111111111111111111" + "110"},
		{67, 0x2, 3, "00000000000000000000000000000000" + "00000000000000000000000000000000" + "010"},
		{131, 0x2, 2,
			"11111111111111111111111111111111" + "11111111111111111111111111111111" +
				"11111111111111111111111111111111" + "11111111111111111111111111111111" + "110"},
		{131, 0x2, 3,
			"00000000000000000000000000000000" + "00000000000000000000000000000000" +
				"00000000000000000000000000000000" + "00000000000000000000000000000000" + "010"},
		{67, 0xAA, 8, "11111111111111111111111111111111" + "11111111111111111111111111110101" + "010"},
		{67, 0xAA, 9, "00000000000000000000000000000000" + "00000000000000000000000000010101" + "010"},
		{131, 0xAA, 8,
			"11111111111111111111111111111111" + "11111111111111111111111111111111" +
				"11111111111111111111111111111111" + "11111111111111111111111111110101" + "010"},
		{131, 0xAA, 9,
			"00000000000000000000000000000000" + "00000000000000000000000000000000" +
				"00000000000000000000000000000000" + "00000000000000000000000000010101" + "010"},
		{12, sLarge, 64, "111011101111"},
		{64, sLarge, 64, "11001010111111101011101010111110" + "11011110101011011011111011101111"},
		{64, uLarge, 64, "01001010111111101011101010111110" + "11011110101011011011111011101111"},
		{67, sLarge, 64, "111" + "11001010111111101011101010111110" + "11011110101011011011111011101111"},
		{67, uLarge, 64, "000" + "01001010111111101011101010111110" + "11011110101011011011111011101111"},
	}

	for _, test := range testData {
		t.Run(fmt.Sprintf("%d/%0*b", test.bitlen, test.valWidth, test.val), func(t *testing.T) {
			d := MakeBitArrayFromInt64(test.bitlen, test.val, test.valWidth)
			res := d.String()
			if res != test.exp {
				t.Fatalf("expected %q, got %q", test.exp, res)
			}
		})
	}

}

func TestAnd(t *testing.T) {
	testData := []struct {
		lhs, rhs, result string
	}{
		{"", "", ""},
		{"0", "0", "0"},
		{"0", "1", "0"},
		{"1", "0", "0"},
		{"1", "1", "1"},
		{"1111", "1100", "1100"},
		{"10111010101111101111101011001110" + "11001010111111101011101010111110" + "010",
			"11001010111111101011101010111110" + "10111010101111101111101011001110" + "011",
			"10001010101111101011101010001110" + "10001010101111101011101010001110" + "010"},
	}

	for _, test := range testData {
		t.Run(test.lhs+"&"+test.rhs, func(t *testing.T) {
			lhs, err := Parse(test.lhs)
			if err != nil {
				t.Fatalf("error during parse: %v", err)
			}
			rhs, err := Parse(test.rhs)
			if err != nil {
				t.Fatalf("error during parse: %v", err)
			}
			res := And(lhs, rhs)
			resStr := res.String()
			if resStr != test.result {
				t.Fatalf("concat expected %q, got %q", test.result, resStr)
			}
			res2, err := Parse(resStr)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(res2, res) {
				t.Fatalf("result does not roundrip, expected %s, got %s", res.viz(), res2.viz())
			}
		})
	}
}

func TestOr(t *testing.T) {
	testData := []struct {
		lhs, rhs, result string
	}{
		{"", "", ""},
		{"0", "0", "0"},
		{"0", "1", "1"},
		{"1", "0", "1"},
		{"1", "1", "1"},
		{"1100", "0101", "1101"},
		{"10111010101111101111101011001110" + "11001010111111101011101010111110" + "010",
			"11001010111111101011101010111110" + "10111010101111101111101011001110" + "011",
			"11111010111111101111101011111110" + "11111010111111101111101011111110" + "011"},
	}

	for _, test := range testData {
		t.Run(test.lhs+"|"+test.rhs, func(t *testing.T) {
			lhs, err := Parse(test.lhs)
			if err != nil {
				t.Fatalf("error during parse: %v", err)
			}
			rhs, err := Parse(test.rhs)
			if err != nil {
				t.Fatalf("error during parse: %v", err)
			}
			res := Or(lhs, rhs)
			resStr := res.String()
			if resStr != test.result {
				t.Fatalf("concat expected %q, got %q", test.result, resStr)
			}

			res2, err := Parse(resStr)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(res2, res) {
				t.Fatalf("result does not roundrip, expected %s, got %s", res.viz(), res2.viz())
			}
		})
	}
}

func TestXor(t *testing.T) {
	testData := []struct {
		lhs, rhs, result string
	}{
		{"", "", ""},
		{"0", "0", "0"},
		{"0", "1", "1"},
		{"1", "0", "1"},
		{"1", "1", "0"},
		{"1100", "0101", "1001"},
		{"10111010101111101111101011001110" + "11001010111111101011101010111110" + "010",
			"11001010111111101011101010111110" + "10111010101111101111101011001110" + "011",
			"01110000010000000100000001110000" + "01110000010000000100000001110000" + "001"},
	}

	for _, test := range testData {
		t.Run(test.lhs+"^"+test.rhs, func(t *testing.T) {
			lhs, err := Parse(test.lhs)
			if err != nil {
				t.Fatalf("error during parse: %v", err)
			}
			rhs, err := Parse(test.rhs)
			if err != nil {
				t.Fatalf("error during parse: %v", err)
			}
			res := Xor(lhs, rhs)
			resStr := res.String()
			if resStr != test.result {
				t.Fatalf("concat expected %q, got %q", test.result, resStr)
			}

			res2, err := Parse(resStr)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(res2, res) {
				t.Fatalf("result does not roundrip, expected %s, got %s", res.viz(), res2.viz())
			}
		})
	}
}

func TestNot(t *testing.T) {
	testData := []struct {
		val, res string
	}{
		{"", ""},
		{"0", "1"},
		{"1", "0"},
		{"1100", "0011"},
		{"10111010101111101111101011001110" + "11001010111111101011101010111110",
			"01000101010000010000010100110001" + "00110101000000010100010101000001"},
		{"10111010101111101111101011001110" + "11001010111111101011101010111110" + "111",
			"01000101010000010000010100110001" + "00110101000000010100010101000001" + "000"},
	}

	for _, test := range testData {
		t.Run(test.val, func(t *testing.T) {
			ba, err := Parse(test.val)
			if err != nil {
				t.Fatal(err)
			}

			res := Not(ba)
			resStr := res.String()
			if resStr != test.res {
				t.Fatalf("not: expected %q, got %q", test.res, resStr)
			}

			res2, err := Parse(resStr)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(res2, res) {
				t.Fatalf("result does not roundrip, expected %s, got %s", res.viz(), res2.viz())
			}
		})
	}
}

func TestNext(t *testing.T) {
	testData := []struct {
		val, res string
	}{
		{"", "0"},
		{"0", "00"},
		{"1", "10"},
		{"1100", "11000"},
		{"10111010101111101111101011001110" + "11001010111111101011101010111110",
			"10111010101111101111101011001110" + "11001010111111101011101010111110" + "0"},
	}

	for _, test := range testData {
		t.Run(test.val, func(t *testing.T) {
			ba, err := Parse(test.val)
			if err != nil {
				t.Fatal(err)
			}

			res := Next(ba)
			resStr := res.String()
			if resStr != test.res {
				t.Fatalf("not: expected %q, got %q", test.res, resStr)
			}

			res2, err := Parse(resStr)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(res2, res) {
				t.Fatalf("result does not roundrip, expected %s, got %s", res.viz(), res2.viz())
			}
		})
	}
}

func TestCompare(t *testing.T) {
	const lt = -1
	const eq = 0
	const gt = 1
	const big = "10111010101111101111101011001110" + "11001010111111101011101010111110"
	testData := []struct {
		lhs string
		cmp int
		rhs string
	}{
		{"", eq, ""},
		{"", lt, "0"},
		{"0", lt, "1"},
		{"", lt, "1"},
		{"0", eq, "0"},
		{"0", gt, ""},
		{"1", gt, "0"},
		{"1", gt, ""},
		{"1", eq, "1"},
		{"0", lt, "0000"},
		{"0000", lt, "00001"},
		{"00001", lt, "0001"},
		{"0001", lt, "00100"},
		{"00100", lt, "00110"},
		{"00110", lt, "010"},
		{"010", lt, "01001001010101"},
		{"01001001010101", lt, "1"},
		{"1", lt, "10"},
		{"10", lt, "1001001010101"},
		{"1001001010101", lt, "11"},
		{"11", lt, "11001001010101"},
	}

	for _, prefix := range []string{"", big} {
		for _, test := range testData {
			const chars = "<=>"
			t.Run(prefix+test.lhs+chars[test.cmp+1:test.cmp+2]+prefix+test.rhs, func(t *testing.T) {
				lhs, err := Parse(prefix + test.lhs)
				if err != nil {
					t.Fatal(err)
				}
				rhs, err := Parse(prefix + test.rhs)
				if err != nil {
					t.Fatal(err)
				}
				cmp := Compare(lhs, rhs)
				if cmp != test.cmp {
					t.Fatalf("compare expected %d, got %d", test.cmp, cmp)
				}
			})
		}
	}
}

func TestShift(t *testing.T) {
	const big = "10111010101111101111101011001110" + "11001010111111101011101010111110" +
		"11001010111111101011101010111110" + "10111010101111101111101011001110"

	const big2s = "1111 0101 0011 1011 1110 1001 0010 1100" +
		"0110 1110 1111 0011 0001 1010 1100 0110" +
		"0000 0011 1101 1110 0101 0100 0010 1010"
	big2 := strings.Replace(big2s, " ", "", -1)

	testData := []struct {
		src string
		sh  int64
		res string
	}{
		{"", 0, ""},
		{"", 1, ""},
		{"", -1, ""},
		{"", 100, ""},
		{"", -100, ""},
		{"0", 0, "0"},
		{"0", 1, "0"},
		{"0", -1, "0"},
		{"0", 100, "0"},
		{"0", -100, "0"},
		{"1", 0, "1"},
		{"1", 1, "0"},
		{"1", -1, "0"},
		{"1", 100, "0"},
		{"1", -100, "0"},
		{"010", 0, "010"},
		{"010", 1, "100"},
		{"010", -1, "001"},
		{"010", 100, "000"},
		{"010", -100, "000"},
		{big, 1, big[1:] + "0"},
		{big, -1, "0" + big[:len(big)-1]},
		{big, 100, big[100:] + fmt.Sprintf("%0*d", 100, 0)},
		{big, -100, fmt.Sprintf("%0*d", 100, 0) + big[:len(big)-100]},
		{big2, 32, big2[32:] + fmt.Sprintf("%0*d", 32, 0)},
		// Regression test for #36606.
		{big2, -32, fmt.Sprintf("%0*d", 32, 0) + big2[:len(big2)-32]},
		{big2, 31, big2[31:] + fmt.Sprintf("%0*d", 31, 0)},
		// Regression test for #36606.
		{big2, -31, fmt.Sprintf("%0*d", 31, 0) + big2[:len(big2)-31]},
	}

	for _, test := range testData {
		t.Run(fmt.Sprintf("%s<<%d", test.src, test.sh), func(t *testing.T) {
			val, err := Parse(test.src)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("source val:\n%s", val.viz())
			res := val.LeftShiftAny(test.sh)
			resStr := res.String()
			if resStr != test.res {
				t.Fatalf("expected %q, got %q", test.res, resStr)
			}

			res2, err := Parse(resStr)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(res2, res) {
				t.Fatalf("result does not roundrip, expected:\n%s, got:\n%s", res.viz(), res2.viz())
			}
		})
	}
}

func TestConcat(t *testing.T) {
	testData := []struct {
		lhs, rhs, result string
	}{
		{"", "", ""},
		{"0", "", "0"},
		{"", "0", "0"},
		{"0", "0", "00"},
		{"1111", "00", "111100"},
		{"11", "0000", "110000"},
		{"10111010101111101111101011001110" + "11001010111111101011101010111110", "000",
			"10111010101111101111101011001110" + "11001010111111101011101010111110" + "000"},
		{"10111010101111101111101011001110", "11001010111111101011101010111110" + "000",
			"10111010101111101111101011001110" + "11001010111111101011101010111110" + "000"},
		{"000", "10111010101111101111101011001110" + "11001010111111101011101010111110",
			"000" + "10111010101111101111101011001110" + "11001010111111101011101010111110"},
		{"000" + "10111010101111101111101011001110", "11001010111111101011101010111110",
			"000" + "10111010101111101111101011001110" + "11001010111111101011101010111110"},
		{"10111010101111101111101011001110" + "11001010111111101011101010111110", "001",
			"10111010101111101111101011001110" + "11001010111111101011101010111110" + "001"},
		{"10111010101111101111101011001110", "11001010111111101011101010111110" + "001",
			"10111010101111101111101011001110" + "11001010111111101011101010111110" + "001"},
		{"001", "10111010101111101111101011001110" + "11001010111111101011101010111110",
			"001" + "10111010101111101111101011001110" + "11001010111111101011101010111110"},
		{"001" + "10111010101111101111101011001110", "11001010111111101011101010111110",
			"001" + "10111010101111101111101011001110" + "11001010111111101011101010111110"},
	}

	for _, test := range testData {
		t.Run(test.lhs+"+"+test.rhs, func(t *testing.T) {
			lhs, err := Parse(test.lhs)
			if err != nil {
				t.Fatalf("error during parse: %v", err)
			}
			rhs, err := Parse(test.rhs)
			if err != nil {
				t.Fatalf("error during parse: %v", err)
			}
			res := Concat(lhs, rhs)
			resStr := res.String()
			if resStr != test.result {
				t.Fatalf("concat expected %q, got %q", test.result, resStr)
			}

			res2, err := Parse(resStr)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(res2, res) {
				t.Fatalf("result does not roundrip, expected %s, got %s", res.viz(), res2.viz())
			}
		})
	}
}

func TestConcatRand(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()

	for i := 0; i < 1000; i++ {
		w1 := uint(rng.Int31n(256))
		w2 := uint(rng.Int31n(256))
		lhs := Rand(rng, w1)
		rhs := Rand(rng, w2)

		r1c := Concat(lhs, rhs)
		r1 := r1c.String()
		r2 := lhs.String() + rhs.String()
		if r1 != r2 {
			t.Errorf("%q || %q: expected %q, got %q", lhs, rhs, r2, r1)
		}

		res2, err := Parse(r1)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(res2, r1c) {
			t.Fatalf("result does not roundrip, expected %s, got %s", r1c.viz(), res2.viz())
		}

	}
}

func TestGetBitAtIndex(t *testing.T) {
	testData := []struct {
		bitString, err string
		index, res     int
	}{
		{"1001010", "", 0, 1},
		{"1010101", "", 1, 0},
		{"111111111111110001010101000000", "", 20, 0},
		{"111111111111110001011101000000", "", 20, 1},
		{"11111111", "", 7, 1},
		{"010110", "GetBitAtIndex: bit index -1 out of valid range (0..5)", -1, 0},
		{"", "GetBitAtIndex: bit index 0 out of valid range (0..-1)", 0, 0},
		{"10100110", "GetBitAtIndex: bit index 8 out of valid range (0..7)", 8, 0},
	}
	for _, test := range testData {
		t.Run(fmt.Sprintf("{%v,%d}", test.bitString, test.index), func(t *testing.T) {
			ba, err := Parse(test.bitString)
			if err != nil {
				t.Fatal(err)
			}
			res, err := ba.GetBitAtIndex(test.index)
			if test.err != "" && (err == nil || test.err != err.Error()) {
				t.Errorf("expected %q error, but got: %+v", test.err, err)
			} else if test.err == "" && err != nil {
				t.Errorf("unexpected error: %s", err.Error())
			} else if !reflect.DeepEqual(test.res, res) {
				t.Errorf("expected %d, got %d", test.res, res)
			}
		})
	}
}

func TestSetBitAtIndex(t *testing.T) {
	testData := []struct {
		bitString, err, res string
		index, toSet        int
	}{
		{"1001010", "", "1101010", 1, 1},
		{"1010101", "", "0010101", 0, 0},
		{"1010101011", "", "1010101011", 0, 1},
		{"1010101011", "", "1010101011", 1, 0},
		{"111111111111110001010101000000", "", "111111111111110001011101000000", 20, 1},
		{"010110", "SetBitAtIndex: bit index -1 out of valid range (0..5)", "", -1, 0},
		{"10100110", "SetBitAtIndex: bit index 8 out of valid range (0..7)", "", 8, 0},
		{"", "SetBitAtIndex: bit index 0 out of valid range (0..-1)", "", 0, 0},
	}
	for _, test := range testData {
		t.Run(fmt.Sprintf("{%v,%d,%d}", test.bitString, test.index, test.toSet), func(t *testing.T) {
			ba, err := Parse(test.bitString)
			if err != nil {
				t.Fatal(err)
			}
			res, err := ba.SetBitAtIndex(test.index, test.toSet)
			if test.err != "" && (err == nil || test.err != err.Error()) {
				t.Errorf("expected %q error, but got: %+v", test.err, err)
			} else if test.err == "" && err != nil {
				t.Errorf("unexpected error: %s", err.Error())
			} else if !reflect.DeepEqual(test.res, res.String()) {
				t.Errorf("expected %s, got %s", test.res, res.String())
			}
		})
	}
}
