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
// An ordered key encoding scheme for arbitrary-precision fixed-point
// numeric values based on sqlite4's key encoding:
// http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package encoding

import (
	"bytes"
	"math/big"
	"unsafe"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/decimal"
)

// EncodeDecimalAscending returns the resulting byte slice with the encoded decimal
// appended to b.
//
// Values are classified as large, medium, or small according to the value of
// E. If E is 11 or more, the value is large. For E between 0 and 10, the value
// is medium. For E less than zero, the value is small.
//
// Large positive values are encoded as a single byte 0x22 followed by E as a
// varint and then M. Medium positive values are a single byte of 0x17+E
// followed by M. Small positive values are encoded as a single byte 0x16
// followed by the ones-complement of the varint for -E followed by M.
//
// Small negative values are encoded as a single byte 0x14 followed by -E as a
// varint and then the ones-complement of M. Medium negative values are encoded
// as a byte 0x13-E followed by the ones-complement of M. Large negative values
// consist of the single byte 0x08 followed by the ones-complement of the
// varint encoding of E followed by the ones-complement of M.
func EncodeDecimalAscending(b []byte, d decimal.Decimal) []byte {
	return encodeDecimal(b, d, false)
}

// EncodeDecimalDescending is the descending version of EncodeDecimalAscending.
func EncodeDecimalDescending(b []byte, d decimal.Decimal) []byte {
	return encodeDecimal(b, d, true)
}

// EncodeDecimalAscendingOld returns the resulting byte slice with the
// encoded decimal appended to b.
//
// The encoding assumes that any number can be writen as ±0.xyz... * 10^exp,
// where xyz is a digit string, x != 0, and the last decimal in xyz is also
// not 0.
//
// The encoding uses its first byte to split decimals into 7 distinct
// ordered groups (no NaN or Infinity support yet). The groups can
// be seen in encoding.go starting on line 51. Following this, the
// absolute value of the exponent of the decimal (as defined above)
// is encoded as an unsigned varint. Next, the absolute value of
// the digit string is added as a big-endian byte slice. Finally,
// a null terminator is appended to the end.
func EncodeDecimalAscendingOld(b []byte, d decimal.Decimal) []byte {
	return encodeDecimalOld(b, d, false)
}

// EncodeDecimalDescendingOld is the descending version of EncodeDecimalAscendingOld.
func EncodeDecimalDescendingOld(b []byte, d decimal.Decimal) []byte {
	return encodeDecimalOld(b, d, true)
}

func encodeDecimal(b []byte, d decimal.Decimal, invert bool) []byte {
	// Handle the simplistic cases first.
	neg := false
	switch d.BigInt().Sign() {
	case -1:
		neg = true
	case 0:
		return append(b, floatZero)
	}
	e, m := decimalMandE(b, d)
	return encodeMandE(b, xor(neg, invert), e, m)
}

func decimalMandE(b []byte, d decimal.Decimal) (int, []byte) {
	bs := d.BigInt().String()
	if bs[0] == '-' {
		bs = bs[1:]
	}

	// The exponent will be the combintation of the decimal's exponent, and the
	// number of digits in the big.Int.
	e10 := int(d.Exponent()) + len(bs)

	// Strip off trailing zeros in big.Int's string representation.
	for bs[len(bs)-1] == '0' {
		bs = bs[:len(bs)-1]
	}

	if n := 2 * ((len(bs) + 2) / 2); n <= cap(b)-len(b) {
		b = b[len(b) : len(b)+len(bs)+1]
	} else {
		b = make([]byte, len(bs)+1, n)
	}
	b[0] = '0'
	copy(b[1:], []byte(bs))

	// Convert the power-10 exponent to a power of 100 exponent.
	var e100 int
	if e10 >= 0 {
		e100 = (e10 + 1) / 2
	} else {
		e100 = e10 / 2
	}
	// Strip the leading 0 if the conversion to e100 did not add a multiple of
	// 10.
	if e100*2 == e10 {
		b = b[1:]
	}

	// Ensure that the number of digits is even.
	if len(b)%2 != 0 {
		b = append(b, '0')
	}

	// Convert the base-10 'b' slice to a base-100 'm' slice. We do this
	// conversion in place to avoid an allocation.
	m := b[:len(b)/2]
	for i := 0; i < len(b); i += 2 {
		accum := 10*int(b[i]-'0') + int(b[i+1]-'0')
		// The bytes are encoded as 2n+1.
		m[i/2] = byte(2*accum + 1)
	}
	// The last byte is encoded as 2n+0.
	m[len(m)-1]--

	return e100, m
}

func encodeDecimalOld(b []byte, d decimal.Decimal, invert bool) []byte {
	neg := false
	bi := d.BigInt()
	switch bi.Sign() {
	case -1:
		neg = true

		// Make a deep copy of the decimal's big.Int by calling Neg.
		// We shouldn't be modifying the provided argument's
		// internal big.Int, so this works like a copy-on-write scheme.
		bi = new(big.Int)
		bi = bi.Neg(d.BigInt())
	case 0:
		return append(b, decimalZero)
	}
	neg = xor(neg, invert)

	// Determine the exponent of the decimal, with the
	// exponent defined as .xyz * 10^exp.
	bs := bi.String()
	e := int(d.Exponent()) + len(bs)

	// Handle big.Int having zeros at the end of it's
	// string by diving them off (ie. 12300 -> 123).
	tens := 0
	for {
		if bs[len(bs)-1-tens] == '0' {
			tens++
		} else {
			break
		}
	}
	if tens > 0 {
		// If the decimal's big.Int hasn't been copied already, copy
		// it now because we will be modifying it.
		from := bi
		if bi == d.BigInt() {
			bi = new(big.Int)
			from = d.BigInt()
		}

		div := big.NewInt(10)
		if tens > 1 {
			pow := big.NewInt(int64(tens))
			div = div.Exp(div, pow, nil)
		}
		bi = bi.Div(from, div)
	}
	bb := bi.Bytes()

	var buf []byte
	if n := len(bb) + maxVarintSize + 2; n <= cap(b)-len(b) {
		buf = b[len(b) : len(b)+n]
	} else {
		buf = make([]byte, len(bb)+maxVarintSize+2)
	}

	switch {
	case neg && e > 0:
		buf[0] = decimalNegValPosExp
		n := encodeDecimalValue(true, false, uint64(e), bb, buf[1:])
		return append(b, buf[:n+2]...)
	case neg && e == 0:
		buf[0] = decimalNegValZeroExp
		n := encodeDecimalValueWithoutExp(true, bb, buf[1:])
		return append(b, buf[:n+2]...)
	case neg && e < 0:
		buf[0] = decimalNegValNegExp
		n := encodeDecimalValue(true, true, uint64(-e), bb, buf[1:])
		return append(b, buf[:n+2]...)
	case !neg && e < 0:
		buf[0] = decimalPosValNegExp
		n := encodeDecimalValue(false, true, uint64(-e), bb, buf[1:])
		return append(b, buf[:n+2]...)
	case !neg && e == 0:
		buf[0] = decimalPosValZeroExp
		n := encodeDecimalValueWithoutExp(false, bb, buf[1:])
		return append(b, buf[:n+2]...)
	case !neg && e > 0:
		buf[0] = decimalPosValPosExp
		n := encodeDecimalValue(false, false, uint64(e), bb, buf[1:])
		return append(b, buf[:n+2]...)
	}
	panic("unreachable")
}

// encodeDecimalValue encodes the absolute value of a decimal's exponent
// and slice of digit bytes into buf, returnining the number of bytes written.
// The function first encodes the the absolute value of a decimal's exponent
// as an unsigned varint. Next, the function copies the decimal's digits
// into the buffer. Finally, the function appends a decimal terminator to the
// end of the buffer. encodeDecimalValue reacts to positive/negative values and
// exponents by performing the proper ones complements to ensure proper logical
// sorting of values encoded in buf.
func encodeDecimalValue(negVal, negExp bool, exp uint64, digits []byte, buf []byte) int {
	n := putUvarint(buf, exp)

	copy(buf[n:], digits)
	l := n + len(digits)

	switch {
	case negVal && negExp:
		onesComplement(buf[n:l])
	case negVal:
		onesComplement(buf[:l])
	case negExp:
		onesComplement(buf[:n])
	}

	buf[l] = decimalTerminator
	return l
}

func encodeDecimalValueWithoutExp(negVal bool, digits []byte, buf []byte) int {
	copy(buf[:], digits)
	l := len(digits)

	if negVal {
		onesComplement(buf[:l])
	}

	buf[l] = decimalTerminator
	return l
}

// DecodeDecimalAscending returns the remaining byte slice after decoding and the decoded
// decimal from buf.
func DecodeDecimalAscending(buf []byte, tmp []byte) ([]byte, decimal.Decimal, error) {
	return decodeDecimal(buf, tmp, false)
}

// DecodeDecimalDescending decodes floats encoded with EncodeDecimalDescending.
func DecodeDecimalDescending(buf []byte, tmp []byte) ([]byte, decimal.Decimal, error) {
	return decodeDecimal(buf, tmp, true)
}

// DecodeDecimalAscendingOld returns the remaining byte slice after decoding and the decoded
// decimal from buf.
func DecodeDecimalAscendingOld(buf []byte, tmp []byte) ([]byte, decimal.Decimal, error) {
	return decodeDecimalOld(buf, tmp, false)
}

// DecodeDecimalDescendingOld decodes floats encoded with EncodeDecimalDescendingOld.
func DecodeDecimalDescendingOld(buf []byte, tmp []byte) ([]byte, decimal.Decimal, error) {
	return decodeDecimalOld(buf, tmp, true)
}

func decodeDecimal(buf []byte, tmp []byte, invert bool) ([]byte, decimal.Decimal, error) {
	if buf[0] == floatZero {
		return buf[1:], decimal.Decimal{}, nil
	}
	tmp = tmp[len(tmp):cap(tmp)]
	idx := bytes.Index(buf, []byte{floatTerminator})
	switch {
	// case buf[0] == floatNaN:
	// return buf[1:], math.NaN(), nil
	// case buf[0] == floatNaNDesc:
	// return buf[1:], math.NaN(), nil
	// case buf[0] == floatInfinity:
	// return buf[1:], math.Inf(1), nil
	// case buf[0] == floatNegativeInfinity:
	// return buf[1:], math.Inf(-1), nil
	case buf[0] == floatNegLarge:
		// Negative large.
		e, m := decodeLargeNumber(true, buf[:idx+1], tmp)
		return buf[idx+1:], makeDecimalFromMandE(!invert, e, m, tmp), nil
	case buf[0] > floatNegLarge && buf[0] <= floatNegMedium:
		// Negative medium.
		e, m := decodeMediumNumber(true, buf[:idx+1], tmp)
		return buf[idx+1:], makeDecimalFromMandE(!invert, e, m, tmp), nil
	case buf[0] == floatNegSmall:
		// Negative small.
		e, m := decodeSmallNumber(true, buf[:idx+1], tmp)
		return buf[idx+1:], makeDecimalFromMandE(!invert, e, m, tmp), nil
	case buf[0] == floatPosLarge:
		// Positive large.
		e, m := decodeLargeNumber(false, buf[:idx+1], tmp)
		return buf[idx+1:], makeDecimalFromMandE(invert, e, m, tmp), nil
	case buf[0] >= floatPosMedium && buf[0] < floatPosLarge:
		// Positive medium.
		e, m := decodeMediumNumber(false, buf[:idx+1], tmp)
		return buf[idx+1:], makeDecimalFromMandE(invert, e, m, tmp), nil
	case buf[0] == floatPosSmall:
		// Positive small.
		e, m := decodeSmallNumber(false, buf[:idx+1], tmp)
		return buf[idx+1:], makeDecimalFromMandE(invert, e, m, tmp), nil
	default:
		return nil, decimal.Decimal{}, util.Errorf("unknown prefix of the encoded byte slice: %q", buf)
	}
}

func makeDecimalFromMandE(negative bool, e int, m []byte, tmp []byte) decimal.Decimal {
	// ±dddd.
	b := tmp[:0]
	if n := len(m)*2 + 1; cap(b) < n {
		b = make([]byte, 0, n)
	}
	if negative {
		b = append(b, '-')
	}
	for i, v := range m {
		t := int(v)
		if i == len(m) {
			t--
		}
		t /= 2
		b = append(b, byte(t/10)+'0', byte(t%10)+'0')
	}

	// We unsafely convert the []byte to a string to avoid the usual allocation
	// when converting to a string.
	bi := new(big.Int)
	bi, ok := bi.SetString(*(*string)(unsafe.Pointer(&b)), 10)
	if !ok {
		panic("could not set big.Int's string value")
	}

	exp := 2*e - len(b)
	if negative {
		exp++
	}

	return decimal.NewFromBigInt(bi, int32(exp))
}

func decodeDecimalOld(buf []byte, tmp []byte, invert bool) ([]byte, decimal.Decimal, error) {
	switch {
	// case buf[0] == decimalNaN:
	// case buf[0] == decimalNegativeInfinity:
	// case buf[0] == decimalInfinity:
	// case buf[0] == decimalNaNDesc:
	case buf[0] == decimalZero:
		return buf[1:], decimal.Decimal{}, nil
	}

	idx := bytes.Index(buf, []byte{decimalTerminator})
	switch {
	case buf[0] == decimalNegValPosExp:
		bi, exp := decodeDecimalOldValue(true, false, buf[:idx+1], tmp)
		if !invert {
			bi = bi.Neg(bi)
		}
		return buf[idx+1:], decimal.NewFromBigInt(bi, int32(exp)), nil
	case buf[0] == decimalNegValZeroExp:
		bi := decodeDecimalOldValueWithoutExp(true, buf[:idx+1], tmp)
		if !invert {
			bi = bi.Neg(bi)
		}
		return buf[idx+1:], decimal.NewFromBigInt(bi, -1), nil
	case buf[0] == decimalNegValNegExp:
		bi, exp := decodeDecimalOldValue(true, true, buf[:idx+1], tmp)
		if !invert {
			bi = bi.Neg(bi)
		}
		return buf[idx+1:], decimal.NewFromBigInt(bi, int32(exp)), nil
	case buf[0] == decimalPosValNegExp:
		bi, exp := decodeDecimalOldValue(false, true, buf[:idx+1], tmp)
		if invert {
			bi = bi.Neg(bi)
		}
		return buf[idx+1:], decimal.NewFromBigInt(bi, int32(exp)), nil
	case buf[0] == decimalPosValZeroExp:
		bi := decodeDecimalOldValueWithoutExp(false, buf[:idx+1], tmp)
		if invert {
			bi = bi.Neg(bi)
		}
		return buf[idx+1:], decimal.NewFromBigInt(bi, -1), nil
	case buf[0] == decimalPosValPosExp:
		bi, exp := decodeDecimalOldValue(false, false, buf[:idx+1], tmp)
		if invert {
			bi = bi.Neg(bi)
		}
		return buf[idx+1:], decimal.NewFromBigInt(bi, int32(exp)), nil
	default:
		return nil, decimal.Decimal{}, util.Errorf("unknown prefix of the encoded byte slice: %q", buf)
	}
}

func decodeDecimalOldValue(negVal, negExp bool, buf []byte, tmp []byte) (*big.Int, int) {
	var m []byte
	if n := len(buf); n <= len(tmp) {
		m = tmp[:n]
	} else {
		m = make([]byte, n)
	}
	copy(m, buf)

	if xor(negVal, negExp) {
		onesComplement(m[1:])
	}

	e, l := getUvarint(m[1:])
	if negExp {
		e = -e
		onesComplement(m[1+l:])
	}

	bi := new(big.Int)
	bi.SetBytes(m[l+1 : len(m)-1])
	bs := bi.String()
	exp := int(e) - len(bs)
	return bi, exp
}

func decodeDecimalOldValueWithoutExp(negVal bool, buf []byte, tmp []byte) *big.Int {
	var m []byte
	if n := len(buf); n <= len(tmp) {
		m = tmp[:n]
	} else {
		m = make([]byte, n)
	}
	copy(m, buf)

	if negVal {
		onesComplement(m[1:])
	}

	bi := new(big.Int)
	return bi.SetBytes(m[1 : len(m)-1])
}

func xor(a, b bool) bool {
	return a != b
}
