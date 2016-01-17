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
// numeric values. The encoding assumes that any number can be writen
// as ±0.xyz... * 10^exp, where xyz is a digit string, x != 0, and the
// last decimal in xyz is also not 0.
//
// The encoding uses its first byte to split decimals into 7 distinct
// ordered groups (no NaN or Infinity support yet). The groups can
// be seen in encoding.go starting on line 51. Following this, the
// absolute value of the exponent of the decimal (as defined above)
// is encoded as an unsigned varint. Next, the absolute value of
// the digit string is added as a big-endian byte slice. Finally,
// a null terminator is appended to the end.
//
// TODO(nvanbenschoten) if we end up keeping this encoding, improve
// this explanation.
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package encoding

import (
	"bytes"
	"math/big"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/decimal"
)

// EncodeDecimalAscending ... TODO(nvanbenschoten)
func EncodeDecimalAscending(b []byte, d decimal.Decimal) []byte {
	return encodeDecimal(b, d, false)
}

// EncodeDecimalDescending ... TODO(nvanbenschoten)
func EncodeDecimalDescending(b []byte, d decimal.Decimal) []byte {
	return encodeDecimal(b, d, true)
}

func encodeDecimal(b []byte, d decimal.Decimal, invert bool) []byte {
	neg := false
	switch d.BigInt().Sign() {
	case -1:
		neg = true
	case 0:
		return append(b, decimalZero)
	}
	neg = xor(neg, invert)

	// Make a deep copy of the decimal's big.Int.
	bi := new(big.Int)
	bi.SetBytes(d.BigInt().Bytes())

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
		ten := big.NewInt(10)
		pow := big.NewInt(int64(tens))
		bi = bi.Div(bi, ten.Exp(ten, pow, nil))
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

func encodeDecimalValue(negVal, negExp bool, exp uint64, intBytes []byte, buf []byte) int {
	n := putUvarint(buf, exp)

	copy(buf[n:], intBytes)
	l := n + len(intBytes)

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

func encodeDecimalValueWithoutExp(negVal bool, intBytes []byte, buf []byte) int {
	copy(buf[:], intBytes)
	l := len(intBytes)

	if negVal {
		onesComplement(buf[:l])
	}

	buf[l] = decimalTerminator
	return l
}

// DecodeDecimalAscending ... TODO(nvanbenschoten)
func DecodeDecimalAscending(buf []byte, tmp []byte) ([]byte, decimal.Decimal, error) {
	return decodeDecimal(buf, tmp, false)
}

// DecodeDecimalDescending ... TODO(nvanbenschoten)
func DecodeDecimalDescending(buf []byte, tmp []byte) ([]byte, decimal.Decimal, error) {
	return decodeDecimal(buf, tmp, true)
}

func decodeDecimal(buf []byte, tmp []byte, invert bool) ([]byte, decimal.Decimal, error) {
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
		bi, exp := decodeDecimalValue(true, false, buf[:idx+1], tmp)
		if !invert {
			bi = bi.Neg(bi)
		}
		return buf[idx+1:], decimal.NewFromBigInt(bi, int32(exp)), nil
	case buf[0] == decimalNegValZeroExp:
		bi := decodeDecimalValueWithoutExp(true, buf[:idx+1], tmp)
		if !invert {
			bi = bi.Neg(bi)
		}
		return buf[idx+1:], decimal.NewFromBigInt(bi, -1), nil
	case buf[0] == decimalNegValNegExp:
		bi, exp := decodeDecimalValue(true, true, buf[:idx+1], tmp)
		if !invert {
			bi = bi.Neg(bi)
		}
		return buf[idx+1:], decimal.NewFromBigInt(bi, int32(exp)), nil
	case buf[0] == decimalPosValNegExp:
		bi, exp := decodeDecimalValue(false, true, buf[:idx+1], tmp)
		if invert {
			bi = bi.Neg(bi)
		}
		return buf[idx+1:], decimal.NewFromBigInt(bi, int32(exp)), nil
	case buf[0] == decimalPosValZeroExp:
		bi := decodeDecimalValueWithoutExp(false, buf[:idx+1], tmp)
		if invert {
			bi = bi.Neg(bi)
		}
		return buf[idx+1:], decimal.NewFromBigInt(bi, -1), nil
	case buf[0] == decimalPosValPosExp:
		bi, exp := decodeDecimalValue(false, false, buf[:idx+1], tmp)
		if invert {
			bi = bi.Neg(bi)
		}
		return buf[idx+1:], decimal.NewFromBigInt(bi, int32(exp)), nil
	default:
		return nil, decimal.Decimal{}, util.Errorf("unknown prefix of the encoded byte slice: %q", buf)
	}
}

func decodeDecimalValue(negVal, negExp bool, buf []byte, tmp []byte) (*big.Int, int) {
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

func decodeDecimalValueWithoutExp(negVal bool, buf []byte, tmp []byte) *big.Int {
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
	return (a || b) && !(a && b)
}
