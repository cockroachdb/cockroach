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

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/util"
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
func EncodeDecimalAscending(b []byte, d *inf.Dec) []byte {
	return encodeDecimal(b, d, false)
}

// EncodeDecimalDescending is the descending version of EncodeDecimalAscending.
func EncodeDecimalDescending(b []byte, d *inf.Dec) []byte {
	return encodeDecimal(b, d, true)
}

func encodeDecimal(b []byte, d *inf.Dec, invert bool) []byte {
	// Handle the simplistic cases first.
	neg := false
	switch d.Sign() {
	case -1:
		neg = true
	case 0:
		return append(b, floatZero)
	}
	// TODO(nvanbenschoten) Switch to a base-256 mantissa.
	e, m := decimalMandE(d, b[len(b):])
	return encodeMandE(b, neg != invert, e, m)
}

// decimalMandE computes and returns the mantissa M and exponent E for d.
//
// The mantissa is a base-100 representation of the value. The exponent E
// determines where to put the decimal point.
//
// Each centimal digit of the mantissa is stored in a byte. If the value of the
// centimal digit is X (hence X>=0 and X<=99) then the byte value will be 2*X+1
// for every byte of the mantissa, except for the last byte which will be
// 2*X+0. The mantissa must be the minimum number of bytes necessary to
// represent the value; trailing X==0 digits are omitted.  This means that the
// mantissa will never contain a byte with the value 0x00.
//
// If we assume all digits of the mantissa occur to the right of the decimal
// point, then the exponent E is the power of one hundred by which one must
// multiply the mantissa to recover the original value.
func decimalMandE(d *inf.Dec, tmp []byte) (int, []byte) {
	bs := d.UnscaledBig().String()
	if bs[0] == '-' {
		bs = bs[1:]
	}

	// The exponent will be the combination of the decimal's exponent, and the
	// number of digits in the big.Int. Note that the decimal's exponent is
	// given by the inverse of dec.Scale.
	e10 := int(-d.Scale()) + len(bs)

	// Strip off trailing zeros in big.Int's string representation.
	for bs[len(bs)-1] == '0' {
		bs = bs[:len(bs)-1]
	}

	// Make sure b is large enough to hold the smallest even number of bytes
	// greater than or equal to the size of bs + 1.
	if n := 2 * ((len(bs) + 2) / 2); n <= cap(tmp) {
		tmp = tmp[:len(bs)+1]
	} else {
		tmp = make([]byte, len(bs)+1, n)
	}
	tmp[0] = '0'
	copy(tmp[1:], []byte(bs))

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
		tmp = tmp[1:]
	}

	// Ensure that the number of digits is even.
	if len(tmp)%2 != 0 {
		tmp = append(tmp, '0')
	}

	// Convert the base-10 'b' slice to a base-100 'm' slice. We do this
	// conversion in place to avoid an allocation.
	m := tmp[:len(tmp)/2]
	for i := 0; i < len(tmp); i += 2 {
		accum := 10*int(tmp[i]-'0') + int(tmp[i+1]-'0')
		// The bytes are encoded as 2n+1.
		m[i/2] = byte(2*accum + 1)
	}
	// The last byte is encoded as 2n+0.
	m[len(m)-1]--

	return e100, m
}

// DecodeDecimalAscending returns the remaining byte slice after decoding and the decoded
// decimal from buf.
func DecodeDecimalAscending(buf []byte, tmp []byte) ([]byte, *inf.Dec, error) {
	return decodeDecimal(buf, tmp, false)
}

// DecodeDecimalDescending decodes floats encoded with EncodeDecimalDescending.
func DecodeDecimalDescending(buf []byte, tmp []byte) ([]byte, *inf.Dec, error) {
	return decodeDecimal(buf, tmp, true)
}

func decodeDecimal(buf []byte, tmp []byte, invert bool) ([]byte, *inf.Dec, error) {
	// Handle the simplistic cases first.
	switch buf[0] {
	case floatNaN, floatNaNDesc:
		return nil, nil, util.Errorf("decimal does not support NaN values: %q", buf)
	case floatInfinity, floatNegativeInfinity:
		return nil, nil, util.Errorf("decimal does not support infinite values: %q", buf)
	case floatZero:
		return buf[1:], inf.NewDec(0, 0), nil
	}
	tmp = tmp[len(tmp):cap(tmp)]
	idx := bytes.IndexByte(buf, floatTerminator)
	if idx == -1 {
		return nil, nil, util.Errorf("did not find terminator %#x in buffer %#x", floatTerminator, buf)
	}
	switch {
	case buf[0] == floatNegLarge:
		// Negative large.
		e, m, tmp2 := decodeLargeNumber(true, buf[:idx+1], tmp)
		return buf[idx+1:], makeDecimalFromMandE(!invert, e, m, tmp2), nil
	case buf[0] > floatNegLarge && buf[0] <= floatNegMedium:
		// Negative medium.
		e, m, tmp2 := decodeMediumNumber(true, buf[:idx+1], tmp)
		return buf[idx+1:], makeDecimalFromMandE(!invert, e, m, tmp2), nil
	case buf[0] == floatNegSmall:
		// Negative small.
		e, m, tmp2 := decodeSmallNumber(true, buf[:idx+1], tmp)
		return buf[idx+1:], makeDecimalFromMandE(!invert, e, m, tmp2), nil
	case buf[0] == floatPosLarge:
		// Positive large.
		e, m, tmp2 := decodeLargeNumber(false, buf[:idx+1], tmp)
		return buf[idx+1:], makeDecimalFromMandE(invert, e, m, tmp2), nil
	case buf[0] >= floatPosMedium && buf[0] < floatPosLarge:
		// Positive medium.
		e, m, tmp2 := decodeMediumNumber(false, buf[:idx+1], tmp)
		return buf[idx+1:], makeDecimalFromMandE(invert, e, m, tmp2), nil
	case buf[0] == floatPosSmall:
		// Positive small.
		e, m, tmp2 := decodeSmallNumber(false, buf[:idx+1], tmp)
		return buf[idx+1:], makeDecimalFromMandE(invert, e, m, tmp2), nil
	default:
		return nil, nil, util.Errorf("unknown prefix of the encoded byte slice: %q", buf)
	}
}

// makeDecimalFromMandE reconstructs the decimal from the mantissa M and
// exponent E.
func makeDecimalFromMandE(negative bool, e int, m []byte, tmp []byte) *inf.Dec {
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

	return inf.NewDecBig(bi, inf.Scale(-exp))
}
