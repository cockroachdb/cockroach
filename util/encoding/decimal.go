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
	"math"
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
// Large positive values are encoded as a single byte 0x3c followed by E as a
// varint and then M. Medium positive values are a single byte of 0x31+E
// followed by M. Small positive values are encoded as a single byte 0x30
// followed by a descending varint encoding for -E followed by M.
//
// Small negative values are encoded as a single byte 0x2e followed by -E as a
// varint and then the ones-complement of M. Medium negative values are encoded
// as a byte 0x2d-E followed by the ones-complement of M. Large negative values
// consist of the single byte 0x22 followed by a descending  varint encoding of
// E followed by the ones-complement of M.
func EncodeDecimalAscending(b []byte, d *inf.Dec) []byte {
	return encodeDecimal(b, d, false)
}

// EncodeDecimalDescending is the descending version of EncodeDecimalAscending.
func EncodeDecimalDescending(b []byte, d *inf.Dec) []byte {
	return encodeDecimal(b, d, true)
}

func encodeDecimal(b []byte, d *inf.Dec, invert bool) []byte {
	neg := false
	switch d.UnscaledBig().Sign() {
	case -1:
		neg = true
	case 0:
		return append(b, decimalZero)
	}
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
	addedZero := false
	if cap(tmp) > 0 {
		tmp = tmp[:1]
		tmp[0] = '0'
		addedZero = true
	}
	tmp = d.UnscaledBig().Append(tmp, 10)
	if addedZero {
		if tmp[1] == '-' {
			tmp[1] = '0'
			tmp = tmp[1:]
		}
	} else {
		if tmp[0] == '-' {
			tmp[0] = '0'
		} else {
			tmp = append(tmp, '0')
			copy(tmp[1:], tmp[:len(tmp)-1])
			tmp[0] = '0'
		}
	}

	// The exponent will be the combination of the decimal's exponent, and the
	// number of digits in the big.Int. Note that the decimal's exponent is
	// given by the inverse of dec.Scale.
	e10 := int(-d.Scale()) + len(tmp[1:])

	// Strip off trailing zeros in big.Int's string representation.
	for tmp[len(tmp)-1] == '0' {
		tmp = tmp[:len(tmp)-1]
	}

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

func encodeMandE(b []byte, negative bool, e int, m []byte) []byte {
	var buf []byte
	if n := len(m) + maxVarintSize + 2; n <= cap(b)-len(b) {
		buf = b[len(b) : len(b)+n]
	} else {
		buf = make([]byte, n)
	}
	switch {
	case e < 0:
		return append(b, encodeSmallNumber(negative, e, m, buf)...)
	case e >= 0 && e <= 10:
		return append(b, encodeMediumNumber(negative, e, m, buf)...)
	case e >= 11:
		return append(b, encodeLargeNumber(negative, e, m, buf)...)
	}
	panic("unreachable")
}

func encodeSmallNumber(negative bool, e int, m []byte, buf []byte) []byte {
	var n int
	if negative {
		buf[0] = decimalNegSmall
		n = len(EncodeUvarintAscending(buf[1:1], uint64(-e)))
	} else {
		buf[0] = decimalPosSmall
		n = len(EncodeUvarintDescending(buf[1:1], uint64(-e)))
	}
	copy(buf[1+n:], m)
	l := 1 + n + len(m)
	if negative {
		onesComplement(buf[1+n : l]) // ones complement of mantissa
	}
	buf[l] = decimalTerminator
	return buf[:l+1]
}

func encodeMediumNumber(negative bool, e int, m []byte, buf []byte) []byte {
	copy(buf[1:], m)
	l := 1 + len(m)
	if negative {
		buf[0] = decimalNegMedium - byte(e)
		onesComplement(buf[1:l])
	} else {
		buf[0] = decimalPosMedium + byte(e)
	}
	buf[l] = decimalTerminator
	return buf[:l+1]
}

func encodeLargeNumber(negative bool, e int, m []byte, buf []byte) []byte {
	var n int
	if negative {
		buf[0] = decimalNegLarge
		n = len(EncodeUvarintDescending(buf[1:1], uint64(e)))
	} else {
		buf[0] = decimalPosLarge
		n = len(EncodeUvarintAscending(buf[1:1], uint64(e)))
	}
	copy(buf[1+n:], m)
	l := 1 + n + len(m)
	if negative {
		onesComplement(buf[1+n : l]) // ones complement of mantissa
	}
	buf[l] = decimalTerminator
	return buf[:l+1]
}

// DecodeDecimalAscending returns the remaining byte slice after decoding and the decoded
// decimal from buf.
func DecodeDecimalAscending(buf []byte, tmp []byte) ([]byte, *inf.Dec, error) {
	return decodeDecimal(buf, tmp, false)
}

// DecodeDecimalDescending decodes decimals encoded with EncodeDecimalDescending.
func DecodeDecimalDescending(buf []byte, tmp []byte) ([]byte, *inf.Dec, error) {
	return decodeDecimal(buf, tmp, true)
}

func decodeDecimal(buf []byte, tmp []byte, invert bool) ([]byte, *inf.Dec, error) {
	// Handle the simplistic cases first.
	switch buf[0] {
	case decimalNaN, decimalNaNDesc:
		return nil, nil, util.Errorf("decimal does not support NaN values: %q", buf)
	case decimalInfinity, decimalNegativeInfinity:
		return nil, nil, util.Errorf("decimal does not support infinite values: %q", buf)
	case decimalZero:
		return buf[1:], inf.NewDec(0, 0), nil
	}
	tmp = tmp[len(tmp):cap(tmp)]
	switch {
	case buf[0] == decimalNegLarge:
		// Negative large.
		e, m, r, tmp2, err := decodeLargeNumber(true, buf, tmp)
		if err != nil {
			return nil, nil, err
		}
		return r, makeDecimalFromMandE(!invert, e, m, tmp2), nil
	case buf[0] > decimalNegLarge && buf[0] <= decimalNegMedium:
		// Negative medium.
		e, m, r, tmp2, err := decodeMediumNumber(true, buf, tmp)
		if err != nil {
			return nil, nil, err
		}
		return r, makeDecimalFromMandE(!invert, e, m, tmp2), nil
	case buf[0] == decimalNegSmall:
		// Negative small.
		e, m, r, tmp2, err := decodeSmallNumber(true, buf, tmp)
		if err != nil {
			return nil, nil, err
		}
		return r, makeDecimalFromMandE(!invert, e, m, tmp2), nil
	case buf[0] == decimalPosLarge:
		// Positive large.
		e, m, r, tmp2, err := decodeLargeNumber(false, buf, tmp)
		if err != nil {
			return nil, nil, err
		}
		return r, makeDecimalFromMandE(invert, e, m, tmp2), nil
	case buf[0] >= decimalPosMedium && buf[0] < decimalPosLarge:
		// Positive medium.
		e, m, r, tmp2, err := decodeMediumNumber(false, buf, tmp)
		if err != nil {
			return nil, nil, err
		}
		return r, makeDecimalFromMandE(invert, e, m, tmp2), nil
	case buf[0] == decimalPosSmall:
		// Positive small.
		e, m, r, tmp2, err := decodeSmallNumber(false, buf, tmp)
		if err != nil {
			return nil, nil, err
		}
		return r, makeDecimalFromMandE(invert, e, m, tmp2), nil
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
	if b[len(b)-1] == '0' {
		b = b[:len(b)-1]
	}

	exp := 2*e - len(b)
	if negative {
		exp++
	}
	dec := new(inf.Dec).SetScale(inf.Scale(-exp))

	// We unsafely convert the []byte to a string to avoid the usual allocation
	// when converting to a string.
	_, ok := dec.UnscaledBig().SetString(*(*string)(unsafe.Pointer(&b)), 10)
	if !ok {
		panic("could not set big.Int's string value")
	}

	return dec
}

func decodeSmallNumber(negative bool, buf []byte, tmp []byte) (e int, m []byte, rest []byte, newTmp []byte, err error) {
	var ex uint64
	var r []byte
	if negative {
		r, ex, err = DecodeUvarintAscending(buf[1:])
	} else {
		r, ex, err = DecodeUvarintDescending(buf[1:])
	}
	if err != nil {
		return 0, nil, nil, nil, err
	}

	// TODO(nvanbenschoten): bytes.IndexByte is inefficient for small slices. This is
	// apparently fixed in go1.7. For now, we manually search for the terminator.
	// idx := bytes.IndexByte(r, decimalTerminator)
	idx := -1
	for i, b := range r {
		if b == decimalTerminator {
			idx = i
			break
		}
	}
	if idx == -1 {
		return 0, nil, nil, nil, util.Errorf("did not find terminator %#x in buffer %#x", decimalTerminator, r)
	}

	m = r[:idx]
	if negative {
		var mCpy []byte
		if k := len(m); k <= len(tmp) {
			mCpy = tmp[:k]
			tmp = tmp[k:]
		} else {
			mCpy = make([]byte, k)
		}
		copy(mCpy, m)
		onesComplement(mCpy)
		m = mCpy
	}
	return int(-ex), m, r[idx+1:], tmp, nil
}

func decodeMediumNumber(negative bool, buf []byte, tmp []byte) (e int, m []byte, rest []byte, newTmp []byte, err error) {
	// TODO(nvanbenschoten): bytes.IndexByte is inefficient for small slices. This is
	// apparently fixed in go1.7. For now, we manually search for the terminator.
	// idx := bytes.IndexByte(buf[1:], decimalTerminator)
	idx := -1
	for i, b := range buf[1:] {
		if b == decimalTerminator {
			idx = i
			break
		}
	}
	if idx == -1 {
		return 0, nil, nil, nil, util.Errorf("did not find terminator %#x in buffer %#x", decimalTerminator, buf[1:])
	}

	m = buf[1 : idx+1]
	if negative {
		e = decimalNegMedium - int(buf[0])
		var mCpy []byte
		if k := len(m); k <= len(tmp) {
			mCpy = tmp[:k]
			tmp = tmp[k:]
		} else {
			mCpy = make([]byte, k)
		}
		copy(mCpy, m)
		onesComplement(mCpy)
		m = mCpy
	} else {
		e = int(buf[0]) - decimalPosMedium
	}
	return e, m, buf[idx+2:], tmp, nil
}

func decodeLargeNumber(negative bool, buf []byte, tmp []byte) (e int, m []byte, rest []byte, newTmp []byte, err error) {
	var ex uint64
	var r []byte
	if negative {
		r, ex, err = DecodeUvarintDescending(buf[1:])
	} else {
		r, ex, err = DecodeUvarintAscending(buf[1:])
	}
	if err != nil {
		return 0, nil, nil, nil, err
	}

	// TODO(nvanbenschoten): bytes.IndexByte is inefficient for small slices. This is
	// apparently fixed in go1.7. For now, we manually search for the terminator.
	// idx := bytes.IndexByte(r, decimalTerminator)
	idx := -1
	for i, b := range r {
		if b == decimalTerminator {
			idx = i
			break
		}
	}
	if idx == -1 {
		return 0, nil, nil, nil, util.Errorf("did not find terminator %#x in buffer %#x", decimalTerminator, r)
	}

	m = r[:idx]
	if negative {
		var mCpy []byte
		if k := len(m); k <= len(tmp) {
			mCpy = tmp[:k]
			tmp = tmp[k:]
		} else {
			mCpy = make([]byte, k)
		}
		copy(mCpy, m)
		onesComplement(mCpy)
		m = mCpy
	}
	return int(ex), m, r[idx+1:], tmp, nil
}

var log2Of100 = math.Log2(100)

// OverestimateDecimalSize overestimates the size of the encoded decimal.
func OverestimateDecimalSize(d *inf.Dec) int {
	// Makeup of upper bound size:
	// - 1 byte for the prefix
	// - maxVarintSize for the exponent
	// - ceil(bitLen / log2(100)) for the mantissa
	// - 1 byte for the suffix
	return 2 + maxVarintSize + int(float64(d.UnscaledBig().BitLen())/log2Of100+1)
}
