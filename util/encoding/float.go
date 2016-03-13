// Copyright 2014 The Cockroach Authors.
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
// An ordered key encoding scheme for numbers (both integers and floats) based
// on sqlite4's key encoding:
// http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
//
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Peter Mattis (peter@cockroachlabs.com)

package encoding

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"unsafe"

	"github.com/cockroachdb/cockroach/util"
)

// EncodeFloatAscending returns the resulting byte slice with the encoded float64
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
func EncodeFloatAscending(b []byte, f float64) []byte {
	// Handle the simplistic cases first.
	switch {
	case math.IsNaN(f):
		return append(b, floatNaN)
	case math.IsInf(f, 1):
		return append(b, floatInfinity)
	case math.IsInf(f, -1):
		return append(b, floatNegativeInfinity)
	case f == 0:
		return append(b, floatZero)
	}
	// TODO(nvanbenschoten) Switch to a base-256 mantissa.
	e, m := floatMandE(f, b[len(b):])
	return encodeMandE(b, f < 0, e, m)
}

// EncodeFloatDescending is the descending version of EncodeFloatAscending.
func EncodeFloatDescending(b []byte, f float64) []byte {
	if math.IsNaN(f) {
		return append(b, floatNaNDesc)
	}
	return EncodeFloatAscending(b, -f)
}

// DecodeFloatAscending returns the remaining byte slice after decoding and the decoded
// float64 from buf.
func DecodeFloatAscending(buf []byte, tmp []byte) ([]byte, float64, error) {
	// Handle the simplistic cases first.
	switch buf[0] {
	case floatNaN, floatNaNDesc:
		return buf[1:], math.NaN(), nil
	case floatInfinity:
		return buf[1:], math.Inf(1), nil
	case floatNegativeInfinity:
		return buf[1:], math.Inf(-1), nil
	case floatZero:
		return buf[1:], 0, nil
	}
	tmp = tmp[len(tmp):cap(tmp)]
	idx := bytes.IndexByte(buf, floatTerminator)
	if idx == -1 {
		return nil, 0, util.Errorf("did not find terminator %#x in buffer %#x", floatTerminator, buf)
	}
	switch {
	case buf[0] == floatNegLarge:
		// Negative large.
		e, m, tmp2 := decodeLargeNumber(true, buf[:idx+1], tmp)
		return buf[idx+1:], makeFloatFromMandE(true, e, m, tmp2), nil
	case buf[0] > floatNegLarge && buf[0] <= floatNegMedium:
		// Negative medium.
		e, m, tmp2 := decodeMediumNumber(true, buf[:idx+1], tmp)
		return buf[idx+1:], makeFloatFromMandE(true, e, m, tmp2), nil
	case buf[0] == floatNegSmall:
		// Negative small.
		e, m, tmp2 := decodeSmallNumber(true, buf[:idx+1], tmp)
		return buf[idx+1:], makeFloatFromMandE(true, e, m, tmp2), nil
	case buf[0] == floatPosLarge:
		// Positive large.
		e, m, tmp2 := decodeLargeNumber(false, buf[:idx+1], tmp)
		return buf[idx+1:], makeFloatFromMandE(false, e, m, tmp2), nil
	case buf[0] >= floatPosMedium && buf[0] < floatPosLarge:
		// Positive medium.
		e, m, tmp2 := decodeMediumNumber(false, buf[:idx+1], tmp)
		return buf[idx+1:], makeFloatFromMandE(false, e, m, tmp2), nil
	case buf[0] == floatPosSmall:
		// Positive small.
		e, m, tmp2 := decodeSmallNumber(false, buf[:idx+1], tmp)
		return buf[idx+1:], makeFloatFromMandE(false, e, m, tmp2), nil
	default:
		return nil, 0, util.Errorf("unknown prefix of the encoded byte slice: %q", buf)
	}
}

// DecodeFloatDescending decodes floats encoded with EncodeFloatDescending.
func DecodeFloatDescending(buf []byte, tmp []byte) ([]byte, float64, error) {
	b, r, err := DecodeFloatAscending(buf, tmp)
	return b, -r, err
}

// floatMandE computes and returns the mantissa M and exponent E for f.
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
func floatMandE(f float64, tmp []byte) (int, []byte) {
	if f < 0 {
		f = -f
	}

	// Use strconv.FormatFloat to handle the intricacies of determining how much
	// precision is necessary to precisely represent f. The 'e' format is
	// d.dddde±dd.
	tmp = strconv.AppendFloat(tmp, f, 'e', -1, 64)
	if len(tmp) < 4 {
		// The formatted float must be at least 4 bytes ("1e+0") or something
		// unexpected has occurred.
		panic(fmt.Sprintf("malformed float: %v -> %s", f, tmp))
	}

	// Parse the exponent.
	e := bytes.IndexByte(tmp, 'e')
	e10 := 0
	for i := e + 2; i < len(tmp); i++ {
		e10 = e10*10 + int(tmp[i]-'0')
	}
	switch tmp[e+1] {
	case '-':
		e10 = -e10
	case '+':
	default:
		panic(fmt.Sprintf("malformed float: %v -> %s", f, tmp))
	}

	// Strip off the exponent.
	tmp = tmp[:e]

	// Move all of the digits after the decimal and prepend a leading 0.
	if len(tmp) > 1 {
		// "d.dddd" -> "dddddd"
		tmp[1] = tmp[0]
	} else {
		// "d" -> "dd"
		tmp = append(tmp, tmp[0])
	}
	tmp[0] = '0' // "0ddddd"
	e10++

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

// makeFloatFromMandE reconstructs the float from the mantissa M and exponent
// E. Properly handling floating point rounding is tough, so we take the
// approach of converting the base-100 mantissa into a base-10 mantissa,
// formatting the floating point number to a string and then using the standard
// library to parse it.
func makeFloatFromMandE(negative bool, e int, m []byte, tmp []byte) float64 {
	// ±.dddde±dd.
	b := tmp[:0]
	if n := len(m)*2 + 6; cap(b) < n {
		b = make([]byte, 0, n)
	}
	if negative {
		b = append(b, '-')
	}
	b = append(b, '.')
	for i, v := range m {
		t := int(v)
		if i == len(m) {
			t--
		}
		t /= 2
		b = append(b, byte(t/10)+'0', byte(t%10)+'0')
	}
	b = append(b, 'e')
	e = 2 * e
	if e < 0 {
		b = append(b, '-')
		e = -e
	} else {
		b = append(b, '+')
	}

	var buf [3]byte
	i := len(buf)
	for e >= 10 {
		i--
		buf[i] = byte(e%10 + '0')
		e /= 10
	}
	i--
	buf[i] = byte(e + '0')

	b = append(b, buf[i:]...)

	// We unsafely convert the []byte to a string to avoid the usual allocation
	// when converting to a string.
	f, err := strconv.ParseFloat(*(*string)(unsafe.Pointer(&b)), 64)
	if err != nil {
		panic(err)
	}
	return f
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
	n := putUvarint(buf[1:], uint64(-e))
	copy(buf[n+1:], m)
	l := 1 + n + len(m)
	if negative {
		buf[0] = floatNegSmall
		onesComplement(buf[1+n : l]) // ones complement of mantissa
	} else {
		buf[0] = floatPosSmall
		onesComplement(buf[1 : 1+n]) // ones complement of exponent
	}
	buf[l] = floatTerminator
	return buf[:l+1]
}

func encodeMediumNumber(negative bool, e int, m []byte, buf []byte) []byte {
	copy(buf[1:], m)
	l := 1 + len(m)
	if negative {
		buf[0] = floatNegMedium - byte(e)
		onesComplement(buf[1:l])
	} else {
		buf[0] = floatPosMedium + byte(e)
	}
	buf[l] = floatTerminator
	return buf[:l+1]
}

func encodeLargeNumber(negative bool, e int, m []byte, buf []byte) []byte {
	n := putUvarint(buf[1:], uint64(e))
	copy(buf[n+1:], m)
	l := 1 + n + len(m)
	if negative {
		buf[0] = floatNegLarge
		onesComplement(buf[1:l])
	} else {
		buf[0] = floatPosLarge
	}
	buf[l] = floatTerminator
	return buf[:l+1]
}

func decodeSmallNumber(negative bool, buf []byte, tmp []byte) (e int, m []byte, newTmp []byte) {
	var ex uint64
	var n int
	if negative {
		ex, n = getUvarint(buf[1:])
	} else {
		var t []byte
		if len(tmp) > 0 {
			t = tmp[:1]
			t[0] = ^buf[1]
		} else {
			t = []byte{^buf[1]}
		}
		ex, n = getUvarint(t)
	}

	// We don't need the prefix and last terminator.
	if k := len(buf) - (2 + n); k <= len(tmp) {
		m = tmp[:k]
		tmp = tmp[k:]
	} else {
		m = make([]byte, len(buf)-(2+n))
	}
	copy(m, buf[1+n:len(buf)-1])

	if negative {
		onesComplement(m)
	}
	return int(-ex), m, tmp
}

func decodeMediumNumber(negative bool, buf []byte, tmp []byte) (e int, m []byte, newTmp []byte) {
	// We don't need the prefix and last terminator.
	if n := len(buf) - 2; n <= len(tmp) {
		m = tmp[:n]
		tmp = tmp[n:]
	} else {
		m = make([]byte, n)
	}
	copy(m, buf[1:len(buf)-1])

	if negative {
		e = floatNegMedium - int(buf[0])
		onesComplement(m)
	} else {
		e = int(buf[0]) - floatPosMedium
	}
	return e, m, tmp
}

func decodeLargeNumber(negative bool, buf []byte, tmp []byte) (e int, m []byte, newTmp []byte) {
	if n := len(buf); n <= len(tmp) {
		m = tmp[:n]
		tmp = tmp[n:]
	} else {
		m = make([]byte, n)
	}
	copy(m, buf)
	if negative {
		onesComplement(m[1:])
	}
	ex, l := getUvarint(m[1:])

	// We don't need the prefix and last terminator.
	return int(ex), m[l+1 : len(m)-1], tmp
}
