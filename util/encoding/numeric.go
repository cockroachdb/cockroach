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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// An ordered key encoding scheme for numbers (both integers and floats) based
// on sqlite4's key encoding:
// http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
//
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Peter Mattis (petermattis@gmail.com)

package encoding

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
)

// Direct mappings or prefixes of encoded data dependent on the type.
const (
	orderedEncodingNaN              = 0x06
	orderedEncodingNegativeInfinity = 0x07
	orderedEncodingZero             = 0x15
	orderedEncodingInfinity         = 0x23
	orderedEncodingTerminator       = 0x00
)

// EncodeNumericInt returns the resulting byte slice with the encoded int64
// appended to b. See the notes for EncodeNumericFloat for a complete
// description.
func EncodeNumericInt(b []byte, i int64) []byte {
	if i == 0 {
		return append(b, orderedEncodingZero)
	}
	e, m := intMandE(i)
	if e <= 0 {
		panic("integer values should not have negative exponents")
	}
	buf := make([]byte, len(m)+maxVarintSize+2)
	switch {
	case e > 0 && e <= 10:
		return append(b, encodeMediumNumber(i < 0, e, m, buf)...)
	case e >= 11:
		return append(b, encodeLargeNumber(i < 0, e, m, buf)...)
	}
	panic(fmt.Sprintf("unexpected value e: %d", e))
}

// EncodeNumericIntDecreasing returns the resulting byte slice with
// the encoded int64 value in decreasing order appended to b.
func EncodeNumericIntDecreasing(b []byte, i int64) []byte {
	return EncodeNumericInt(b, ^i)
}

// DecodeNumericInt returns the remaining byte slice after decoding
// and the decoded int64 from buf.
func DecodeNumericInt(buf []byte) ([]byte, int64) {
	if buf[0] == 0x15 {
		return buf[1:], 0
	}
	if buf[0] == 0x14 || buf[0] == 0x16 {
		// Negative small or positive small.
		panic("integer values should not have negative exponents")
	}
	idx := bytes.Index(buf, []byte{orderedEncodingTerminator})
	switch {
	case buf[0] == 0x08:
		// Negative large.
		e, m := decodeLargeNumber(true, buf[:idx+1])
		return buf[idx+1:], makeIntFromMandE(true, e, m)
	case buf[0] > 0x08 && buf[0] <= 0x13:
		// Negative medium.
		e, m := decodeMediumNumber(true, buf[:idx+1])
		return buf[idx+1:], makeIntFromMandE(true, e, m)
	case buf[0] == 0x22:
		// Positive large.
		e, m := decodeLargeNumber(false, buf[:idx+1])
		return buf[idx+1:], makeIntFromMandE(false, e, m)
	case buf[0] >= 0x17 && buf[0] < 0x22:
		// Positive large.
		e, m := decodeMediumNumber(false, buf[:idx+1])
		return buf[idx+1:], makeIntFromMandE(false, e, m)
	default:
		panic(fmt.Sprintf("unknown prefix of the encoded byte slice: %q", buf))
	}
}

// DecodeNumericIntDecreasing returns the remaining byte slice after decoding and
// the decoded int64 in decreasing order from buf.
func DecodeNumericIntDecreasing(buf []byte) ([]byte, int64) {
	b, v := DecodeNumericInt(buf)
	return b, ^v
}

// intMandE computes and returns the mantissa M and exponent E for i.
//
// See the comments in floatMandE for more details of the representation
// of M. This method computes E and M in a simpler loop.
func intMandE(i int64) (int, []byte) {
	var e int
	var m [12]byte
	for v := i; v != 0; v /= 100 {
		e++
		mod := v % 100
		if mod < 0 {
			m[12-e] = byte(2*-mod + 1)
		} else {
			m[12-e] = byte(2*mod + 1)
		}
	}

	// Remove trailing zeroes which were encoded above as 1.
	n := 11
	for ; n > 0; n-- {
		if m[n] != 1 {
			break
		}
	}

	// The last byte is encoded as 2n+0.
	m[n]--
	return e, m[12-e : n+1]
}

// makeIntFromMandE reconstructs the integer from the mantissa M
// and exponent E.
func makeIntFromMandE(negative bool, e int, m []byte) int64 {
	var i int64
	for v := 0; v < e; v++ {
		var t int64
		if v < len(m) {
			t = int64(m[v])
		} else {
			// Trailing X==0 digits were omitted.
			t = 0
		}
		// The last byte was encoded as 2n+0.
		if v != len(m)-1 {
			t--
		}
		if negative {
			t = -t
		}
		i = i*100 + t/2
	}
	return i
}

// EncodeNumericFloat returns the resulting byte slice with the encoded float64
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
//
// The resulting numeric encodings are all comparable. That is, the result from
// EncodeNumericInt is comparable with the result from EncodeNumericFloat. But
// this flexibility comes at the cost of speed. Prefer the EncodeUvarint{32,64}
// routines for speed.
func EncodeNumericFloat(b []byte, f float64) []byte {
	// Handle the simplistic cases first.
	switch {
	case math.IsNaN(f):
		return append(b, orderedEncodingNaN)
	case math.IsInf(f, 1):
		return append(b, orderedEncodingInfinity)
	case math.IsInf(f, -1):
		return append(b, orderedEncodingNegativeInfinity)
	case f == 0:
		return append(b, orderedEncodingZero)
	}
	e, m := floatMandE(f)
	buf := make([]byte, len(m)+maxVarintSize+2)
	switch {
	case e < 0:
		return append(b, encodeSmallNumber(f < 0, e, m, buf)...)
	case e >= 0 && e <= 10:
		return append(b, encodeMediumNumber(f < 0, e, m, buf)...)
	case e >= 11:
		return append(b, encodeLargeNumber(f < 0, e, m, buf)...)
	}
	return nil
}

// DecodeNumericFloat returns the remaining byte slice after decoding and the decoded
// float64 from buf.
func DecodeNumericFloat(buf []byte) ([]byte, float64) {
	if buf[0] == 0x15 {
		return buf[1:], 0
	}
	idx := bytes.Index(buf, []byte{orderedEncodingTerminator})
	switch {
	case buf[0] == orderedEncodingNaN:
		return buf[1:], math.NaN()
	case buf[0] == orderedEncodingInfinity:
		return buf[1:], math.Inf(1)
	case buf[0] == orderedEncodingNegativeInfinity:
		return buf[1:], math.Inf(-1)
	case buf[0] == 0x08:
		// Negative large.
		e, m := decodeLargeNumber(true, buf[:idx+1])
		return buf[idx+1:], makeFloatFromMandE(true, e, m)
	case buf[0] > 0x08 && buf[0] <= 0x13:
		// Negative medium.
		e, m := decodeMediumNumber(true, buf[:idx+1])
		return buf[idx+1:], makeFloatFromMandE(true, e, m)
	case buf[0] == 0x14:
		// Negative small.
		e, m := decodeSmallNumber(true, buf[:idx+1])
		return buf[idx+1:], makeFloatFromMandE(true, e, m)
	case buf[0] == 0x22:
		// Positive large.
		e, m := decodeLargeNumber(false, buf[:idx+1])
		return buf[idx+1:], makeFloatFromMandE(false, e, m)
	case buf[0] >= 0x17 && buf[0] < 0x22:
		// Positive large.
		e, m := decodeMediumNumber(false, buf[:idx+1])
		return buf[idx+1:], makeFloatFromMandE(false, e, m)
	case buf[0] == 0x16:
		// Positive small.
		e, m := decodeSmallNumber(false, buf[:idx+1])
		return buf[idx+1:], makeFloatFromMandE(false, e, m)
	default:
		panic(fmt.Sprintf("unknown prefix of the encoded byte slice: %q", buf))
	}
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
func floatMandE(f float64) (int, []byte) {
	if f < 0 {
		f = -f
	}

	// Use strconv.FormatFloat to handle the intricacies of determining how much
	// precision is necessary to precisely represent f. The 'e' format is
	// d.dddde±dd.
	b := strconv.AppendFloat(nil, f, 'e', -1, 64)
	if len(b) < 4 {
		// The formatted float must be at least 4 bytes ("1e+0") or something
		// unexpected has occurred.
		panic(fmt.Errorf("malformed float: %v -> %s", f, b))
	}

	// Parse the exponent.
	e := bytes.IndexByte(b, 'e')
	e10 := 0
	for i := e + 2; i < len(b); i++ {
		e10 = e10*10 + int(b[i]-'0')
	}
	switch b[e+1] {
	case '-':
		e10 = -e10
	case '+':
	default:
		panic(fmt.Errorf("malformed float: %v -> %s", f, b))
	}

	// Strip off the exponent.
	b = b[:e]

	// Move all of the digits after the decimal and prepend a leading 0.
	if len(b) > 1 {
		// "d.dddd" -> "dddddd"
		b[1] = b[0]
	} else {
		// "d" -> "dd"
		b = append(b, b[0])
	}
	b[0] = '0' // "0ddddd"
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

// makeFloatFromMandE reconstructs the float from the mantissa M and exponent
// E. Properly handling floating point rounding is tough, so we take the
// approach of converting the base-100 mantissa into a base-10 mantissa,
// formatting the floating point number to a string and then using the standard
// library to parse it.
func makeFloatFromMandE(negative bool, e int, m []byte) float64 {
	// ±.dddde±dd.
	b := make([]byte, 0, len(m)*2+6)
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
		b = append(b, byte(t/10)+'0')
		b = append(b, byte(t%10)+'0')
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
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		panic(err)
	}
	return f
}

func encodeSmallNumber(negative bool, e int, m []byte, buf []byte) []byte {
	n := putUvarint(buf[1:], uint64(-e))
	copy(buf[n+1:], m)
	l := 1 + n + len(m)
	if negative {
		buf[0] = 0x14
		onesComplement(buf[1+n : l]) // ones complement of mantissa
	} else {
		buf[0] = 0x16
		onesComplement(buf[1 : 1+n]) // ones complement of exponent
	}
	buf[l] = orderedEncodingTerminator
	return buf[:l+1]
}

func encodeMediumNumber(negative bool, e int, m []byte, buf []byte) []byte {
	copy(buf[1:], m)
	l := 1 + len(m)
	if negative {
		buf[0] = 0x13 - byte(e)
		onesComplement(buf[1:l])
	} else {
		buf[0] = 0x17 + byte(e)
	}
	buf[l] = orderedEncodingTerminator
	return buf[:l+1]
}

func encodeLargeNumber(negative bool, e int, m []byte, buf []byte) []byte {
	n := putUvarint(buf[1:], uint64(e))
	copy(buf[n+1:], m)
	l := 1 + n + len(m)
	if negative {
		buf[0] = 0x08
		onesComplement(buf[1:l])
	} else {
		buf[0] = 0x22
	}
	buf[l] = orderedEncodingTerminator
	return buf[:l+1]
}

func decodeSmallNumber(negative bool, buf []byte) (int, []byte) {
	var e uint64
	var n int
	if negative {
		e, n = getUvarint(buf[1:])
	} else {
		tmp := []byte{^buf[1]}
		e, n = getUvarint(tmp)
	}

	// We don't need the prefix and last terminator.
	m := make([]byte, len(buf)-(2+n))
	copy(m, buf[1+n:len(buf)-1])

	if negative {
		onesComplement(m)
	}
	return int(-e), m
}

func decodeMediumNumber(negative bool, buf []byte) (int, []byte) {
	// We don't need the prefix and last terminator.
	m := make([]byte, len(buf)-2)
	copy(m, buf[1:len(buf)-1])

	var e int
	if negative {
		e = 0x13 - int(buf[0])
		onesComplement(m)
	} else {
		e = int(buf[0]) - 0x17
	}
	return e, m
}

func decodeLargeNumber(negative bool, buf []byte) (int, []byte) {
	m := make([]byte, len(buf))
	copy(m, buf)
	if negative {
		onesComplement(m[1:])
	}
	e, l := getUvarint(m[1:])

	// We don't need the prefix and last terminator.
	return int(e), m[l+1 : len(m)-1]
}
