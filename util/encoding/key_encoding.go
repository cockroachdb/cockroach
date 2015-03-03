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
// An ordered key encoding scheme based on sqlite4's key encoding:
// http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
//
// Author: Andrew Bonventre (andybons@gmail.com)
// TODO(andybons): Add get* functions for decoding.

package encoding

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"unicode/utf8"
)

// Maximum sizes for allocating buffers to hold encoded values.
const (
	// MaxIntSize is the maximum encoded integer size in bytes.
	MaxIntSize = 12
)

// Direct mappings or prefixes of encoded data dependent on the type.
const (
	orderedEncodingNil                 = 0x05
	orderedEncodingNaN                 = 0x06
	orderedEncodingNegativeInfinity    = 0x07
	orderedEncodingZero                = 0x15
	orderedEncodingInfinity            = 0x23
	orderedEncodingText                = 0x24
	orderedEncodingBinary              = 0x25
	orderedEncodingBinaryNoTermination = 0x26
	orderedEncodingTerminator          = 0x00
)

// EncodeNil returns a byte slice containing a nil-encoded value.
func EncodeNil() []byte {
	return []byte{orderedEncodingNil}
}

// EncodeString returns the resulting byte slice with s encoded
// and appended to b. If b is nil, it is treated as an empty
// byte slice. If s is not a valid utf8-encoded string or
// contains an intervening 0x00 byte, EncodeString will panic.
//
// Each value that is TEXT begins with a single byte of 0x24
// and ends with a single byte of 0x00. There are zero or more
// intervening bytes that encode the text value. The intervening
// bytes are chosen so that the encoding will sort in the desired
// collating order. The default sequence of bytes is simply UTF8.
// The intervening bytes may not contain a 0x00 character; the only
// 0x00 byte allowed in a text encoding is the final byte.
//
// Note that all key-encoded text with the BINARY collating sequence
// is simply UTF8 text. UTF8 not UTF16. Strings must be converted to
// UTF8 so that equivalent strings in different encodings compare the
// same and so that the strings contain no embedded 0x00 bytes. In other
// words, strcmp() should be sufficient for comparing two text keys.
//
// The text encoding ends in 0x00 in order to ensure that when there
// are two strings where one is a prefix of the other that the shorter
// string will sort first.
func EncodeString(b []byte, s string) []byte {
	if !utf8.ValidString(s) {
		panic("invalid utf8 string passed")
	}
	b = append(b, orderedEncodingText)
	for _, v := range []byte(s) {
		if v == 0x00 {
			panic("string contains intervening 0x00 byte")
		}
		b = append(b, v)
	}
	return append(b, orderedEncodingTerminator)
}

// DecodeString returns the remaining byte slice after decoding and the
// decoded string from b.
func DecodeString(b []byte) ([]byte, string) {
	if b[0] != orderedEncodingText {
		panic("first byte of encoded string must be 0x24")
	}
	for i, v := range b[1:] {
		if v == orderedEncodingTerminator {
			return b[1+i:], string(b[1 : 1+i])
		}
	}
	panic("encoded string must have terminator byte")
}

// EncodeBinary returns the resulting byte slice with i encoded
// and appended to b.
//
// The encoding of binaries fields is different depending
// on whether or not the value to be encoded is the last value
// (the right-most value) in the key.
//
// Each value that is BINARY that is not the last value of the key
// begins with a single byte of 0x25 and ends with a single terminator
// byte of 0x00. There are zero or more intervening bytes that encode
// the binary value. None of the intervening bytes may be zero. Each
// of the intervening bytes contains 7 bits of blob content with a 1
// in the high-order bit (the 0x80 bit). Each encoded byte thereafter
// consists of a high-order bit followed by 7 bits of payload. An
// empty input value is encoded as the header byte immediately
// followed by a termination byte 0x00.
//
// When the very last value of a key is BINARY, then it is encoded
// as a single byte of 0x26 and is followed by a byte-for-byte
// copy of the BINARY value. This alternative encoding is more efficient,
// but it only works if there are no subsequent values in the key,
// since there is no termination mark on the BLOB being encoded.
//
// The initial byte of a binary value, 0x25 or 0x26, is larger than
// the initial byte of a text value, 0x24, ensuring that every binary
// value will sort after every text value.
func EncodeBinary(b []byte, i []byte) []byte {
	b = append(b, orderedEncodingBinary)
	s := uint(1)
	t := byte(0)
	for _, v := range i {
		b = append(b, byte(0x80|t|((v&0xff)>>s)))
		if s < 7 {
			t = v << (7 - s)
			s++
		} else {
			b = append(b, byte(0x80|v))
			s = 1
			t = 0
		}
	}
	if s > 1 {
		b = append(b, byte(0x80|t))
	}
	return append(b, orderedEncodingTerminator)
}

// DecodeBinary decodes the given key-encoded byte slice, returning
// the unencoded byte slice value. (see documentation for EncodeBinary
// for more details). The first return argument is the remainder of
// the input buffer, after decoding the binary value.
func DecodeBinary(buf []byte) ([]byte, []byte) {
	if buf[0] != orderedEncodingBinary {
		panic(fmt.Sprintf("%q doesn't begin with binary encoding byte", buf))
	}
	out := new(bytes.Buffer)
	s := uint(6)
	i := int(1)
	if buf[i] == orderedEncodingTerminator {
		return buf[2:], out.Bytes()
	}
	t := (buf[i] << 1) & 0xff
	for i = 2; buf[i] != orderedEncodingTerminator; i++ {
		if s == 7 {
			out.WriteByte(t | (buf[i] & 0x7f))
			i++
		} else {
			out.WriteByte(t | ((buf[i] & 0x7f) >> s))
		}

		t = (buf[i] << (8 - s)) & 0xff

		if buf[i] == orderedEncodingTerminator {
			break
		}

		if s == 1 {
			s = 7
		} else {
			s--
		}
	}
	if t != 0 {
		panic("unexpected bits remaining after decoding blob")
	}
	return buf[i+1:], out.Bytes()
}

// DecodeBinaryFinal decodes a byte slice and returns the
// result presuming that it is the last part of the buffer
// (see documentation for EncodeBinary for more details).
func DecodeBinaryFinal(buf []byte) []byte {
	if buf[0] != orderedEncodingBinaryNoTermination {
		panic(fmt.Sprintf("%q doesn't begin with binary-no-termination encoding byte", buf))
	}
	out := make([]byte, len(buf)-1)
	copy(buf[1:], out)
	return out
}

// EncodeBinaryFinal encodes a byte slice and returns
// the result presuming that the byte slice is the last
// portion of the buffer (see the comment for EncodeBinary).
func EncodeBinaryFinal(b []byte) []byte {
	buf := make([]byte, 1+len(b))
	buf[0] = orderedEncodingBinaryNoTermination
	for i, v := range b {
		buf[i+1] = v
	}
	return buf
}

// EncodeInt returns the resulting byte slice with the encoded int64 and
// appended to b. See the notes for EncodeFloat for a complete description.
func EncodeInt(b []byte, i int64) []byte {
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

// EncodeIntDecreasing returns the resulting byte slice with the
// encoded int64 values in decreasing order and appended to b.
func EncodeIntDecreasing(b []byte, i int64) []byte {
	return EncodeInt(b, ^i)
}

// DecodeInt returns the remaining byte slice after decoding and the decoded
// int64 from buf.
func DecodeInt(buf []byte) ([]byte, int64) {
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

// DecodeIntDecreasing returns the remaining byte slice after decoding and
// the decoded int64 in decreasing order from buf.
func DecodeIntDecreasing(buf []byte) ([]byte, int64) {
	b, v := DecodeInt(buf)
	return b, ^v
}

// intMandE computes and returns the mantissa M and exponent E for i.
//
// See the comments in floatMandE for more details of the representation
// of M. This method computes E and M in a simpler loop.
func intMandE(i int64) (int, []byte) {
	var e int
	var buf bytes.Buffer
	for v := i; v != 0; v /= 100 {
		mod := v % 100
		if mod == 0 && buf.Len() == 0 {
			// Trailing X==0 digits are omitted.
		} else if mod < 0 {
			buf.WriteByte(byte(2*-mod + 1))
		} else {
			buf.WriteByte(byte(2*mod + 1))
		}
		e++
	}
	m := buf.Bytes()
	// Reverse the mantissa so highest byte sorts first.
	for i := range m {
		j := len(m) - i - 1
		if i >= j {
			break
		}
		m[i], m[j] = m[j], m[i]
	}
	// The last byte is encoded as 2n+0.
	m[len(m)-1]--

	return e, m
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

func removeTrailingZeros(m []byte) []byte {
	for i := len(m); i > 0; i-- {
		if m[i-1] != 0 {
			return m[:i]
		}
	}
	return []byte{}
}

// EncodeFloat returns the resulting byte slice with the encoded
// float64 and appended to b.
//
// Values are classified as large, medium, or small
// according to the value of E. If E is 11 or more,
// the value is large. For E between 0 and 10, the
// value is medium. For E less than zero, the value is small.
//
// Large positive values are encoded as a single byte 0x22
// followed by E as a varint and then M. Medium positive
// values are a single byte of 0x17+E followed by M. Small
// positive values are encoded as a single byte 0x16 followed
// by the ones-complement of the varint for -E followed by M.
//
// Small negative values are encoded as a single byte 0x14
// followed by -E as a varint and then the ones-complement
// of M. Medium negative values are encoded as a byte 0x13-E
// followed by the ones-complement of M. Large negative values
// consist of the single byte 0x08 followed by the ones-complement
// of the varint encoding of E followed by the ones-complement of M.
func EncodeFloat(b []byte, f float64) []byte {
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

// DecodeFloat returns the remaining byte slice after decoding and the decoded
// float64 from buf.
func DecodeFloat(buf []byte) ([]byte, float64) {
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
// The mantissa is a base-100 representation of the value. The exponent
// E determines where to put the decimal point.
//
// Each centimal digit of the mantissa is stored in a byte. If the value
// of the centimal digit is X (hence X>=0 and X<=99) then the byte value
// will be 2*X+1 for every byte of the mantissa, except for the last byte
// which will be 2*X+0. The mantissa must be the minimum number of bytes
// necessary to represent the value; trailing X==0 digits are omitted.
// This means that the mantissa will never contain a byte with the value 0x00.
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

// onesComplement inverts each byte in buf from index start to end.
func onesComplement(buf []byte, start, end int) {
	for i := start; i < end; i++ {
		buf[i] = ^buf[i]
	}
}

func encodeSmallNumber(negative bool, e int, m []byte, buf []byte) []byte {
	n := PutUvarint(buf[1:], uint64(-e))
	copy(buf[n+1:], m)
	l := 1 + n + len(m)
	if negative {
		buf[0] = 0x14
		onesComplement(buf, 1+n, l) // ones complement of mantissa
	} else {
		buf[0] = 0x16
		onesComplement(buf, 1, 1+n) // ones complement of exponent
	}
	buf[l] = orderedEncodingTerminator
	return buf[:l+1]
}

func encodeMediumNumber(negative bool, e int, m []byte, buf []byte) []byte {
	copy(buf[1:], m)
	l := 1 + len(m)
	if negative {
		buf[0] = 0x13 - byte(e)
		onesComplement(buf, 1, l)
	} else {
		buf[0] = 0x17 + byte(e)
	}
	buf[l] = orderedEncodingTerminator
	return buf[:l+1]
}

func encodeLargeNumber(negative bool, e int, m []byte, buf []byte) []byte {
	n := PutUvarint(buf[1:], uint64(e))
	copy(buf[n+1:], m)
	l := 1 + n + len(m)
	if negative {
		buf[0] = 0x08
		onesComplement(buf, 1, l)
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
		e, n = GetUVarint(buf[1:])
	} else {
		tmp := []byte{^buf[1]}
		e, n = GetUVarint(tmp)
	}

	// We don't need the prefix and last terminator.
	m := make([]byte, len(buf)-(2+n))
	copy(m, buf[1+n:len(buf)-1])

	if negative {
		onesComplement(m, 0, len(m))
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
		onesComplement(m, 0, len(m))
	} else {
		e = int(buf[0]) - 0x17
	}
	return e, m
}

func decodeLargeNumber(negative bool, buf []byte) (int, []byte) {
	m := make([]byte, len(buf))
	copy(m, buf)
	if negative {
		onesComplement(m, 1, len(m))
	}
	e, l := GetUVarint(m[1:])

	// We don't need the prefix and last terminator.
	return int(e), m[l+1 : len(m)-1]
}
