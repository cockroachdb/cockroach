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
	"fmt"
	"math/big"
	"strconv"
	"unsafe"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/util"
)

var (
	bigInt10   = big.NewInt(10)
	bigInt100  = big.NewInt(100)
	bigInt1000 = big.NewInt(1000)
)

// EncodeDecimalAscending returns the resulting byte slice with the encoded decimal
// appended to b.
//
// Values are classified as large, medium, or small according to the value of
// E. If E is 11 or more, the value is large. For E between 0 and 10, the value
// is medium. For E less than zero, the value is small.
//
// Large positive values are encoded as a single byte 0x34 followed by E as a
// varint and then M. Medium positive values are a single byte of 0x29+E
// followed by M. Small positive values are encoded as a single byte 0x28
// followed by a descending varint encoding for -E followed by M.
//
// Small negative values are encoded as a single byte 0x26 followed by -E as a
// varint and then the ones-complement of M. Medium negative values are encoded
// as a byte 0x25-E followed by the ones-complement of M. Large negative values
// consist of the single byte 0x1a followed by a descending  varint encoding of
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
	s := *(*string)(unsafe.Pointer(&b))
	_, ok := dec.UnscaledBig().SetString(s, 10)
	if !ok {
		panic(fmt.Sprintf("could not set big.Int's string value: %q", s))
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
		e = int(decimalNegMedium - buf[0])
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
		e = int(buf[0] - decimalPosMedium)
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

// EncodeNonsortingDecimal returns the resulting byte slice with the
// encoded decimal appended to b. The encoding is limited compared to
// standard encodings in this package in that
// - It will not sort lexicographically
// - It does not encode its length or terminate itself, so decoding
//   functions must be provided the exact encoded bytes
//
// The encoding assumes that any number can be written as ±0.xyz... * 10^exp,
// where xyz is a digit string, x != 0, and the last decimal in xyz is also
// not 0.
//
// The encoding uses its first byte to split decimals into 7 distinct
// ordered groups (no NaN or Infinity support yet). The groups can
// be seen in encoding.go's const definition. Following this, the
// absolute value of the exponent of the decimal (as defined above)
// is encoded as an unsigned varint. Second, the absolute value of
// the digit string is added as a big-endian byte slice.
//
// All together, the encoding looks like:
//   <marker><uvarint exponent><big-endian encoded big.Int>.
//
// The markers are shared with the sorting decimal encoding as follows:
//  decimalNaN              -> decimalNaN
//  decimalNegativeInfinity -> decimalNegativeInfinity
//  decimalNegLarge         -> decimalNegValPosExp
//  decimalNegMedium        -> decimalNegValZeroExp
//  decimalNegSmall         -> decimalNegValNegExp
//  decimalZero             -> decimalZero
//  decimalPosSmall         -> decimalPosValNegExp
//  decimalPosMedium        -> decimalPosValZeroExp
//  decimalPosLarge         -> decimalPosValPosExp
//  decimalInfinity         -> decimalInfinity
//  decimalNaNDesc          -> decimalNaNDesc
//
func EncodeNonsortingDecimal(b []byte, d *inf.Dec) []byte {
	tmp := b[len(b):]
	neg := false
	bi := d.UnscaledBig()
	switch bi.Sign() {
	case -1:
		neg = true

		// Make a deep copy of the decimal's big.Int by calling Neg.
		// We shouldn't be modifying the provided argument's
		// internal big.Int, so this works like a copy-on-write scheme.
		bi = new(big.Int)
		bi = bi.Neg(d.UnscaledBig())
	case 0:
		return append(b, decimalZero)
	}

	// Determine the exponent of the decimal, with the
	// exponent defined as .xyz * 10^exp.
	nDigits, formatted := numDigits(bi, tmp)
	e := nDigits - int(d.Scale())

	// Handle big.Int having zeros at the end of its
	// string by dividing them off (ie. 12300 -> 123).
	bi = normalizeBigInt(bi, bi == d.UnscaledBig(), formatted, tmp)
	bNat := bi.Bits()

	var buf []byte
	if n := UpperBoundNonsortingDecimalSize(d); n <= cap(b)-len(b) {
		// We append the marker directly to the input buffer b below, so
		// we are off by 1 for each of these, which explains the adjustments.
		buf = b[len(b)+1 : len(b)+1]
	} else {
		buf = make([]byte, 0, n-1)
	}

	switch {
	case neg && e > 0:
		b = append(b, decimalNegLarge)
		buf = encodeNonsortingDecimalValue(uint64(e), bNat, buf)
		return append(b, buf...)
	case neg && e == 0:
		b = append(b, decimalNegMedium)
		buf = encodeNonsortingDecimalValueWithoutExp(bNat, buf)
		return append(b, buf...)
	case neg && e < 0:
		b = append(b, decimalNegSmall)
		buf = encodeNonsortingDecimalValue(uint64(-e), bNat, buf)
		return append(b, buf...)
	case !neg && e < 0:
		b = append(b, decimalPosSmall)
		buf = encodeNonsortingDecimalValue(uint64(-e), bNat, buf)
		return append(b, buf...)
	case !neg && e == 0:
		b = append(b, decimalPosMedium)
		buf = encodeNonsortingDecimalValueWithoutExp(bNat, buf)
		return append(b, buf...)
	case !neg && e > 0:
		b = append(b, decimalPosLarge)
		buf = encodeNonsortingDecimalValue(uint64(e), bNat, buf)
		return append(b, buf...)
	}
	panic("unreachable")
}

// encodeNonsortingDecimalValue encodes the absolute value of a decimal's
// exponent and slice of digit bytes into buf, returning the populated buffer
// after encoding. The function first encodes the the absolute value of a
// decimal's exponent as an unsigned varint. Following this, it copies the
// decimal's big-endian digits themselves into the buffer.
func encodeNonsortingDecimalValue(exp uint64, digits []big.Word, buf []byte) []byte {
	// Encode the exponent using a Uvarint.
	buf = EncodeUvarintAscending(buf, exp)

	// Encode the digits into the end of the byte slice.
	return copyWords(buf, digits)
}

func encodeNonsortingDecimalValueWithoutExp(digits []big.Word, buf []byte) []byte {
	// Encode the digits into the end of the byte slice.
	return copyWords(buf, digits)
}

// normalizeBigInt divides off all trailing zeros from the provided big.Int.
// It will only modify the provided big.Int if copyOnWrite is not set, and
// it will use the formatted representation of the big.Int if it is provided.
func normalizeBigInt(bi *big.Int, copyOnWrite bool, formatted, tmp []byte) *big.Int {
	tens := 0
	if formatted != nil {
		tens = trailingZerosFromBytes(formatted)
	} else {
		tens = trailingZeros(bi, tmp)
	}
	if tens > 0 {
		// If the decimal's big.Int hasn't been copied already, copy
		// it now because we will be modifying it.
		from := bi
		if copyOnWrite {
			bi = new(big.Int)
		}

		var div *big.Int
		switch tens {
		case 1:
			div = bigInt10
		case 2:
			div = bigInt100
		case 3:
			div = bigInt1000
		default:
			div = big.NewInt(10)
			pow := big.NewInt(int64(tens))
			div.Exp(div, pow, nil)
		}
		bi.Div(from, div)
	}
	return bi
}

// DecodeNonsortingDecimal returns the decoded decimal from buf encoded with
// EncodeNonsortingDecimal. buf is assumed to contain only the encoded decimal,
// as the function does not know from the encoding itself what the length
// of the encoded value is.
func DecodeNonsortingDecimal(buf []byte, tmp []byte) (*inf.Dec, error) {
	switch {
	// TODO(nvanbenschoten) These cases are left unimplemented until we add support for
	// Infinity and NaN Decimal values.
	// case buf[0] == decimalNaN:
	// case buf[0] == decimalNegativeInfinity:
	// case buf[0] == decimalInfinity:
	// case buf[0] == decimalNaNDesc:
	case buf[0] == decimalZero:
		return inf.NewDec(0, 0), nil
	}

	dec := new(inf.Dec)
	switch {
	case buf[0] == decimalNegLarge:
		if err := decodeNonsortingDecimalValue(dec, false, buf[1:], tmp); err != nil {
			return nil, err
		}
		dec.UnscaledBig().Neg(dec.UnscaledBig())
		return dec, nil
	case buf[0] == decimalNegMedium:
		decodeNonsortingDecimalValueWithoutExp(dec, buf[1:], tmp)
		dec.UnscaledBig().Neg(dec.UnscaledBig())
		return dec, nil
	case buf[0] == decimalNegSmall:
		if err := decodeNonsortingDecimalValue(dec, true, buf[1:], tmp); err != nil {
			return nil, err
		}
		dec.UnscaledBig().Neg(dec.UnscaledBig())
		return dec, nil
	case buf[0] == decimalPosSmall:
		if err := decodeNonsortingDecimalValue(dec, true, buf[1:], tmp); err != nil {
			return nil, err
		}
		return dec, nil
	case buf[0] == decimalPosMedium:
		decodeNonsortingDecimalValueWithoutExp(dec, buf[1:], tmp)
		return dec, nil
	case buf[0] == decimalPosLarge:
		if err := decodeNonsortingDecimalValue(dec, false, buf[1:], tmp); err != nil {
			return nil, err
		}
		return dec, nil
	default:
		return nil, util.Errorf("unknown prefix of the encoded byte slice: %q", buf)
	}
}

func decodeNonsortingDecimalValue(dec *inf.Dec, negExp bool, buf, tmp []byte) error {
	// Decode the exponent.
	buf, e, err := DecodeUvarintAscending(buf)
	if err != nil {
		return err
	}
	if negExp {
		e = -e
	}

	bi := dec.UnscaledBig()
	bi.SetBytes(buf)

	// Set the decimal's scale.
	nDigits, _ := numDigits(bi, tmp)
	exp := int(e) - nDigits
	dec.SetScale(inf.Scale(-exp))
	return nil
}

func decodeNonsortingDecimalValueWithoutExp(dec *inf.Dec, buf, tmp []byte) {
	bi := dec.UnscaledBig()
	bi.SetBytes(buf)

	// Set the decimal's scale.
	nDigits, _ := numDigits(bi, tmp)
	dec.SetScale(inf.Scale(nDigits))
}

// UpperBoundNonsortingDecimalSize returns the upper bound number of bytes
// that the decimal will need for the non-sorting encoding.
func UpperBoundNonsortingDecimalSize(d *inf.Dec) int {
	// Makeup of upper bound size:
	// - 1 byte for the prefix
	// - maxVarintSize for the exponent
	// - wordLen for the big.Int bytes
	return 1 + maxVarintSize + wordLen(d.UnscaledBig().Bits())
}

// Taken from math/big/arith.go.
const bigWordSize = int(unsafe.Sizeof(big.Word(0)))

func wordLen(nat []big.Word) int {
	return len(nat) * bigWordSize
}

// copyWords was adapted from math/big/nat.go. It writes the value of
// nat into buf using big-endian encoding. len(buf) must be >= len(nat)*bigWordSize.
func copyWords(buf []byte, nat []big.Word) []byte {
	// Omit leading zeros from the resulting byte slice, which is both
	// safe and exactly what big.Int.Bytes() does. See big.nat.setBytes()
	// and big.nat.norm() for how this is normalized on decoding.
	leading := true
	for w := len(nat) - 1; w >= 0; w-- {
		d := nat[w]
		for j := bigWordSize - 1; j >= 0; j-- {
			by := byte(d >> (8 * big.Word(j)))
			if by == 0 && leading {
				continue
			}
			leading = false
			buf = append(buf, by)
		}
	}
	return buf
}

// digitsLookupTable is used to map binary digit counts to their corresponding
// decimal border values. The map relies on the proof that (without leading zeros)
// for any given number of binary digits r, such that the number represented is
// between 2^r and 2^(r+1)-1, there are only two possible decimal digit counts
// k and k+1 that the binary r digits could be representing.
//
// Using this proof, for a given digit count, the map will return the lower number
// of decimal digits (k) the binary digit count could represenent, along with the
// value of the border between the two decimal digit counts (10^k).
const tableSize = 128

var digitsLookupTable [tableSize + 1]tableVal

type tableVal struct {
	digits int
	border big.Int
}

func init() {
	curVal := big.NewInt(1)
	curExp := new(big.Int)
	for i := 1; i <= tableSize; i++ {
		if i > 1 {
			curVal.Lsh(curVal, 1)
		}

		elem := &digitsLookupTable[i]
		elem.digits = len(curVal.String())

		elem.border.SetInt64(10)
		curExp.SetInt64(int64(elem.digits))
		elem.border.Exp(&elem.border, curExp, nil)
	}
}

func lookupBits(bitLen int) (tableVal, bool) {
	if bitLen > 0 && bitLen < len(digitsLookupTable) {
		return digitsLookupTable[bitLen], true
	}
	return tableVal{}, false
}

// numDigits returns the number of decimal digits that make up
// big.Int value. The function first attempts to look this digit
// count up in the digitsLookupTable. If the value is not there,
// it defaults to constructing a string value for the big.Int and
// using this to determine the number of digits. If a string value
// is constructed, it will be returned so it can be used again.
func numDigits(bi *big.Int, tmp []byte) (int, []byte) {
	if val, ok := lookupBits(bi.BitLen()); ok {
		if bi.Cmp(&val.border) < 0 {
			return val.digits, nil
		}
		return val.digits + 1, nil
	}
	bs := bi.Append(tmp, 10)
	return len(bs), bs
}

// trailingZeros counts the number of trailing zeros in the
// big.Int value. It first attempts to use an unsigned integer
// representation of the big.Int to compute this because it is
// roughly 8x faster. If this unsigned integer would overflow,
// it falls back to formatting the big.Int itself.
func trailingZeros(bi *big.Int, tmp []byte) int {
	if bi.BitLen() <= 64 {
		i := bi.Uint64()
		bs := strconv.AppendUint(tmp, i, 10)
		return trailingZerosFromBytes(bs)
	}
	bs := bi.Append(tmp, 10)
	return trailingZerosFromBytes(bs)
}

func trailingZerosFromBytes(bs []byte) int {
	tens := 0
	for bs[len(bs)-1-tens] == '0' {
		tens++
	}
	return tens
}
