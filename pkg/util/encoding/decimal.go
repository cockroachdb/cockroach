// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// An ordered key encoding scheme for arbitrary-precision fixed-point
// numeric values based on sqlite4's key encoding:
// http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki

package encoding

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"unsafe"

	"github.com/cockroachdb/apd"
	"github.com/pkg/errors"
)

// EncodeDecimalAscending returns the resulting byte slice with the encoded decimal
// appended to the given buffer.
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
func EncodeDecimalAscending(appendTo []byte, d *apd.Decimal) []byte {
	return encodeDecimal(appendTo, d, false)
}

// EncodeDecimalDescending is the descending version of EncodeDecimalAscending.
func EncodeDecimalDescending(appendTo []byte, d *apd.Decimal) []byte {
	return encodeDecimal(appendTo, d, true)
}

func encodeDecimal(appendTo []byte, d *apd.Decimal, invert bool) []byte {
	if d.IsZero() {
		// Negative and positive zero are encoded identically. Only nonsorting
		// decimal encoding can retain the sign.
		return append(appendTo, decimalZero)
	}
	neg := d.Negative != invert
	switch d.Form {
	case apd.Finite:
		// ignore
	case apd.Infinite:
		if neg {
			return append(appendTo, decimalNegativeInfinity)
		}
		return append(appendTo, decimalInfinity)
	case apd.NaN:
		if invert {
			return append(appendTo, decimalNaNDesc)
		}
		return append(appendTo, decimalNaN)
	default:
		panic(errors.Errorf("unknown form: %s", d.Form))
	}
	e, m := decimalEandM(d, appendTo[len(appendTo):])
	return encodeEandM(appendTo, neg, e, m)
}

// decimalEandM computes and returns the exponent E and mantissa M for d.
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
func decimalEandM(d *apd.Decimal, tmp []byte) (int, []byte) {
	addedZero := false
	if cap(tmp) > 0 {
		tmp = tmp[:1]
		tmp[0] = '0'
		addedZero = true
	}
	tmp = d.Coeff.Append(tmp, 10)
	if !addedZero {
		tmp = append(tmp, '0')
		copy(tmp[1:], tmp[:len(tmp)-1])
		tmp[0] = '0'
	}

	// The exponent will be the combination of the decimal's exponent, and the
	// number of digits in the big.Int.
	e10 := int(d.Exponent) + len(tmp[1:])

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

// encodeEandM encodes the exponent and mantissa, appending the encoding to a byte buffer.
//
// The mantissa m can be stored in the spare capacity of appendTo.
func encodeEandM(appendTo []byte, negative bool, e int, m []byte) []byte {
	var buf []byte
	if n := len(m) + maxVarintSize + 2; n <= cap(appendTo)-len(appendTo) {
		buf = appendTo[len(appendTo) : len(appendTo)+n]
	} else {
		buf = make([]byte, n)
	}
	switch {
	case e < 0:
		return append(appendTo, encodeSmallNumber(negative, e, m, buf)...)
	case e >= 0 && e <= 10:
		return append(appendTo, encodeMediumNumber(negative, e, m, buf)...)
	case e >= 11:
		return append(appendTo, encodeLargeNumber(negative, e, m, buf)...)
	}
	panic("unreachable")
}

// encodeVarExpNumber encodes a Uvarint exponent and mantissa into a buffer;
// only used for small (less than 0) and large (greater than 10) exponents.
//
// If required, the mantissa should already be in ones complement.
//
// The encoding must fit in encInto. The mantissa m can overlap with encInto.
//
// Returns the length-adjusted buffer.
func encodeVarExpNumber(tag byte, expAscending bool, exp uint64, m []byte, encInto []byte) []byte {
	// Because m can overlap with encInto, we must first copy m to the right place
	// before modifying encInto.
	var n int
	if expAscending {
		n = EncLenUvarintAscending(exp)
	} else {
		n = EncLenUvarintDescending(exp)
	}
	l := 1 + n + len(m)
	if len(encInto) < l+1 {
		panic("buffer too short")
	}
	copy(encInto[1+n:], m)
	encInto[0] = tag
	if expAscending {
		EncodeUvarintAscending(encInto[1:1], exp)
	} else {
		EncodeUvarintDescending(encInto[1:1], exp)
	}
	encInto[l] = decimalTerminator
	return encInto[:l+1]
}

// encodeSmallNumber encodes the exponent and mantissa into a buffer; only used
// when the exponent is negative. See encodeVarExpNumber.
func encodeSmallNumber(negative bool, e int, m []byte, encInto []byte) []byte {
	if negative {
		onesComplement(m)
		return encodeVarExpNumber(decimalNegSmall, true, uint64(-e), m, encInto)
	}
	return encodeVarExpNumber(decimalPosSmall, false, uint64(-e), m, encInto)
}

// encodeLargeNumber encodes the exponent and mantissa into a buffer; only used
// when the exponent is larger than 10. See encodeVarExpNumber.
func encodeLargeNumber(negative bool, e int, m []byte, encInto []byte) []byte {
	if negative {
		onesComplement(m)
		return encodeVarExpNumber(decimalNegLarge, false, uint64(e), m, encInto)
	}
	return encodeVarExpNumber(decimalPosLarge, true, uint64(e), m, encInto)
}

// encodeMediumNumber encodes the exponent and mantissa into a buffer, only used
// when the exponent is in [0, 10].
//
// The encoding must fit in encInto. The mantissa m can overlap with encInto.
//
// Returns the length-adjusted buffer.
func encodeMediumNumber(negative bool, e int, m []byte, encInto []byte) []byte {
	l := 1 + len(m)
	if len(encInto) < l+1 {
		panic("buffer too short")
	}
	// Because m can overlap with encInto, we must first copy m to the right place
	// before modifying encInto.
	copy(encInto[1:], m)
	if negative {
		encInto[0] = decimalNegMedium - byte(e)
		onesComplement(encInto[1:l])
	} else {
		encInto[0] = decimalPosMedium + byte(e)
	}
	encInto[l] = decimalTerminator
	return encInto[:l+1]
}

// DecodeDecimalAscending returns the remaining byte slice after decoding and the decoded
// decimal from buf.
func DecodeDecimalAscending(buf []byte, tmp []byte) ([]byte, apd.Decimal, error) {
	return decodeDecimal(buf, tmp, false)
}

// DecodeDecimalDescending decodes decimals encoded with EncodeDecimalDescending.
func DecodeDecimalDescending(buf []byte, tmp []byte) ([]byte, apd.Decimal, error) {
	return decodeDecimal(buf, tmp, true)
}

func decodeDecimal(buf []byte, tmp []byte, invert bool) ([]byte, apd.Decimal, error) {
	// Handle the simplistic cases first.
	switch buf[0] {
	case decimalNaN, decimalNaNDesc:
		return buf[1:], apd.Decimal{Form: apd.NaN}, nil
	case decimalInfinity:
		return buf[1:], apd.Decimal{Form: apd.Infinite, Negative: invert}, nil
	case decimalNegativeInfinity:
		return buf[1:], apd.Decimal{Form: apd.Infinite, Negative: !invert}, nil
	case decimalZero:
		return buf[1:], apd.Decimal{}, nil
	}
	tmp = tmp[len(tmp):cap(tmp)]
	switch {
	case buf[0] == decimalNegLarge:
		// Negative large.
		e, m, r, tmp2, err := decodeLargeNumber(true, buf, tmp)
		if err != nil {
			return nil, apd.Decimal{}, err
		}
		d, err := makeDecimalFromMandE(!invert, e, m, tmp2)
		return r, d, err
	case buf[0] > decimalNegLarge && buf[0] <= decimalNegMedium:
		// Negative medium.
		e, m, r, tmp2, err := decodeMediumNumber(true, buf, tmp)
		if err != nil {
			return nil, apd.Decimal{}, err
		}
		d, err := makeDecimalFromMandE(!invert, e, m, tmp2)
		return r, d, err
	case buf[0] == decimalNegSmall:
		// Negative small.
		e, m, r, tmp2, err := decodeSmallNumber(true, buf, tmp)
		if err != nil {
			return nil, apd.Decimal{}, err
		}
		d, err := makeDecimalFromMandE(!invert, e, m, tmp2)
		return r, d, err
	case buf[0] == decimalPosLarge:
		// Positive large.
		e, m, r, tmp2, err := decodeLargeNumber(false, buf, tmp)
		if err != nil {
			return nil, apd.Decimal{}, err
		}
		d, err := makeDecimalFromMandE(invert, e, m, tmp2)
		return r, d, err
	case buf[0] >= decimalPosMedium && buf[0] < decimalPosLarge:
		// Positive medium.
		e, m, r, tmp2, err := decodeMediumNumber(false, buf, tmp)
		if err != nil {
			return nil, apd.Decimal{}, err
		}
		d, err := makeDecimalFromMandE(invert, e, m, tmp2)
		return r, d, err
	case buf[0] == decimalPosSmall:
		// Positive small.
		e, m, r, tmp2, err := decodeSmallNumber(false, buf, tmp)
		if err != nil {
			return nil, apd.Decimal{}, err
		}
		d, err := makeDecimalFromMandE(invert, e, m, tmp2)
		return r, d, err
	default:
		return nil, apd.Decimal{}, errors.Errorf("unknown prefix of the encoded byte slice: %q", buf)
	}
}

// getDecimalLen returns the length of an encoded decimal.
func getDecimalLen(buf []byte) (int, error) {
	m := buf[0]
	p := 1
	if m < decimalNaN || m > decimalNaNDesc {
		panic(fmt.Errorf("invalid tag %d", m))
	}
	switch m {
	case decimalNaN, decimalNegativeInfinity, decimalNaNDesc, decimalInfinity, decimalZero:
		return 1, nil
	case decimalNegLarge, decimalNegSmall, decimalPosLarge, decimalPosSmall:
		// Skip the varint exponent.
		l, err := getVarintLen(buf[p:])
		if err != nil {
			return 0, err
		}
		p += l
	}

	idx, err := findDecimalTerminator(buf[p:])
	if err != nil {
		return 0, err
	}
	return p + idx + 1, nil
}

// makeDecimalFromMandE reconstructs the decimal from the mantissa M and
// exponent E.
func makeDecimalFromMandE(negative bool, e int, m []byte, tmp []byte) (apd.Decimal, error) {
	if len(m) == 0 {
		return apd.Decimal{}, errors.New("expected mantissa, got zero bytes")
	}
	// ±dddd.
	b := tmp[:0]
	if n := len(m)*2 + 1; cap(b) < n {
		b = make([]byte, 0, n)
	}
	for _, v := range m {
		t := int(v) / 2
		if t < 0 || t > 99 {
			return apd.Decimal{}, errors.Errorf("base-100 encoded digit %d out of range [0,99]", t)
		}
		b = append(b, byte(t/10)+'0', byte(t%10)+'0')
	}
	if b[len(b)-1] == '0' {
		b = b[:len(b)-1]
	}

	exp := 2*e - len(b)
	dec := apd.Decimal{
		Exponent: int32(exp),
	}

	// We unsafely convert the []byte to a string to avoid the usual allocation
	// when converting to a string.
	s := *(*string)(unsafe.Pointer(&b))
	_, ok := dec.Coeff.SetString(s, 10)
	if !ok {
		return apd.Decimal{}, errors.Errorf("could not set big.Int's string value: %q", s)
	}
	dec.Negative = negative

	return dec, nil
}

// findDecimalTerminator finds the decimalTerminator in the given slice.
func findDecimalTerminator(buf []byte) (int, error) {
	if idx := bytes.IndexByte(buf, decimalTerminator); idx != -1 {
		return idx, nil
	}
	return -1, errors.Errorf("did not find terminator %#x in buffer %#x", decimalTerminator, buf)
}

func decodeSmallNumber(
	negative bool, buf []byte, tmp []byte,
) (e int, m []byte, rest []byte, newTmp []byte, err error) {
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

	idx, err := findDecimalTerminator(r)
	if err != nil {
		return 0, nil, nil, nil, err
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

func decodeMediumNumber(
	negative bool, buf []byte, tmp []byte,
) (e int, m []byte, rest []byte, newTmp []byte, err error) {
	idx, err := findDecimalTerminator(buf[1:])
	if err != nil {
		return 0, nil, nil, nil, err
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

func decodeLargeNumber(
	negative bool, buf []byte, tmp []byte,
) (e int, m []byte, rest []byte, newTmp []byte, err error) {
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

	idx, err := findDecimalTerminator(r)
	if err != nil {
		return 0, nil, nil, nil, err
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
func EncodeNonsortingDecimal(b []byte, d *apd.Decimal) []byte {
	neg := d.Negative
	switch d.Form {
	case apd.Finite:
		// ignore
	case apd.Infinite:
		if neg {
			return append(b, decimalNegativeInfinity)
		}
		return append(b, decimalInfinity)
	case apd.NaN:
		return append(b, decimalNaN)
	default:
		panic(errors.Errorf("unknown form: %s", d.Form))
	}

	// We only encode "0" as decimalZero. All others ("0.0", "-0", etc) are
	// encoded like normal values.
	if d.IsZero() && !neg && d.Exponent == 0 {
		return append(b, decimalZero)
	}

	// Determine the exponent of the decimal, with the
	// exponent defined as .xyz * 10^exp.
	nDigits := int(d.NumDigits())
	e := nDigits + int(d.Exponent)

	bNat := d.Coeff.Bits()

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
// after encoding. The function first encodes the absolute value of a
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

// DecodeNonsortingDecimal returns the decoded decimal from buf encoded with
// EncodeNonsortingDecimal. buf is assumed to contain only the encoded decimal,
// as the function does not know from the encoding itself what the length
// of the encoded value is.
func DecodeNonsortingDecimal(buf []byte, tmp []byte) (apd.Decimal, error) {
	var dec apd.Decimal
	err := DecodeIntoNonsortingDecimal(&dec, buf, tmp)
	return dec, err
}

// DecodeIntoNonsortingDecimal is like DecodeNonsortingDecimal, but it operates
// on the passed-in *apd.Decimal instead of producing a new one.
func DecodeIntoNonsortingDecimal(dec *apd.Decimal, buf []byte, tmp []byte) error {
	*dec = apd.Decimal{}
	switch buf[0] {
	case decimalNaN:
		dec.Form = apd.NaN
		return nil
	case decimalNegativeInfinity:
		dec.Form = apd.Infinite
		dec.Negative = true
		return nil
	case decimalInfinity:
		dec.Form = apd.Infinite
		return nil
	case decimalZero:
		return nil
	}

	dec.Form = apd.Finite
	switch {
	case buf[0] == decimalNegLarge:
		if err := decodeNonsortingDecimalValue(dec, false, buf[1:], tmp); err != nil {
			return err
		}
		dec.Negative = true
		return nil
	case buf[0] == decimalNegMedium:
		decodeNonsortingDecimalValueWithoutExp(dec, buf[1:], tmp)
		dec.Negative = true
		return nil
	case buf[0] == decimalNegSmall:
		if err := decodeNonsortingDecimalValue(dec, true, buf[1:], tmp); err != nil {
			return err
		}
		dec.Negative = true
		return nil
	case buf[0] == decimalPosSmall:
		if err := decodeNonsortingDecimalValue(dec, true, buf[1:], tmp); err != nil {
			return err
		}
		return nil
	case buf[0] == decimalPosMedium:
		decodeNonsortingDecimalValueWithoutExp(dec, buf[1:], tmp)
		return nil
	case buf[0] == decimalPosLarge:
		if err := decodeNonsortingDecimalValue(dec, false, buf[1:], tmp); err != nil {
			return err
		}
		return nil
	default:
		return errors.Errorf("unknown decimal prefix of the encoded byte slice: %q", buf)
	}
}

func decodeNonsortingDecimalValue(dec *apd.Decimal, negExp bool, buf, tmp []byte) error {
	// Decode the exponent.
	buf, e, err := DecodeUvarintAscending(buf)
	if err != nil {
		return err
	}
	if negExp {
		e = -e
	}

	bi := &dec.Coeff
	bi.SetBytes(buf)

	// Set the decimal's scale.
	nDigits := int(apd.NumDigits(bi))
	exp := int(e) - nDigits
	dec.Exponent = int32(exp)
	return nil
}

func decodeNonsortingDecimalValueWithoutExp(dec *apd.Decimal, buf, tmp []byte) {
	bi := &dec.Coeff
	bi.SetBytes(buf)

	// Set the decimal's scale.
	nDigits := apd.NumDigits(bi)
	dec.Exponent = -int32(nDigits)
}

// UpperBoundNonsortingDecimalSize returns the upper bound number of bytes
// that the decimal will need for the non-sorting encoding.
func UpperBoundNonsortingDecimalSize(d *apd.Decimal) int {
	// Makeup of upper bound size:
	// - 1 byte for the prefix
	// - maxVarintSize for the exponent
	// - WordLen for the big.Int bytes
	return 1 + maxVarintSize + WordLen(d.Coeff.Bits())
}

// upperBoundNonsortingDecimalUnscaledSize is the same as
// UpperBoundNonsortingDecimalSize but for a decimal with the given unscaled
// length. The upper bound here may not be as tight as the one returned by
// UpperBoundNonsortingDecimalSize.
func upperBoundNonsortingDecimalUnscaledSize(unscaledLen int) int {
	// The number of digits needed to represent a base 10 number of length
	// unscaledLen in base 2.
	unscaledLenBase2 := float64(unscaledLen) * math.Log2(10)
	unscaledLenBase2Words := math.Ceil(unscaledLenBase2 / 8 / float64(bigWordSize))
	unscaledLenWordRounded := int(unscaledLenBase2Words) * bigWordSize
	// Makeup of upper bound size:
	// - 1 byte for the prefix
	// - maxVarintSize for the exponent
	// - unscaledLenWordRounded for the big.Int bytes
	return 1 + maxVarintSize + unscaledLenWordRounded
}

// Taken from math/big/arith.go.
const bigWordSize = int(unsafe.Sizeof(big.Word(0)))

// WordLen returns the size in bytes of the given array of Words.
func WordLen(nat []big.Word) int {
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
