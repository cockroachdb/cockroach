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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package encoding

import (
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

// EncodeDecimalAscending returns the resulting byte slice with the
// encoded decimal appended to b.
//
// The encoding assumes that any number can be written as ±0.xyz... * 10^exp,
// where xyz is a digit string, x != 0, and the last decimal in xyz is also
// not 0.
//
// The encoding uses its first byte to split decimals into 7 distinct
// ordered groups (no NaN or Infinity support yet). The groups can
// be seen in encoding.go's const definition. Following this, the
// absolute value of the exponent of the decimal (as defined above)
// is encoded as an unsigned varint. Finally, the absolute value of
// the digit string is added as a big-endian byte slice, with the
// length of this byte slice appended to its front as an unsigned
// varint.
//
// All together, the encoding looks like:
//   <marker><uvarint exponent><uvarint length prefix><length bytes of big-endian encoded big.Int>.
func EncodeDecimalAscending(b []byte, d *inf.Dec) []byte {
	return encodeDecimal(b, d, false)
}

// EncodeDecimalDescending is the descending version of EncodeDecimalAscending.
func EncodeDecimalDescending(b []byte, d *inf.Dec) []byte {
	return encodeDecimal(b, d, true)
}

func encodeDecimal(b []byte, d *inf.Dec, invert bool) []byte {
	tmp := b[len(b):]
	neg := false
	bi := d.UnscaledBig()
	switch bi.Sign() {
	case -1:
		neg = !invert

		// Make a deep copy of the decimal's big.Int by calling Neg.
		// We shouldn't be modifying the provided argument's
		// internal big.Int, so this works like a copy-on-write scheme.
		bi = new(big.Int)
		bi = bi.Neg(d.UnscaledBig())
	case 0:
		return append(b, decimalZero)
	case 1:
		neg = invert
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
	if n := UpperBoundDecimalSize(d); n <= cap(b)-len(b) {
		// We append the marker directly to the input buffer b below, so
		// we are off by 1 for each of these, which explains the adjustments.
		buf = b[len(b)+1 : len(b)+1]
	} else {
		buf = make([]byte, 0, n-1)
	}

	switch {
	case neg && e > 0:
		b = append(b, decimalNegValPosExp)
		buf = encodeDecimalValue(true, false, uint64(e), bNat, buf)
		return append(b, buf...)
	case neg && e == 0:
		b = append(b, decimalNegValZeroExp)
		buf = encodeDecimalValueWithoutExp(true, bNat, buf)
		return append(b, buf...)
	case neg && e < 0:
		b = append(b, decimalNegValNegExp)
		buf = encodeDecimalValue(true, true, uint64(-e), bNat, buf)
		return append(b, buf...)
	case !neg && e < 0:
		b = append(b, decimalPosValNegExp)
		buf = encodeDecimalValue(false, true, uint64(-e), bNat, buf)
		return append(b, buf...)
	case !neg && e == 0:
		b = append(b, decimalPosValZeroExp)
		buf = encodeDecimalValueWithoutExp(false, bNat, buf)
		return append(b, buf...)
	case !neg && e > 0:
		b = append(b, decimalPosValPosExp)
		buf = encodeDecimalValue(false, false, uint64(e), bNat, buf)
		return append(b, buf...)
	}
	panic("unreachable")
}

// encodeDecimalValue encodes the absolute value of a decimal's exponent
// and slice of digit bytes into buf, returning the populated buffer after
// encoding. The function first encodes the the absolute value of a decimal's
// exponent as an unsigned varint. Next, the function determines the length
// of the decimal's digit buffer and encodes this as an unsigned varint. Finally,
// it copies the decimal's big-endian digits themselves into the buffer.
// encodeDecimalValue reacts to positive/negative values and exponents by
// performing the proper encoding routines and ones complements to ensure
// proper logical sorting of values encoded in buf.
func encodeDecimalValue(negVal, negExp bool, exp uint64, digits []big.Word, buf []byte) []byte {
	// Encode the exponent using a Uvarint.
	if negVal != negExp {
		buf = EncodeUvarintDescending(buf, exp)
	} else {
		buf = EncodeUvarintAscending(buf, exp)
	}
	expL := len(buf)

	// Encode the digit length and digits. First copy the digit bytes
	// into their furthest possible position to the right.
	buf = buf[:expL+maxVarintSize+wordLen(digits)]
	digitBytesBuf := copyWords(buf[expL+maxVarintSize:], digits)

	// Then encode this byte buffer's length using a Uvarint to the
	// front of the buffer.
	if negVal {
		buf = EncodeUvarintDescending(buf[:expL], uint64(len(digitBytesBuf)))
	} else {
		buf = EncodeUvarintAscending(buf[:expL], uint64(len(digitBytesBuf)))
	}
	digitBytesLenL := len(buf) - expL

	// Finally, shift the digit bytes to the left and truncate.
	if negVal {
		onesComplement(digitBytesBuf)
	}
	buf = buf[:expL+digitBytesLenL+len(digitBytesBuf)]
	copy(buf[expL+digitBytesLenL:], digitBytesBuf)

	return buf
}

func encodeDecimalValueWithoutExp(negVal bool, digits []big.Word, buf []byte) []byte {
	// Encode the digit length and digits. First copy the digit bytes
	// into their furthest possible position to the right.
	buf = buf[:maxVarintSize+wordLen(digits)]
	digitBytesBuf := copyWords(buf[maxVarintSize:], digits)

	// Then encode this byte buffer's length using a Uvarint to the
	// front of the buffer.
	if negVal {
		buf = EncodeUvarintDescending(buf[:0], uint64(len(digitBytesBuf)))
	} else {
		buf = EncodeUvarintAscending(buf[:0], uint64(len(digitBytesBuf)))
	}
	digitBytesLenL := len(buf)

	// Finally, shift the digit bytes to the left and truncate.
	if negVal {
		onesComplement(digitBytesBuf)
	}
	buf = buf[:digitBytesLenL+len(digitBytesBuf)]
	copy(buf[digitBytesLenL:], digitBytesBuf)

	return buf
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

// DecodeDecimalAscending returns the remaining byte slice after decoding and the decoded
// decimal from buf.
func DecodeDecimalAscending(buf []byte, tmp []byte) ([]byte, *inf.Dec, error) {
	return decodeDecimal(buf, false, tmp)
}

// DecodeDecimalDescending decodes floats encoded with EncodeDecimalDescending.
func DecodeDecimalDescending(buf []byte, tmp []byte) ([]byte, *inf.Dec, error) {
	return decodeDecimal(buf, true, tmp)
}

func decodeDecimal(buf []byte, invert bool, tmp []byte) ([]byte, *inf.Dec, error) {
	switch {
	// TODO(nvanbenschoten) These cases are left unimplemented until we add support for
	// Infinity and NaN Decimal values.
	// case buf[0] == decimalNaN:
	// case buf[0] == decimalNegativeInfinity:
	// case buf[0] == decimalInfinity:
	// case buf[0] == decimalNaNDesc:
	case buf[0] == decimalZero:
		return buf[1:], inf.NewDec(0, 0), nil
	}

	dec := new(inf.Dec).SetScale(1)
	switch {
	case buf[0] == decimalNegValPosExp:
		r, err := decodeDecimalValue(dec, true, false, buf[1:], tmp)
		if err != nil {
			return nil, nil, err
		}
		if !invert {
			dec.UnscaledBig().Neg(dec.UnscaledBig())
		}
		return r, dec, nil
	case buf[0] == decimalNegValZeroExp:
		r, err := decodeDecimalValueWithoutExp(dec, true, buf[1:], tmp)
		if err != nil {
			return nil, nil, err
		}
		if !invert {
			dec.UnscaledBig().Neg(dec.UnscaledBig())
		}
		return r, dec, nil
	case buf[0] == decimalNegValNegExp:
		r, err := decodeDecimalValue(dec, true, true, buf[1:], tmp)
		if err != nil {
			return nil, nil, err
		}
		if !invert {
			dec.UnscaledBig().Neg(dec.UnscaledBig())
		}
		return r, dec, nil
	case buf[0] == decimalPosValNegExp:
		r, err := decodeDecimalValue(dec, false, true, buf[1:], tmp)
		if err != nil {
			return nil, nil, err
		}
		if invert {
			dec.UnscaledBig().Neg(dec.UnscaledBig())
		}
		return r, dec, nil
	case buf[0] == decimalPosValZeroExp:
		r, err := decodeDecimalValueWithoutExp(dec, false, buf[1:], tmp)
		if err != nil {
			return nil, nil, err
		}
		if invert {
			dec.UnscaledBig().Neg(dec.UnscaledBig())
		}
		return r, dec, nil
	case buf[0] == decimalPosValPosExp:
		r, err := decodeDecimalValue(dec, false, false, buf[1:], tmp)
		if err != nil {
			return nil, nil, err
		}
		if invert {
			dec.UnscaledBig().Neg(dec.UnscaledBig())
		}
		return r, dec, nil
	default:
		return nil, nil, util.Errorf("unknown prefix of the encoded byte slice: %q", buf)
	}
}

func decodeDecimalValue(dec *inf.Dec, negVal, negExp bool, buf, tmp []byte) ([]byte, error) {
	// Decode the exponent.
	var e uint64
	var err error
	if negVal != negExp {
		buf, e, err = DecodeUvarintDescending(buf)
	} else {
		buf, e, err = DecodeUvarintAscending(buf)
	}
	if err != nil {
		return nil, err
	}
	if negExp {
		e = -e
	}

	// Decode the big.Int byte length.
	var biByteLen uint64
	if negVal {
		buf, biByteLen, err = DecodeUvarintDescending(buf)
	} else {
		buf, biByteLen, err = DecodeUvarintAscending(buf)
	}
	if err != nil {
		return nil, err
	}

	// Decode the big.Int and set on the decimal.
	if uint64(len(buf)) < biByteLen {
		return nil, util.Errorf("insufficient byte count to decode big.Int bytes, expected at least %d bytes: %v", biByteLen, buf)
	}
	biBuf := buf[:biByteLen]
	if negVal {
		// Use tmp to make sure we dont modify the immutable buf slice.
		if uint64(cap(tmp)) >= biByteLen {
			tmp = tmp[:biByteLen]
		} else {
			tmp = make([]byte, biByteLen)
		}
		copy(tmp, biBuf)
		onesComplement(tmp)
		biBuf = tmp
	}
	bi := dec.UnscaledBig()
	bi.SetBytes(biBuf)

	// Set the decimal's scale.
	nDigits, _ := numDigits(bi, tmp)
	exp := int(e) - nDigits
	dec.SetScale(inf.Scale(-exp))
	return buf[biByteLen:], nil
}

func decodeDecimalValueWithoutExp(dec *inf.Dec, negVal bool, buf, tmp []byte) ([]byte, error) {
	// Decode the big.Int byte length.
	var biByteLen uint64
	var err error
	if negVal {
		buf, biByteLen, err = DecodeUvarintDescending(buf)
	} else {
		buf, biByteLen, err = DecodeUvarintAscending(buf)
	}
	if err != nil {
		return nil, err
	}

	// Decode the big.Int and set on the decimal.
	if uint64(len(buf)) < biByteLen {
		return nil, util.Errorf("insufficient byte count to decode big.Int bytes, expected at least %d bytes: %v", biByteLen, buf)
	}
	biBuf := buf[:biByteLen]
	if negVal {
		// Use tmp to make sure we dont modify the immutable buf slice.
		if uint64(cap(tmp)) >= biByteLen {
			tmp = tmp[:biByteLen]
		} else {
			tmp = make([]byte, biByteLen)
		}
		copy(tmp, biBuf)
		onesComplement(tmp)
		biBuf = tmp
	}
	dec.UnscaledBig().SetBytes(biBuf)
	return buf[biByteLen:], nil
}

// UpperBoundDecimalSize returns the upper bound number of bytes that the
// decimal will need for encoding.
func UpperBoundDecimalSize(d *inf.Dec) int {
	// Makeup of upper bound size:
	// - 1 byte for the prefix
	// - maxVarintSize for the exponent
	// - maxVarintSize for the big.Int bytes length
	// - wordLen for the big.Int bytes
	return 1 + 2*maxVarintSize + wordLen(d.UnscaledBig().Bits())
}

// Taken from math/big/arith.go.
const bigWordSize = int(unsafe.Sizeof(big.Word(0)))

func wordLen(nat []big.Word) int {
	return len(nat) * bigWordSize
}

// copyWords was adapted from math/big/nat.go. It writes the value of
// nat into buf using big-endian encoding. len(buf) must be >= len(nat)*bigWordSize.
// The value of nat is encoded in the slice buf[i:], and the unused bytes
// at the beginning of buf are trimmed before returning.
func copyWords(buf []byte, nat []big.Word) []byte {
	i := len(buf)
	for _, d := range nat {
		for j := 0; j < bigWordSize; j++ {
			i--
			buf[i] = byte(d)
			d >>= 8
		}
	}

	for i < len(buf) && buf[i] == 0 {
		i++
	}

	return buf[i:]
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
	border *big.Int
}

func init() {
	digitBi := new(big.Int)
	var bigIntArr [tableSize]big.Int
	for i := 1; i <= tableSize; i++ {
		val := int(1 << uint(i-1))
		digits := 1
		for ; val > 10; val /= 10 {
			digits++
		}

		digitBi.SetInt64(int64(digits))
		digitsLookupTable[i] = tableVal{
			digits: digits,
			border: bigIntArr[i-1].Exp(bigInt10, digitBi, nil),
		}
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
		if bi.Cmp(val.border) < 0 {
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
