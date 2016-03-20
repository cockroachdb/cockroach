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
// is encoded as an unsigned varint. Next, the absolute value of
// the digit string is added as a big-endian byte slice. Finally,
// a null terminator is appended to the end.
func EncodeDecimalAscending(b []byte, d *inf.Dec) []byte {
	return encodeDecimal(b, d, false)
}

// EncodeDecimalDescending is the descending version of EncodeDecimalAscending.
func EncodeDecimalDescending(b []byte, d *inf.Dec) []byte {
	return encodeDecimal(b, d, true)
}

func encodeDecimal(b []byte, d *inf.Dec, invert bool) []byte {
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
	nDigits, formatted := numDigits(bi, b[len(b):])
	e := int(-d.Scale()) + nDigits

	// Handle big.Int having zeros at the end of its
	// string by dividing them off (ie. 12300 -> 123).
	tens := 0
	if formatted != nil {
		tens = trailingZerosFromBytes(formatted)
	} else {
		tens = trailingZeros(bi, b[len(b):])
	}
	if tens > 0 {
		// If the decimal's big.Int hasn't been copied already, copy
		// it now because we will be modifying it.
		from := bi
		if bi == d.UnscaledBig() {
			bi = new(big.Int)
			from = d.UnscaledBig()
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
			div = div.Exp(div, pow, nil)
		}
		bi = bi.Div(from, div)
	}
	bNat := bi.Bits()

	var buf []byte
	if n := wordLen(bNat) + maxVarintSize + 2; n <= cap(b)-len(b) {
		buf = b[len(b) : len(b)+n]
	} else {
		buf = make([]byte, n)
	}

	switch {
	case neg && e > 0:
		buf[0] = decimalNegValPosExp
		n := encodeDecimalValue(true, false, uint64(e), bNat, buf[1:])
		return append(b, buf[:n+2]...)
	case neg && e == 0:
		buf[0] = decimalNegValZeroExp
		n := encodeDecimalValueWithoutExp(true, bNat, buf[1:])
		return append(b, buf[:n+2]...)
	case neg && e < 0:
		buf[0] = decimalNegValNegExp
		n := encodeDecimalValue(true, true, uint64(-e), bNat, buf[1:])
		return append(b, buf[:n+2]...)
	case !neg && e < 0:
		buf[0] = decimalPosValNegExp
		n := encodeDecimalValue(false, true, uint64(-e), bNat, buf[1:])
		return append(b, buf[:n+2]...)
	case !neg && e == 0:
		buf[0] = decimalPosValZeroExp
		n := encodeDecimalValueWithoutExp(false, bNat, buf[1:])
		return append(b, buf[:n+2]...)
	case !neg && e > 0:
		buf[0] = decimalPosValPosExp
		n := encodeDecimalValue(false, false, uint64(e), bNat, buf[1:])
		return append(b, buf[:n+2]...)
	}
	panic("unreachable")
}

// encodeDecimalValue encodes the absolute value of a decimal's exponent
// and slice of digit bytes into buf, returning the number of bytes written.
// The function first encodes the the absolute value of a decimal's exponent
// as an unsigned varint. Next, the function copies the decimal's digits
// into the buffer. Finally, the function appends a decimal terminator to the
// end of the buffer. encodeDecimalValue reacts to positive/negative values and
// exponents by performing the proper ones complements to ensure proper logical
// sorting of values encoded in buf.
func encodeDecimalValue(negVal, negExp bool, exp uint64, digits []big.Word, buf []byte) int {
	n := putUvarint(buf, exp)

	trimmed := copyWords(buf[n:], digits)
	copy(buf[n:], trimmed)
	l := n + len(trimmed)

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

func encodeDecimalValueWithoutExp(negVal bool, digits []big.Word, buf []byte) int {
	trimmed := copyWords(buf, digits)
	copy(buf, trimmed)
	l := len(trimmed)

	if negVal {
		onesComplement(buf[:l])
	}

	buf[l] = decimalTerminator
	return l
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

	// TODO(pmattis): bytes.IndexByte is inefficient for small slices. This is
	// apparently fixed in go1.7. For now, we manually search for the terminator.
	// idx := bytes.IndexByte(buf, decimalTerminator)
	idx := -1
	for i, b := range buf {
		if b == decimalTerminator {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil, nil, util.Errorf("did not find terminator %#x in buffer %#x", decimalTerminator, buf)
	}
	dec := new(inf.Dec).SetScale(1)
	switch {
	case buf[0] == decimalNegValPosExp:
		decodeDecimalValue(dec, true, false, buf[1:idx], tmp)
		if !invert {
			dec.UnscaledBig().Neg(dec.UnscaledBig())
		}
		return buf[idx+1:], dec, nil
	case buf[0] == decimalNegValZeroExp:
		decodeDecimalValueWithoutExp(dec, true, buf[1:idx], tmp)
		if !invert {
			dec.UnscaledBig().Neg(dec.UnscaledBig())
		}
		return buf[idx+1:], dec, nil
	case buf[0] == decimalNegValNegExp:
		decodeDecimalValue(dec, true, true, buf[1:idx], tmp)
		if !invert {
			dec.UnscaledBig().Neg(dec.UnscaledBig())
		}
		return buf[idx+1:], dec, nil
	case buf[0] == decimalPosValNegExp:
		decodeDecimalValue(dec, false, true, buf[1:idx], tmp)
		if invert {
			dec.UnscaledBig().Neg(dec.UnscaledBig())
		}
		return buf[idx+1:], dec, nil
	case buf[0] == decimalPosValZeroExp:
		decodeDecimalValueWithoutExp(dec, false, buf[1:idx], tmp)
		if invert {
			dec.UnscaledBig().Neg(dec.UnscaledBig())
		}
		return buf[idx+1:], dec, nil
	case buf[0] == decimalPosValPosExp:
		decodeDecimalValue(dec, false, false, buf[1:idx], tmp)
		if invert {
			dec.UnscaledBig().Neg(dec.UnscaledBig())
		}
		return buf[idx+1:], dec, nil
	default:
		return nil, nil, util.Errorf("unknown prefix of the encoded byte slice: %q", buf)
	}
}

func decodeDecimalValue(dec *inf.Dec, negVal, negExp bool, buf, tmp []byte) {
	if negVal != negExp {
		onesComplement(buf)
	}
	e, l := getUvarint(buf)
	if negExp {
		e = -e
		onesComplement(buf[l:])
	}

	bi := dec.UnscaledBig()
	bi.SetBytes(buf[l:])
	nDigits, _ := numDigits(bi, tmp)
	exp := int(e) - nDigits
	dec.SetScale(inf.Scale(-exp))

	if negExp {
		onesComplement(buf[l:])
	}
	if negVal != negExp {
		onesComplement(buf)
	}
}

func decodeDecimalValueWithoutExp(dec *inf.Dec, negVal bool, buf, tmp []byte) {
	if negVal {
		onesComplement(buf)
	}
	dec.UnscaledBig().SetBytes(buf)
	if negVal {
		onesComplement(buf)
	}
}

// UpperBoundDecimalSize returns the upper bound number of bytes that the
// decimal will need for encoding.
func UpperBoundDecimalSize(d *inf.Dec) int {
	// Makeup of upper bound size:
	// - 1 byte for the prefix
	// - maxVarintSize for the exponent
	// - wordLen for the big.Int bytes
	// - 1 byte for the terminator
	return 2 + maxVarintSize + wordLen(d.UnscaledBig().Bits())
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
