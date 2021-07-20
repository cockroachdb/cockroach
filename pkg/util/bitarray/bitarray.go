// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bitarray

import (
	"bytes"
	"fmt"
	"math/rand"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// BitArray implements a bit string of arbitrary length.
//
// This uses a packed encoding (i.e. groups of 64 bits at a time) for
// memory efficiency and speed of bitwise operations (enables use of
// full machine registers for comparisons and logical operations),
// akin to the big.nat type.
//
// There is something fancy needed to handle sorting values properly:
// the last group of bits must be padded right (start on the MSB)
// inside its word to compare properly according to pg semantics.
//
// This type is designed for immutable instances. The functions and
// methods defined below never write to a bit array in-place. Of note,
// the ToWidth() and Next() functions will share the backing array
// between their operand and their result in some cases.
//
// For portability, the size of the backing word is guaranteed to be 64
// bits.
type BitArray struct {
	// words is the backing array.
	//
	// The leftmost bits in the literal representation are placed in the
	// MSB of each word.
	//
	// The last word contain the rightmost bits in the literal
	// representation, right-padded. For example if there are 3 bits
	// to store, the 3 MSB bits of the last word will be set and the
	// remaining LSB bits will be set to zero.
	//
	// The number of stored bits is actually:
	//   0 if lastBitsUsed = 0  or len(word) == 0
	//   otherwise, (len(words)-1)*numBitsPerWord + lastBitsUsed
	//
	// TODO(jutin, nathan): consider using the trick in bytes.Buffer of
	// keeping a static [1]word which word can initially point to to
	// avoid heap allocations in the common case of small arrays.
	words []word

	// lastBitsUsed is the number of bits in the last word that
	// participate in the value stored. It can only be zero
	// for empty bit arrays; otherwise it's always between 1 and
	// numBitsPerWord.
	//
	// For example:
	// - 0 bits in array: len(words) == 0, lastBitsUsed = 0
	// - 1 bits in array: len(words) == 1, lastBitsUsed = 1
	// - 64 bits in array: len(words) == 1, lastBitsUsed = 64
	// - 65 bits in array: len(words) == 2, lastBitsUsed = 1
	lastBitsUsed uint8
}

type word = uint64

const numBytesPerWord = 8
const numBitsPerWord = 64

// BitLen returns the number of bits stored.
func (d BitArray) BitLen() uint {
	if len(d.words) == 0 {
		return 0
	}
	return d.nonEmptyBitLen()
}

func (d BitArray) nonEmptyBitLen() uint {
	return uint(len(d.words)-1)*numBitsPerWord + uint(d.lastBitsUsed)
}

// String implements the fmt.Stringer interface.
func (d BitArray) String() string {
	var buf bytes.Buffer
	d.Format(&buf)
	return buf.String()
}

// Clone makes a copy of the bit array.
func (d BitArray) Clone() BitArray {
	return BitArray{
		words:        append([]word(nil), d.words...),
		lastBitsUsed: d.lastBitsUsed,
	}
}

// MakeZeroBitArray creates a bit array with the specified bit size.
func MakeZeroBitArray(bitLen uint) BitArray {
	a, b := EncodingPartsForBitLen(bitLen)
	return mustFromEncodingParts(a, b)
}

// ToWidth resizes the bit array to the specified size.
// If the specified width is shorter, bits on the right are truncated away.
// If the specified width is larger, zero bits are added on the right.
func (d BitArray) ToWidth(desiredLen uint) BitArray {
	bitlen := d.BitLen()
	if bitlen == desiredLen {
		// Nothing to do; fast path.
		return d
	}
	if desiredLen == 0 {
		// Nothing to do; fast path.
		return BitArray{}
	}
	if desiredLen < bitlen {
		// Destructive, we have to copy.
		words, lastBitsUsed := EncodingPartsForBitLen(desiredLen)
		copy(words, d.words[:len(words)])
		words[len(words)-1] &= (^word(0) << (numBitsPerWord - lastBitsUsed))
		return mustFromEncodingParts(words, lastBitsUsed)
	}

	// New length is larger.
	numWords, lastBitsUsed := SizesForBitLen(desiredLen)
	var words []word
	if numWords <= uint(cap(d.words)) {
		words = d.words[0:numWords]
	} else {
		words = make([]word, numWords)
		copy(words, d.words)
	}
	return mustFromEncodingParts(words, lastBitsUsed)
}

// Sizeof returns the size in bytes of the bit array and its components.
func (d BitArray) Sizeof() uintptr {
	return unsafe.Sizeof(d) + uintptr(numBytesPerWord*cap(d.words))
}

// IsEmpty returns true iff the array is empty.
func (d BitArray) IsEmpty() bool {
	return d.lastBitsUsed == 0
}

// MakeBitArrayFromInt64 creates a bit array with the specified
// size. The bits from the integer are written to the right of the bit
// array and the sign bit is extended.
func MakeBitArrayFromInt64(bitLen uint, val int64, valWidth uint) BitArray {
	if bitLen == 0 {
		return BitArray{}
	}
	d := MakeZeroBitArray(bitLen)
	if bitLen < valWidth {
		// Fast path, no sign extension to compute.
		d.words[len(d.words)-1] = word(val << (numBitsPerWord - bitLen))
		return d
	}
	if val&(1<<(valWidth-1)) != 0 {
		// Sign extend, fill ones in every word but the last.
		for i := 0; i < len(d.words)-1; i++ {
			d.words[i] = ^word(0)
		}
	}
	// Shift the value to its given number of bits, to position the sign
	// bit to the left.
	val = val << (numBitsPerWord - valWidth)
	// Shift right back with arithmetic shift to extend the sign bit.
	val = val >> (numBitsPerWord - valWidth)
	// Store the right part of the value in the last word.
	d.words[len(d.words)-1] = word(val << (numBitsPerWord - d.lastBitsUsed))
	// Store the left part in the next-to-last word, if any.
	if valWidth > uint(d.lastBitsUsed) {
		d.words[len(d.words)-2] = word(val >> d.lastBitsUsed)
	}
	return d
}

// AsInt64 returns the int constituted from the rightmost bits in the
// bit array.
func (d BitArray) AsInt64(nbits uint) int64 {
	if d.lastBitsUsed == 0 {
		// Fast path.
		return 0
	}

	lowPart := d.words[len(d.words)-1] >> (numBitsPerWord - d.lastBitsUsed)
	highPart := word(0)
	if nbits > uint(d.lastBitsUsed) && len(d.words) > 1 {
		highPart = d.words[len(d.words)-2] << d.lastBitsUsed
	}
	combined := lowPart | highPart
	signExtended := int64(combined<<(numBitsPerWord-nbits)) >> (numBitsPerWord - nbits)
	return signExtended
}

// LeftShiftAny performs a logical left shift, with a possible
// negative count.
// The number of bits to shift can be arbitrarily large (i.e. possibly
// larger than 64 in absolute value).
func (d BitArray) LeftShiftAny(n int64) BitArray {
	bitlen := d.BitLen()
	if n == 0 || bitlen == 0 {
		// Fast path.
		return d
	}

	r := MakeZeroBitArray(bitlen)
	if (n > 0 && n > int64(bitlen)) || (n < 0 && -n > int64(bitlen)) {
		// Fast path.
		return r
	}

	if n > 0 {
		// This is a left shift.
		dstWord := uint(0)
		srcWord := uint(uint64(n) / numBitsPerWord)
		srcShift := uint(uint64(n) % numBitsPerWord)
		for i, j := srcWord, dstWord; i < uint(len(d.words)); i++ {
			r.words[j] = d.words[i] << srcShift
			j++
		}
		for i, j := srcWord+1, dstWord; i < uint(len(d.words)); i++ {
			r.words[j] |= d.words[i] >> (numBitsPerWord - srcShift)
			j++
		}
	} else {
		// A right shift.
		n = -n
		srcWord := uint(0)
		dstWord := uint(uint64(n) / numBitsPerWord)
		srcShift := uint(uint64(n) % numBitsPerWord)
		for i, j := srcWord, dstWord; j < uint(len(r.words)); i++ {
			r.words[j] = d.words[i] >> srcShift
			j++
		}
		for i, j := srcWord, dstWord+1; j < uint(len(r.words)); i++ {
			r.words[j] |= d.words[i] << (numBitsPerWord - srcShift)
			j++
		}
		// Erase the trailing bits that are not used any more.
		// See #36606.
		if len(r.words) > 0 {
			r.words[len(r.words)-1] &= ^word(0) << (numBitsPerWord - r.lastBitsUsed)
		}
	}

	return r
}

// byteReprs contains the bit representation of the 256 possible
// groups of 8 bits.
var byteReprs = func() (ret [256]string) {
	for i := range ret {
		// Change this format if numBitsPerWord changes.
		ret[i] = fmt.Sprintf("%08b", i)
	}
	return ret
}()

// Format prints out the bit array to the buffer.
func (d BitArray) Format(buf *bytes.Buffer) {
	bitLen := d.BitLen()
	buf.Grow(int(bitLen))
	for i := uint(0); i < bitLen/numBitsPerWord; i++ {
		w := d.words[i]
		// Change this loop if numBitsPerWord changes.
		buf.WriteString(byteReprs[(w>>56)&0xff])
		buf.WriteString(byteReprs[(w>>48)&0xff])
		buf.WriteString(byteReprs[(w>>40)&0xff])
		buf.WriteString(byteReprs[(w>>32)&0xff])
		buf.WriteString(byteReprs[(w>>24)&0xff])
		buf.WriteString(byteReprs[(w>>16)&0xff])
		buf.WriteString(byteReprs[(w>>8)&0xff])
		buf.WriteString(byteReprs[(w>>0)&0xff])
	}
	remainingBits := bitLen % numBitsPerWord
	if remainingBits > 0 {
		lastWord := d.words[bitLen/numBitsPerWord]
		minShift := numBitsPerWord - 1 - remainingBits
		for i := numBitsPerWord - 1; i > int(minShift); i-- {
			bitVal := (lastWord >> uint(i)) & 1
			buf.WriteByte('0' + byte(bitVal))
		}
	}
}

// EncodingPartsForBitLen creates a word backing array and the
// "last bits used" value given the given total number of bits.
func EncodingPartsForBitLen(bitLen uint) ([]uint64, uint64) {
	if bitLen == 0 {
		return nil, 0
	}
	numWords, lastBitsUsed := SizesForBitLen(bitLen)
	words := make([]word, numWords)
	return words, lastBitsUsed
}

// SizesForBitLen computes the number of words and last bits used for
// the requested bit array size.
func SizesForBitLen(bitLen uint) (uint, uint64) {
	// This computes ceil(bitLen / numBitsPerWord).
	numWords := (bitLen + numBitsPerWord - 1) / numBitsPerWord
	lastBitsUsed := uint64(bitLen % numBitsPerWord)
	if lastBitsUsed == 0 {
		lastBitsUsed = numBitsPerWord
	}
	return numWords, lastBitsUsed
}

// Parse parses a bit array from the specified string.
func Parse(s string) (res BitArray, err error) {
	if len(s) == 0 {
		return res, nil
	}

	if s[0] == 'x' || s[0] == 'X' {
		return parseFromHex(s[1:])
	}
	return parseFromBinary(s)
}

func parseFromBinary(s string) (res BitArray, err error) {
	words, lastBitsUsed := EncodingPartsForBitLen(uint(len(s)))

	// Parse the bits.
	wordIdx := 0
	bitIdx := uint(0)
	curWord := word(0)
	for _, c := range s {
		val := word(c - '0')
		bitVal := val & 1
		if bitVal != val {
			// Note: the prefix "could not parse" is important as it is used
			// to detect parsing errors in tests.
			err := fmt.Errorf(`could not parse string as bit array: "%c" is not a valid binary digit`, c)
			return res, pgerror.WithCandidateCode(err, pgcode.InvalidTextRepresentation)
		}
		curWord |= bitVal << (63 - bitIdx)
		bitIdx = (bitIdx + 1) % numBitsPerWord
		if bitIdx == 0 {
			words[wordIdx] = curWord
			curWord = 0
			wordIdx++
		}
	}
	if bitIdx > 0 {
		// Ensure the last word is stored.
		words[wordIdx] = curWord
	}

	return FromEncodingParts(words, lastBitsUsed)
}

func parseFromHex(s string) (res BitArray, err error) {
	words, lastBitsUsed := EncodingPartsForBitLen(uint(len(s)) * 4)

	// Parse the bits.
	wordIdx := 0
	bitIdx := uint(0)
	curWord := word(0)
	for _, c := range s {
		var bitVal word
		if c >= '0' && c <= '9' {
			bitVal = word(c - '0')
		} else if c >= 'a' && c <= 'f' {
			bitVal = word(c-'a') + 10
		} else if c >= 'A' && c <= 'F' {
			bitVal = word(c-'A') + 10
		} else {
			// Note: the prefix "could not parse" is important as it is used
			// to detect parsing errors in tests.
			err := fmt.Errorf(`could not parse string as bit array: "%c" is not a valid hexadecimal digit`, c)
			return res, pgerror.WithCandidateCode(err, pgcode.InvalidTextRepresentation)
		}
		curWord |= bitVal << (60 - bitIdx)
		bitIdx = (bitIdx + 4) % numBitsPerWord
		if bitIdx == 0 {
			words[wordIdx] = curWord
			curWord = 0
			wordIdx++
		}
	}
	if bitIdx > 0 {
		// Ensure the last word is stored.
		words[wordIdx] = curWord
	}

	return FromEncodingParts(words, lastBitsUsed)
}

// Concat concatenates two bit arrays.
func Concat(lhs, rhs BitArray) BitArray {
	if lhs.lastBitsUsed == 0 {
		return rhs
	}
	if rhs.lastBitsUsed == 0 {
		return lhs
	}
	words := make([]word, (lhs.nonEmptyBitLen()+rhs.nonEmptyBitLen()+numBitsPerWord-1)/numBitsPerWord)

	// The first bits come from the lhs unchanged.
	copy(words, lhs.words)
	var lastBitsUsed uint8
	if lhs.lastBitsUsed == numBitsPerWord {
		// Fast path. Just concatenate.
		copy(words[len(lhs.words):], rhs.words)
		lastBitsUsed = rhs.lastBitsUsed
	} else {
		// We need to shift all the words in the RHS
		// by the lastBitsUsed of the LHS.
		rhsShift := lhs.lastBitsUsed
		targetWordIdx := len(lhs.words) - 1
		trailingBits := words[targetWordIdx]
		for _, w := range rhs.words {
			headingBits := w >> rhsShift
			combinedBits := trailingBits | headingBits
			words[targetWordIdx] = combinedBits
			targetWordIdx++
			trailingBits = w << (numBitsPerWord - rhsShift)
		}
		lastBitsUsed = lhs.lastBitsUsed + rhs.lastBitsUsed
		if lastBitsUsed > numBitsPerWord {
			// Some bits from the RHS didn't fill a
			// word, we need to fit them in the last word.
			words[targetWordIdx] = trailingBits
		}

		// Compute the final thing.
		lastBitsUsed %= numBitsPerWord
		if lastBitsUsed == 0 {
			lastBitsUsed = numBitsPerWord
		}
	}
	return BitArray{words: words, lastBitsUsed: lastBitsUsed}
}

// Not computes the complement of a bit array.
func Not(d BitArray) BitArray {
	res := d.Clone()
	for i, w := range res.words {
		res.words[i] = ^w
	}
	if res.lastBitsUsed > 0 {
		lastWord := len(res.words) - 1
		res.words[lastWord] &= (^word(0) << (numBitsPerWord - res.lastBitsUsed))
	}
	return res
}

// And computes the logical AND of two bit arrays.
// The caller must ensure they have the same bit size.
func And(lhs, rhs BitArray) BitArray {
	res := lhs.Clone()
	for i, w := range rhs.words {
		res.words[i] &= w
	}
	return res
}

// Or computes the logical OR of two bit arrays.
// The caller must ensure they have the same bit size.
func Or(lhs, rhs BitArray) BitArray {
	res := lhs.Clone()
	for i, w := range rhs.words {
		res.words[i] |= w
	}
	return res
}

// Xor computes the logical XOR of two bit arrays.
// The caller must ensure they have the same bit size.
func Xor(lhs, rhs BitArray) BitArray {
	res := lhs.Clone()
	for i, w := range rhs.words {
		res.words[i] ^= w
	}
	return res
}

// Compare compares two bit arrays. They can have mixed sizes.
func Compare(lhs, rhs BitArray) int {
	n := len(lhs.words)
	if n > len(rhs.words) {
		n = len(rhs.words)
	}
	i := 0
	for ; i < n; i++ {
		lw := lhs.words[i]
		rw := rhs.words[i]
		if lw < rw {
			return -1
		}
		if lw > rw {
			return 1
		}
	}
	if i < len(rhs.words) {
		// lhs is shorter.
		return -1
	}
	if i < len(lhs.words) {
		// rhs is shorter.
		return 1
	}
	// Same length.
	if lhs.lastBitsUsed < rhs.lastBitsUsed {
		return -1
	}
	if lhs.lastBitsUsed > rhs.lastBitsUsed {
		return 1
	}
	return 0
}

// EncodingParts retrieves the encoding bits from the bit array. The
// words are presented in big-endian order, with the leftmost bits of
// the bitarray (MSB) in the MSB of each word.
func (d BitArray) EncodingParts() ([]uint64, uint64) {
	return d.words, uint64(d.lastBitsUsed)
}

// FromEncodingParts creates a bit array from the encoding parts.
func FromEncodingParts(words []uint64, lastBitsUsed uint64) (BitArray, error) {
	if lastBitsUsed > numBitsPerWord {
		err := fmt.Errorf("FromEncodingParts: lastBitsUsed must not exceed %d, got %d",
			errors.Safe(numBitsPerWord), errors.Safe(lastBitsUsed))
		return BitArray{}, pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
	}
	return BitArray{
		words:        words,
		lastBitsUsed: uint8(lastBitsUsed),
	}, nil
}

// mustFromEncodingParts is like FromEncodingParts but errors cause a panic.
func mustFromEncodingParts(words []uint64, lastBitsUsed uint64) BitArray {
	ba, err := FromEncodingParts(words, lastBitsUsed)
	if err != nil {
		panic(err)
	}
	return ba
}

// Rand generates a random bit array of the specified length.
func Rand(rng *rand.Rand, bitLen uint) BitArray {
	d := MakeZeroBitArray(bitLen)
	for i := range d.words {
		d.words[i] = rng.Uint64()
	}
	if len(d.words) > 0 {
		d.words[len(d.words)-1] <<= (numBitsPerWord - d.lastBitsUsed)
	}
	return d
}

// Next returns the next possible bit array in lexicographic order.
// The backing array of words is shared if possible.
func Next(d BitArray) BitArray {
	if d.lastBitsUsed == 0 {
		return BitArray{words: []word{0}, lastBitsUsed: 1}
	}
	if d.lastBitsUsed < numBitsPerWord {
		res := d
		res.lastBitsUsed++
		return res
	}
	res := BitArray{
		words:        make([]word, len(d.words)+1),
		lastBitsUsed: 1,
	}
	copy(res.words, d.words)
	return res
}

// GetBitAtIndex extract bit at given index in the BitArray.
func (d BitArray) GetBitAtIndex(index int) (int, error) {
	// Check whether index asked is inside BitArray.
	if index < 0 || uint(index) >= d.BitLen() {
		err := fmt.Errorf("GetBitAtIndex: bit index %d out of valid range (0..%d)", index, int(d.BitLen())-1)
		return 0, pgerror.WithCandidateCode(err, pgcode.ArraySubscript)
	}
	// To extract bit at the given index, we have to determine the
	// position within words array, i.e. index/numBitsPerWord after
	// that checked the bit at residual index.
	if d.words[index/numBitsPerWord]&(word(1)<<(numBitsPerWord-1-uint(index)%numBitsPerWord)) != 0 {
		return 1, nil
	}
	return 0, nil
}

// SetBitAtIndex returns the BitArray with an updated bit at a given index.
func (d BitArray) SetBitAtIndex(index, toSet int) (BitArray, error) {
	res := d.Clone()
	// Check whether index asked is inside BitArray.
	if index < 0 || uint(index) >= res.BitLen() {
		err := fmt.Errorf("SetBitAtIndex: bit index %d out of valid range (0..%d)", index, int(res.BitLen())-1)
		return BitArray{}, pgerror.WithCandidateCode(err, pgcode.ArraySubscript)
	}
	// To update bit at the given index, we have to determine the
	// position within words array, i.e. index/numBitsPerWord after
	// that updated the bit at residual index.
	// Forcefully making bit at the index to 0.
	res.words[index/numBitsPerWord] &= ^(word(1) << (numBitsPerWord - 1 - uint(index)%numBitsPerWord))
	// Updating value at the index to toSet.
	res.words[index/numBitsPerWord] |= word(toSet) << (numBitsPerWord - 1 - uint(index)%numBitsPerWord)
	return res, nil
}
