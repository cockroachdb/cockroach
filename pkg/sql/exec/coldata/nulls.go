// Copyright 2018 The Cockroach Authors.
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

package coldata

// zeroedNulls is a zeroed out slice representing a bitmap of size BatchSize.
// This is copied to efficiently set all nulls.
var zeroedNulls [(BatchSize-1)/8 + 1]byte

// filledNulls is a slice representing a bitmap of size BatchSize with every
// single bit set.
var filledNulls [(BatchSize-1)/8 + 1]byte

// bitMask[i] is a byte with a single bit set at i.
var bitMask = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}

// flippedBitMask[i] is a byte with all bits set except at i.
var flippedBitMask = [8]byte{254, 253, 251, 247, 239, 223, 191, 127}

// onesMask is a byte where every bit is set to 1.
const onesMask = byte(255)

func init() {
	// Initializes filledNulls to the desired slice.
	for i := range filledNulls {
		filledNulls[i] = onesMask
	}
}

// Nulls represents a list of potentially nullable values.
type Nulls struct {
	nulls []byte
	// hasNulls represents whether or not the memColumn has any null values set.
	hasNulls bool
}

// NewNulls returns a new nulls vector, initialized with a length.
func NewNulls(len int) Nulls {
	if len > 0 {
		n := Nulls{
			nulls: make([]byte, (len-1)/8+1),
		}
		n.UnsetNulls()
		return n
	}
	return Nulls{
		nulls: make([]byte, 0),
	}
}

// HasNulls returns true if the column has any null values.
func (n *Nulls) HasNulls() bool {
	return n.hasNulls
}

// NullAt returns true if the ith value of the column is null.
func (n *Nulls) NullAt(i uint16) bool {
	return n.NullAt64(uint64(i))
}

// SetNull sets the ith value of the column to null.
func (n *Nulls) SetNull(i uint16) {
	n.SetNull64(uint64(i))
}

// SetNullRange sets all the values in [start, end) to null.
func (n *Nulls) SetNullRange(start uint64, end uint64) {
	if start >= end {
		return
	}

	n.hasNulls = true
	sIdx := start / 8
	eIdx := (end - 1) / 8

	// Case where mask only spans one byte.
	if sIdx == eIdx {
		mask := onesMask >> (8 - (start % 8))
		// Mask the end if needed.
		if end%8 != 0 {
			mask |= onesMask << (end % 8)
		}
		n.nulls[sIdx] &= mask
		return
	}

	// Case where mask spans at least two bytes.
	if sIdx < eIdx {
		mask := onesMask >> (8 - (start % 8))
		n.nulls[sIdx] &= mask

		if end%8 == 0 {
			n.nulls[eIdx] = 0
		} else {
			mask = onesMask << (end % 8)
			n.nulls[eIdx] &= mask
		}

		for i := sIdx + 1; i < eIdx; i++ {
			n.nulls[i] = 0
		}
	}
}

// UnsetNulls sets the column to have no null values.
func (n *Nulls) UnsetNulls() {
	n.hasNulls = false

	startIdx := 0
	for startIdx < len(n.nulls) {
		startIdx += copy(n.nulls[startIdx:], filledNulls[:])
	}
}

// SetNulls sets the column to have only null values.
func (n *Nulls) SetNulls() {
	n.hasNulls = true

	startIdx := 0
	for startIdx < len(n.nulls) {
		startIdx += copy(n.nulls[startIdx:], zeroedNulls[:])
	}
}

// NullAt64 returns true if the ith value of the column is null.
func (n *Nulls) NullAt64(i uint64) bool {
	return n.nulls[i/8]&bitMask[i%8] == 0
}

// SetNull64 sets the ith value of the column to null.
func (n *Nulls) SetNull64(i uint64) {
	n.hasNulls = true
	n.nulls[i/8] &= flippedBitMask[i%8]
}

// Extend extends the nulls vector with the next toAppend values from src,
// starting at srcStartIdx.
func (n *Nulls) Extend(src *Nulls, destStartIdx uint64, srcStartIdx uint16, toAppend uint16) {
	if toAppend == 0 {
		return
	}
	outputLen := destStartIdx + uint64(toAppend)
	// We will need ceil(outputLen/8) bytes to encode the combined nulls.
	needed := (outputLen-1)/8 + 1
	current := uint64(len(n.nulls))
	if current < needed {
		n.nulls = append(n.nulls, filledNulls[:needed-current]...)
	}
	if src.HasNulls() {
		for i := uint16(0); i < toAppend; i++ {
			// TODO(yuzefovich): this can be done more efficiently with a bitwise OR:
			// like n.nulls[i] |= vec.nulls[i].
			if src.NullAt(srcStartIdx + i) {
				n.SetNull64(destStartIdx + uint64(i))
			}
		}
	}
}

// ExtendWithSel extends the nulls vector with the next toAppend values from
// src, starting at srcStartIdx and using the provided selection vector.
func (n *Nulls) ExtendWithSel(
	src *Nulls, destStartIdx uint64, srcStartIdx uint16, toAppend uint16, sel []uint16,
) {
	if toAppend == 0 {
		return
	}
	outputLen := destStartIdx + uint64(toAppend)
	// We will need ceil(outputLen/8) bytes to encode the combined nulls.
	needed := (outputLen-1)/8 + 1
	current := uint64(len(n.nulls))
	if current < needed {
		n.nulls = append(n.nulls, filledNulls[:needed-current]...)
	}
	if src.HasNulls() {
		for i := uint16(0); i < toAppend; i++ {
			// TODO(yuzefovich): this can be done more efficiently with a bitwise OR:
			// like n.nulls[i] |= vec.nulls[i].
			if src.NullAt(sel[srcStartIdx+i]) {
				n.SetNull64(destStartIdx + uint64(i))
			}
		}
	}
}

// Slice returns a new Nulls representing a slice of the current Nulls from
// [start, end).
func (n *Nulls) Slice(start uint64, end uint64) Nulls {
	if !n.hasNulls {
		return NewNulls(int(end - start))
	}
	if start >= end {
		return NewNulls(0)
	}
	s := NewNulls(int(end - start))
	s.hasNulls = true
	mod := start % 8
	startIdx := int(start / 8)
	if mod == 0 {
		copy(s.nulls, n.nulls[startIdx:])
	} else {
		for i := range s.nulls {
			// If start is not a multiple of 8, we need to shift over the bitmap
			// to have the first index correspond.
			s.nulls[i] = n.nulls[startIdx+i] >> mod
			if startIdx+i+1 < len(n.nulls) {
				// And now bitwise or the remaining bits with the bits we want to
				// bring over from the next index.
				s.nulls[i] |= (n.nulls[startIdx+i+1] << (8 - mod))
			}
		}
	}
	// Zero out any trailing bits in the final byte.
	endBits := (end - start) % 8
	if endBits != 0 {
		mask := onesMask << endBits
		s.nulls[len(s.nulls)-1] |= mask
	}
	return s
}

// NullBitmap returns the null bitmap.
func (n *Nulls) NullBitmap() []byte {
	return n.nulls
}

// SetNullBitmap sets the null bitmap.
func (n *Nulls) SetNullBitmap(bm []byte) {
	n.nulls = bm
	n.hasNulls = false
	for _, i := range bm {
		if i != 0 {
			n.hasNulls = true
			return
		}
	}
}

// Or returns a new Nulls vector where NullAt(i) iff n1.NullAt(i) or
// n2.NullAt(i).
func (n *Nulls) Or(n2 *Nulls) *Nulls {
	// For simplicity, enforce that len(n.nulls) <= len(n2.nulls).
	if len(n.nulls) > len(n2.nulls) {
		n, n2 = n2, n
	}
	nulls := make([]byte, len(n2.nulls))
	if n.hasNulls && n2.hasNulls {
		for i := 0; i < len(n.nulls); i++ {
			nulls[i] = n.nulls[i] & n2.nulls[i]
		}
		// If n2 is longer, we can just copy the remainder.
		copy(nulls[len(n.nulls):], n2.nulls[len(n.nulls):])
	} else if n.hasNulls {
		copy(nulls, n.nulls)
	} else if n2.hasNulls {
		copy(nulls, n2.nulls)
	}
	return &Nulls{
		hasNulls: n.hasNulls || n2.hasNulls,
		nulls:    nulls,
	}
}

// Copy returns a copy of n which can be modified independently.
func (n *Nulls) Copy() Nulls {
	c := Nulls{
		hasNulls: n.hasNulls,
		nulls:    make([]byte, len(n.nulls)),
	}
	copy(c.nulls, n.nulls)
	return c
}
