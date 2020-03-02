// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

// zeroedNulls is a zeroed out slice representing a bitmap of size MaxBatchSize.
// This is copied to efficiently set all nulls.
var zeroedNulls [(MaxBatchSize-1)/8 + 1]byte

// filledNulls is a slice representing a bitmap of size MaxBatchSize with every
// single bit set.
var filledNulls [(MaxBatchSize-1)/8 + 1]byte

// bitMask[i] is a byte with a single bit set at i.
var bitMask = [8]byte{0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80}

// flippedBitMask[i] is a byte with all bits set except at i.
var flippedBitMask = [8]byte{0xFE, 0xFD, 0xFB, 0xF7, 0xEF, 0xDF, 0xBF, 0x7F}

// onesMask is a byte where every bit is set to 1.
const onesMask = byte(255)

func init() {
	// Initializes filledNulls to the desired slice.
	for i := range filledNulls {
		filledNulls[i] = onesMask
	}
}

// Nulls represents a list of potentially nullable values using a bitmap. It is
// intended to be used alongside a slice (e.g. in the Vec interface) -- if the
// ith bit is off, then the ith element in that slice should be treated as NULL.
type Nulls struct {
	nulls []byte
	// maybeHasNulls is a best-effort representation of whether or not the
	// vector has any null values set. If it is false, there definitely will be
	// no null values. If it is true, there may or may not be null values.
	maybeHasNulls bool
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

// MaybeHasNulls returns true if the column possibly has any null values, and
// returns false if the column definitely has no null values.
func (n *Nulls) MaybeHasNulls() bool {
	return n.maybeHasNulls
}

// SetNullRange sets all the values in [startIdx, endIdx) to null.
func (n *Nulls) SetNullRange(startIdx int, endIdx int) {
	start, end := uint64(startIdx), uint64(endIdx)
	if start >= end {
		return
	}

	n.maybeHasNulls = true
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

// UnsetNullRange unsets all the nulls in the range [startIdx, endIdx).
// After using UnsetNullRange, n might not contain any null values,
// but maybeHasNulls could still be true.
func (n *Nulls) UnsetNullRange(startIdx, endIdx int) {
	start, end := uint64(startIdx), uint64(endIdx)
	if start >= end {
		return
	}
	if !n.maybeHasNulls {
		return
	}

	sIdx := start / 8
	eIdx := (end - 1) / 8

	// Case where mask only spans one byte.
	if sIdx == eIdx {
		mask := onesMask << (start % 8)
		if end%8 != 0 {
			mask = mask & (onesMask >> (8 - (end % 8)))
		}
		n.nulls[sIdx] |= mask
		return
	}

	// Case where mask spans at least two bytes.
	mask := onesMask << (start % 8)
	n.nulls[sIdx] |= mask
	if end%8 == 0 {
		n.nulls[eIdx] = onesMask
	} else {
		mask = onesMask >> (8 - (end % 8))
		n.nulls[eIdx] |= mask
	}

	for i := sIdx + 1; i < eIdx; i++ {
		n.nulls[i] = onesMask
	}
}

// Truncate sets all values with index greater than or equal to start to null.
func (n *Nulls) Truncate(start int) {
	end := len(n.nulls) * 8
	n.SetNullRange(start, end)
}

// UnsetNulls sets the column to have no null values.
func (n *Nulls) UnsetNulls() {
	n.maybeHasNulls = false

	startIdx := 0
	for startIdx < len(n.nulls) {
		startIdx += copy(n.nulls[startIdx:], filledNulls[:])
	}
}

// UnsetNullsAfter sets all values with index greater than or equal to idx to
// non-null.
func (n *Nulls) UnsetNullsAfter(idx int) {
	end := len(n.nulls) * 8
	n.UnsetNullRange(idx, end)
}

// SetNulls sets the column to have only null values.
func (n *Nulls) SetNulls() {
	n.maybeHasNulls = true

	startIdx := 0
	for startIdx < len(n.nulls) {
		startIdx += copy(n.nulls[startIdx:], zeroedNulls[:])
	}
}

// NullAt returns true if the ith value of the column is null.
func (n *Nulls) NullAt(i int) bool {
	return n.nulls[i>>3]&bitMask[i&7] == 0
}

// SetNull sets the ith value of the column to null.
func (n *Nulls) SetNull(i int) {
	n.maybeHasNulls = true
	n.nulls[i>>3] &= flippedBitMask[i&7]
}

// UnsetNull unsets the ith values of the column.
func (n *Nulls) UnsetNull(i int) {
	n.nulls[i>>3] |= bitMask[i&7]
}

// Remove the unused warning.
var (
	n = Nulls{}
	_ = n.swap
)

// swap swaps the null values at the argument indices. We implement the logic
// directly on the byte array rather than case on the result of NullAt to avoid
// having to take some branches.
func (n *Nulls) swap(iIdx, jIdx int) {
	i, j := uint64(iIdx), uint64(jIdx)
	// Get original null values.
	ni := (n.nulls[i/8] >> (i % 8)) & 0x1
	nj := (n.nulls[j/8] >> (j % 8)) & 0x1
	// Write into the correct positions.
	iMask := bitMask[i%8]
	jMask := bitMask[j%8]
	n.nulls[i/8] = (n.nulls[i/8] & ^iMask) | (nj << (i % 8))
	n.nulls[j/8] = (n.nulls[j/8] & ^jMask) | (ni << (j % 8))
}

// set copies over a slice [args.SrcStartIdx: args.SrcEndIdx] of
// args.Src.Nulls() and puts it into this nulls starting at args.DestIdx. If
// the length of this nulls is smaller than args.DestIdx, then this nulls is
// extended; otherwise, any overlapping old values are overwritten, and this
// nulls is also extended if necessary.
func (n *Nulls) set(args SliceArgs) {
	if args.SrcStartIdx == args.SrcEndIdx {
		return
	}
	toDuplicate := args.SrcEndIdx - args.SrcStartIdx
	outputLen := args.DestIdx + toDuplicate
	// We will need ceil(outputLen/8) bytes to encode the combined nulls.
	needed := (outputLen-1)/8 + 1
	current := len(n.nulls)
	if current < needed {
		n.nulls = append(n.nulls, filledNulls[:needed-current]...)
	}
	// First, we unset the whole range that is overwritten. If there are any NULL
	// values in the source, those will be copied over below, one at a time.
	n.UnsetNullRange(args.DestIdx, args.DestIdx+toDuplicate)
	if args.Src.MaybeHasNulls() {
		src := args.Src.Nulls()
		if args.Sel != nil {
			for i := 0; i < toDuplicate; i++ {
				if src.NullAt(args.Sel[args.SrcStartIdx+i]) {
					n.SetNull(args.DestIdx + i)
				}
			}
		} else {
			for i := 0; i < toDuplicate; i++ {
				// TODO(yuzefovich): this can be done more efficiently with a bitwise OR:
				// like n.nulls[i] |= vec.nulls[i].
				if src.NullAt(args.SrcStartIdx + i) {
					n.SetNull(args.DestIdx + i)
				}
			}
		}
	}
}

// Slice returns a new Nulls representing a slice of the current Nulls from
// [start, end).
func (n *Nulls) Slice(start int, end int) Nulls {
	startUnsigned, endUnsigned := uint64(start), uint64(end)
	if !n.maybeHasNulls {
		return NewNulls(end - start)
	}
	if start >= end {
		return NewNulls(0)
	}
	s := NewNulls(end - start)
	s.maybeHasNulls = true
	mod := startUnsigned % 8
	startIdx := start / 8
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
				s.nulls[i] |= n.nulls[startIdx+i+1] << (8 - mod)
			}
		}
	}
	// Zero out any trailing bits in the final byte.
	endBits := (endUnsigned - startUnsigned) % 8
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

// SetNullBitmap sets the null bitmap. size corresponds to how many elements
// this bitmap represents. The bits past the end of this size will be set to
// valid.
func (n *Nulls) SetNullBitmap(bm []byte, size int) {
	n.nulls = bm
	n.maybeHasNulls = false
	// Set all indices as valid past the last element.
	if len(bm) > 0 && size != 0 {
		// Set the last bits in the last element in which we want to preserve null
		// information. mod, if non-zero, is the number of bits we don't want to
		// overwrite (otherwise all bits are important). Note that we cast size to a
		// uint64 to avoid extra instructions when modding.
		mod := uint64(size) % 8
		endIdx := size - 1
		if mod != 0 {
			bm[endIdx/8] |= onesMask << mod
		}
		// Fill the rest of the bitmap.
		for i := (endIdx / 8) + 1; i < len(bm); {
			i += copy(bm[i:], filledNulls[:])
		}
	}

	for i := 0; i < len(bm); i++ {
		if bm[i] != onesMask {
			n.maybeHasNulls = true
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
	if n.maybeHasNulls && n2.maybeHasNulls {
		for i := 0; i < len(n.nulls); i++ {
			nulls[i] = n.nulls[i] & n2.nulls[i]
		}
		// If n2 is longer, we can just copy the remainder.
		copy(nulls[len(n.nulls):], n2.nulls[len(n.nulls):])
	} else if n.maybeHasNulls {
		copy(nulls, n.nulls)
	} else if n2.maybeHasNulls {
		copy(nulls, n2.nulls)
	}
	return &Nulls{
		maybeHasNulls: n.maybeHasNulls || n2.maybeHasNulls,
		nulls:         nulls,
	}
}

// Copy returns a copy of n which can be modified independently.
func (n *Nulls) Copy() Nulls {
	c := Nulls{
		maybeHasNulls: n.maybeHasNulls,
		nulls:         make([]byte, len(n.nulls)),
	}
	copy(c.nulls, n.nulls)
	return c
}
