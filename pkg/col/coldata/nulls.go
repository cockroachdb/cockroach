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

	for idx := int(sIdx + 1); idx < int(eIdx); {
		idx += copy(n.nulls[idx:eIdx], zeroedNulls[:])
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

	for idx := int(sIdx + 1); idx < int(eIdx); {
		idx += copy(n.nulls[idx:eIdx], filledNulls[:])
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

// setSmallRange is a helper that copies over a slice [startIdx, startIdx+toSet)
// of src and puts it into this nulls starting at destIdx.
func (n *Nulls) setSmallRange(src *Nulls, destIdx, startIdx, toSet int) {
	for i := 0; i < toSet; i++ {
		if src.NullAt(startIdx + i) {
			n.SetNull(destIdx + i)
		} else {
			n.UnsetNull(destIdx + i)
		}
	}
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
	if args.Src.MaybeHasNulls() {
		n.maybeHasNulls = true
		src := args.Src.Nulls()
		if args.Sel != nil {
			// With the selection vector present, we can't do any smarts, so we
			// unset the whole range that is overwritten and then set new null
			// values one at a time.
			n.UnsetNullRange(args.DestIdx, args.DestIdx+toDuplicate)
			for i := 0; i < toDuplicate; i++ {
				if src.NullAt(args.Sel[args.SrcStartIdx+i]) {
					n.SetNull(args.DestIdx + i)
				}
			}
		} else {
			if toDuplicate > 16 && args.DestIdx%8 == args.SrcStartIdx%8 {
				// We have a special (but a very common) case when we're
				// copying a lot of elements, and the shifts within the nulls
				// vectors for the destination and the source ranges are the
				// same, so we can optimize the performance here.
				// The fact that shifts are the same allows us to copy all
				// elements as is (except for the first and the last which are
				// handled separately).
				dstStart := args.DestIdx / 8
				srcStart := args.SrcStartIdx / 8
				srcEnd := (args.SrcEndIdx-1)/8 + 1
				// Since the first and the last elements might not be fully
				// included in the range to be set, we're not touching them.
				copy(n.nulls[dstStart+1:], src.nulls[srcStart+1:srcEnd-1])
				// Handle the first element.
				n.setSmallRange(src, args.DestIdx, args.SrcStartIdx, 8-args.DestIdx%8)
				// Handle the last element.
				toSet := (args.DestIdx + toDuplicate) % 8
				if toSet == 0 {
					toSet = 8
				}
				offset := toDuplicate - toSet
				n.setSmallRange(src, args.DestIdx+offset, args.SrcStartIdx+offset, toSet)
				return
			}
			n.UnsetNullRange(args.DestIdx, args.DestIdx+toDuplicate)
			for i := 0; i < toDuplicate; i++ {
				if src.NullAt(args.SrcStartIdx + i) {
					n.SetNull(args.DestIdx + i)
				}
			}
		}
	} else {
		// No nulls in the source, so we unset the whole range that is
		// overwritten.
		n.UnsetNullRange(args.DestIdx, args.DestIdx+toDuplicate)
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

// SetNullBitmap sets the validity of first size elements in n according to bm.
// The bits past the end of this size will be set to valid. It is assumed that
// n has enough capacity to store size number of elements. If bm is zero length
// or if size is 0, then all elements will be set to valid.
func (n *Nulls) SetNullBitmap(bm []byte, size int) {
	if len(bm) == 0 || size == 0 {
		n.UnsetNulls()
		return
	}
	numBytesToCopy := (size-1)/8 + 1
	copy(n.nulls, bm[:numBytesToCopy])
	n.UnsetNullsAfter(size)
	// Compute precisely whether we have any invalid values or not.
	n.maybeHasNulls = false
	for i := 0; i < numBytesToCopy; i++ {
		if n.nulls[i] != onesMask {
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
	res := &Nulls{
		maybeHasNulls: n.maybeHasNulls || n2.maybeHasNulls,
		nulls:         make([]byte, len(n2.nulls)),
	}
	if n.maybeHasNulls && n2.maybeHasNulls {
		for i := 0; i < len(n.nulls); i++ {
			res.nulls[i] = n.nulls[i] & n2.nulls[i]
		}
		// If n2 is longer, we can just copy the remainder.
		copy(res.nulls[len(n.nulls):], n2.nulls[len(n.nulls):])
	} else if n.maybeHasNulls {
		copy(res.nulls, n.nulls)
		// We need to set all positions after len(n.nulls) to valid.
		res.UnsetNullsAfter(8 * len(n.nulls))
	} else if n2.maybeHasNulls {
		// Since n2 is not of a smaller length, we can copy its bitmap without
		// having to do anything extra.
		copy(res.nulls, n2.nulls)
	} else {
		// We need to set the whole bitmap to valid.
		res.UnsetNulls()
	}
	return res
}

// makeCopy returns a copy of n which can be modified independently.
func (n *Nulls) makeCopy() Nulls {
	c := Nulls{
		maybeHasNulls: n.maybeHasNulls,
		nulls:         make([]byte, len(n.nulls)),
	}
	copy(c.nulls, n.nulls)
	return c
}

// Copy copies the contents of other into n.
func (n *Nulls) Copy(other *Nulls) {
	n.maybeHasNulls = other.maybeHasNulls
	if cap(n.nulls) < len(other.nulls) {
		n.nulls = make([]byte, len(other.nulls))
	} else {
		n.nulls = n.nulls[:len(other.nulls)]
	}
	copy(n.nulls, other.nulls)
}
