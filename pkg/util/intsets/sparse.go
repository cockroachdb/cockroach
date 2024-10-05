// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package intsets

// Sparse is a set of integers. It is not thread-safe. It must be copied with
// the Copy method.
//
// Sparse is implemented as a linked list of blocks, each containing an offset
// and a bitmap. A block with offset=o contains an integer o+b if the b-th bit
// of the bitmap is set. Block offsets are always divisible by smallCutoff.
//
// For example, here is a diagram of the set {0, 1, 128, 129, 512}, where
// each block is denoted by {offset, bitmap}:
//
//	{0, ..011} ---> {128, ..011} ---> {512, ..001}
//
// Sparse is inspired by golang.org/x/tools/container/intsets. Sparse implements
// a smaller API, providing only the methods required by Fast. The omission of a
// Max method allows us to use a singly-linked list here instead of a
// circular, doubly-linked list.
type Sparse struct {
	root block
}

// block is a node in a singly-linked list with an offset and a bitmap. A block
// with offset=o contains an integer o+b if the b-th bit of the bitmap is set.
type block struct {
	offset int
	bits   bitmap
	next   *block
}

const (
	// MaxInt is the maximum integer that can be stored in a set.
	MaxInt = int(^uint(0) >> 1)
	// MinInt is the minimum integer that can be stored in a set.
	MinInt = -MaxInt - 1

	smallCutoffMask = smallCutoff - 1
)

func init() {
	if smallCutoff == 0 || (smallCutoff&smallCutoffMask) != 0 {
		panic("smallCutoff must be a power of two; see offset and bit")
	}
}

// offset returns the block offset for the given integer.
// Note: Bitwise AND NOT only works here because smallCutoff is a power of two.
//
//gcassert:inline
func offset(i int) int {
	return i &^ smallCutoffMask
}

// bit returns the bit within a block that should be set for the given integer.
// Note: Bitwise AND only works here because smallCutoff is a power of two.
//
//gcassert:inline
func bit(i int) int {
	return i & smallCutoffMask
}

// empty returns true if the block is empty, i.e., none of its bits have been
// set.
//
//gcassert:inline
func (s block) empty() bool {
	return s.bits == bitmap{}
}

// insertBlock inserts a block after prev and returns it. If prev is nil, a
// block is inserted at the front of the list.
func (s *Sparse) insertBlock(prev *block) *block {
	if s.Empty() {
		return &s.root
	}
	if prev == nil {
		// Insert a new block at the front of the list.
		second := s.root
		s.root = block{}
		s.root.next = &second
		return &s.root
	}
	// Insert a new block in the middle of the list.
	n := block{}
	n.next = prev.next
	prev.next = &n
	return &n
}

// removeBlock removes a block from the list. prev must be the block before b.
func (s *Sparse) removeBlock(prev, b *block) *block {
	if prev == nil {
		if b.next == nil {
			s.root = block{}
			return nil
		}
		s.root.offset = b.next.offset
		s.root.bits = b.next.bits
		s.root.next = b.next.next
		return &s.root
	}
	prev.next = prev.next.next
	return prev.next
}

// Clear empties the set.
func (s *Sparse) Clear() {
	s.root = block{}
}

// Add adds an integer to the set.
func (s *Sparse) Add(i int) {
	o := offset(i)
	b := bit(i)
	var last *block
	for sb := &s.root; sb != nil && sb.offset <= o; sb = sb.next {
		if sb.offset == o {
			sb.bits.Set(b)
			return
		}
		last = sb
	}
	n := s.insertBlock(last)
	n.offset = o
	n.bits.Set(b)
}

// Remove removes an integer from the set.
func (s *Sparse) Remove(i int) {
	o := offset(i)
	b := bit(i)
	var last *block
	for sb := &s.root; sb != nil && sb.offset <= o; sb = sb.next {
		if sb.offset == o {
			sb.bits.Unset(b)
			if sb.empty() {
				s.removeBlock(last, sb)
			}
			return
		}
		last = sb
	}
}

// Contains returns true if the set contains the given integer.
func (s Sparse) Contains(i int) bool {
	o := offset(i)
	b := bit(i)
	for sb := &s.root; sb != nil && sb.offset <= o; sb = sb.next {
		if sb.offset == o {
			return sb.bits.IsSet(b)
		}
	}
	return false
}

// Empty returns true if the set contains no integers.
func (s Sparse) Empty() bool {
	return s.root.empty()
}

// Len returns the number of integers in the set.
func (s Sparse) Len() int {
	l := 0
	for sb := &s.root; sb != nil; sb = sb.next {
		l += sb.bits.OnesCount()
	}
	return l
}

// LowerBound returns the smallest element >= startVal, or MaxInt if there is no
// such element.
func (s *Sparse) LowerBound(startVal int) int {
	if s.Empty() {
		return MaxInt
	}
	o := offset(startVal)
	b := bit(startVal)
	for sb := &s.root; sb != nil; sb = sb.next {
		if sb.offset > o {
			v, _ := sb.bits.Next(0)
			return v + sb.offset
		}
		if sb.offset == o {
			if v, ok := sb.bits.Next(b); ok {
				return v + sb.offset
			}
		}
	}
	return MaxInt
}

// Min returns the minimum value in the set. If the set is empty, MaxInt is
// returned.
func (s *Sparse) Min() int {
	if s.Empty() {
		return MaxInt
	}
	b := s.root
	v, _ := b.bits.Next(0)
	return v + b.offset
}

// Copy sets the receiver to a copy of rhs, which can then be modified
// independently.
func (s *Sparse) Copy(rhs *Sparse) {
	var last *block
	sb := &s.root
	rb := &rhs.root
	for rb != nil {
		if sb == nil {
			sb = s.insertBlock(last)
		}
		sb.offset = rb.offset
		sb.bits = rb.bits
		last = sb
		sb = sb.next
		rb = rb.next
	}
	if last != nil {
		last.next = nil
	}
}

// UnionWith adds all the elements from rhs to this set.
func (s *Sparse) UnionWith(rhs *Sparse) {
	if rhs.Empty() {
		return
	}

	var last *block
	sb := &s.root
	rb := &rhs.root
	for rb != nil {
		if sb != nil && sb.offset == rb.offset {
			sb.bits.UnionWith(rb.bits)
			rb = rb.next
		} else if sb == nil || sb.offset > rb.offset {
			sb = s.insertBlock(last)
			sb.offset = rb.offset
			sb.bits = rb.bits
			rb = rb.next
		}
		last = sb
		sb = sb.next
	}
}

// IntersectionWith removes any elements not in rhs from this set.
func (s *Sparse) IntersectionWith(rhs *Sparse) {
	var last *block
	sb := &s.root
	rb := &rhs.root
	for sb != nil && rb != nil {
		switch {
		case sb.offset > rb.offset:
			rb = rb.next
		case sb.offset < rb.offset:
			sb = s.removeBlock(last, sb)
		default:
			sb.bits.IntersectionWith(rb.bits)
			if !sb.empty() {
				// If sb is not empty, then advance sb and last.
				//
				// If sb is empty, we advance neither sb nor last so that the
				// empty sb will be removed in the next iteration of the loop
				// (the sb.offset < rb.offset case), or after the loop (see the
				// comment below).
				last = sb
				sb = sb.next
			}
			rb = rb.next
		}
	}
	if sb == &s.root {
		// This is a special case that only happens when all the following are
		// true:
		//
		//   1. Either s or rhs has a single block.
		//   2. The first blocks of s and rhs have matching offsets.
		//   3. The intersection of the first blocks of s and rhs yields an
		//      empty block.
		//
		// In this case, the root block would not have been removed in the loop,
		// and it may have a non-zero offset and a non-nil next block, so we
		// clear it here.
		s.root = block{}
	}
	if last != nil {
		// At this point, last is a pointer to the last block in s that we've
		// intersected with a block in rhs. If there are no remaining blocks in
		// s, then last.next will be nil. If there are no remaining blocks in
		// rhs, then we must remove any blocks after last. Unconditionally
		// clearing last.next works in both cases.
		last.next = nil
	}
}

// Intersects returns true if s has any elements in common with rhs.
func (s *Sparse) Intersects(rhs *Sparse) bool {
	sb := &s.root
	rb := &rhs.root
	for sb != nil && rb != nil {
		switch {
		case sb.offset > rb.offset:
			rb = rb.next
		case sb.offset < rb.offset:
			sb = sb.next
		default:
			if sb.bits.Intersects(rb.bits) {
				return true
			}
			sb = sb.next
			rb = rb.next
		}
	}
	return false
}

// DifferenceWith removes any elements in rhs from this set.
func (s *Sparse) DifferenceWith(rhs *Sparse) {
	var last *block
	sb := &s.root
	rb := &rhs.root
	for sb != nil && rb != nil {
		switch {
		case sb.offset > rb.offset:
			rb = rb.next
		case sb.offset < rb.offset:
			last = sb
			sb = sb.next
		default:
			sb.bits.DifferenceWith(rb.bits)
			if sb.empty() {
				sb = s.removeBlock(last, sb)
			} else {
				last = sb
				sb = sb.next
			}
			rb = rb.next
		}
	}
}

// Equals returns true if the two sets are identical.
func (s *Sparse) Equals(rhs *Sparse) bool {
	sb := &s.root
	rb := &rhs.root
	for sb != nil && rb != nil {
		if sb.offset != rb.offset || sb.bits != rb.bits {
			return false
		}
		sb = sb.next
		rb = rb.next
	}
	return sb == nil && rb == nil
}

// SubsetOf returns true if rhs contains all the elements in s.
func (s *Sparse) SubsetOf(rhs *Sparse) bool {
	if s.Empty() {
		return true
	}
	sb := &s.root
	rb := &rhs.root
	for sb != nil && rb != nil {
		if sb.offset > rb.offset {
			rb = rb.next
			continue
		}
		if sb.offset < rb.offset {
			return false
		}
		if !sb.bits.SubsetOf(rb.bits) {
			return false
		}
		sb = sb.next
		rb = rb.next
	}
	return sb == nil
}
