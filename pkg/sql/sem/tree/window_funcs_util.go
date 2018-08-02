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

package tree

// PeerGroupChecker can check if a pair of row indices within a partition are
// in the same peer group.
type PeerGroupChecker interface {
	InSameGroup(i, j int) bool
}

// PeerGroupsIndicesHelper computes peer groups using given PeerGroupChecker.
// In ROWS and RANGE modes, it processes one peer group at a time and stores
// information only about single peer group. In GROUPS mode, it's behavior
// depends on the frame bounds; in the worst case, it stores max(F, O) peer
// groups at the same time, where F is the maximum number of peer groups within
// the frame at any point and O is the maximum of two offsets if we have
// OFFSET_FOLLOWING type of bound (both F and O are upper-bounded by total
// number of peer groups).
type PeerGroupsIndicesHelper struct {
	groups                RingBuffer // queue of information about peer groups using ring buffer
	peerGrouper           PeerGroupChecker
	headPeerGroupNum      int  // number of the peer group at the head of the queue
	tailPeerGroupRowCount int  // number of rows within the peer group at the tail of the queue
	allPeerGroupsSkipped  bool // in GROUP mode, indicates whether all peer groups were skipped during Init
	allRowsProcessed      bool // indicates whether peer groups for all rows within partition have been already computed
	unboundedFollowing    int  // index of the first row after all rows of the partition
}

// Init computes all peer groups necessary to perform calculations of a window
// function over the first row of the partition.
func (p *PeerGroupsIndicesHelper) Init(wfr *WindowFrameRun, peerGrouper PeerGroupChecker) {
	p.Reset(wfr.unboundedFollowing())
	p.peerGrouper = peerGrouper
	startIdxOfFirstPeerGroupWithinFrame := 0
	if wfr.Frame != nil && wfr.Frame.Mode == GROUPS && wfr.Frame.Bounds.StartBound.BoundType == OffsetFollowing {
		// In GROUPS mode with OFFSET_PRECEDING as a start bound, 'peerGroupOffset'
		// number of peer groups needs to be processed upfront before we get to
		// peer groups that will be within a frame of the first row.
		// If start bound is of type:
		// - UNBOUNDED_PRECEDING - we don't use this helper at all
		// - OFFSET_PRECEDING - no need to process any peer groups upfront
		// - CURRENT_ROW - no need to process any peer groups upfront
		// - OFFSET_FOLLOWING - processing is done here
		// - UNBOUNDED_FOLLOWING - invalid as a start bound
		//
		// We also cannot simply discard information about these peer groups: even
		// though they will never be within frames of any rows, we still might need
		// information about them. For example, with frame as follows:
		//   GROUPS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
		// when processing the rows from zeroth peer group, we will need to know
		// where zeroth peer group starts and how many rows it has, but the rows of
		// zeroth group will never be in any frame.
		peerGroupOffset := int(MustBeDInt(wfr.StartBoundOffset))
		peerGroupStartIdx := 0
		for peerGroupStartIdx < wfr.PartitionSize() && p.groups.Len() < peerGroupOffset {
			p.groups.Add(&IndexedValue{Idx: peerGroupStartIdx})
			p.tailPeerGroupRowCount = 1
			for ; peerGroupStartIdx+p.tailPeerGroupRowCount < wfr.PartitionSize(); p.tailPeerGroupRowCount++ {
				idx := peerGroupStartIdx + p.tailPeerGroupRowCount
				if !p.peerGrouper.InSameGroup(idx, idx-1) {
					break
				}
			}
			peerGroupStartIdx += p.tailPeerGroupRowCount
		}

		if peerGroupStartIdx == wfr.PartitionSize() {
			// Frame starts after all peer groups of the partition.
			p.allPeerGroupsSkipped = true
			// TODO: we won't need info about peer groups in this case. Should we free up the memory?
			return
		}

		startIdxOfFirstPeerGroupWithinFrame = peerGroupStartIdx
	}

	// Compute the first peer group that is within the frame.
	p.groups.Add(&IndexedValue{Idx: startIdxOfFirstPeerGroupWithinFrame})
	p.tailPeerGroupRowCount = 1
	for ; startIdxOfFirstPeerGroupWithinFrame+p.tailPeerGroupRowCount < wfr.PartitionSize(); p.tailPeerGroupRowCount++ {
		idx := startIdxOfFirstPeerGroupWithinFrame + p.tailPeerGroupRowCount
		if !p.peerGrouper.InSameGroup(idx, idx-1) {
			break
		}
	}
	if startIdxOfFirstPeerGroupWithinFrame+p.tailPeerGroupRowCount == wfr.PartitionSize() {
		p.allRowsProcessed = true
		return
	}

	if wfr.Frame != nil && wfr.Frame.Mode == GROUPS && wfr.Frame.Bounds.EndBound != nil && wfr.Frame.Bounds.EndBound.BoundType == OffsetFollowing {
		// In GROUPS mode, 'peerGroupOffset' number of peer groups need to be
		// processed upfront because they are within the frame of the first row.
		// If end bound is of type:
		// - UNBOUNDED_PRECEDING - invalid as an end bound
		// - OFFSET_PRECEDING - no need to process any peer groups upfront
		// - CURRENT_ROW - no need to process any more peer groups upfront
		// - OFFSET_FOLLOWING - processing is done here
		// - UNBOUNDED_FOLLOWING - we don't use this helper at all
		peerGroupOffset := int(MustBeDInt(wfr.EndBoundOffset))
		peerGroupStartIdx := startIdxOfFirstPeerGroupWithinFrame + p.tailPeerGroupRowCount
		for peerGroupStartIdx < wfr.PartitionSize() && p.groups.Len() <= peerGroupOffset {
			p.groups.Add(&IndexedValue{Idx: peerGroupStartIdx})
			p.tailPeerGroupRowCount = 1
			for ; peerGroupStartIdx+p.tailPeerGroupRowCount < wfr.PartitionSize(); p.tailPeerGroupRowCount++ {
				idx := peerGroupStartIdx + p.tailPeerGroupRowCount
				if !p.peerGrouper.InSameGroup(idx, idx-1) {
					break
				}
			}
			peerGroupStartIdx += p.tailPeerGroupRowCount
		}
		if peerGroupStartIdx == wfr.PartitionSize() {
			p.allRowsProcessed = true
		}
	}
}

// Update should be called after a window function has been computed over all
// rows in wfr.CurRowPeerGroupNum peer group. If not all rows have been already
// processed, it computes the next peer group.
func (p *PeerGroupsIndicesHelper) Update(wfr *WindowFrameRun) {
	if p.allPeerGroupsSkipped {
		// No peer groups to process.
		return
	}

	// nextPeerGroupStartIdx is the index of the first row that we haven't
	// computed peer group for.
	nextPeerGroupStartIdx := p.GetFirstPeerIdx(p.GetLastPeerGroupNum()) + p.tailPeerGroupRowCount

	if (wfr.Frame == nil || wfr.Frame.Mode == ROWS || wfr.Frame.Mode == RANGE) ||
		(wfr.Frame.Bounds.StartBound.BoundType == OffsetPreceding && wfr.CurRowPeerGroupNum-p.headPeerGroupNum > int(MustBeDInt(wfr.StartBoundOffset)) ||
			wfr.Frame.Bounds.StartBound.BoundType == CurrentRow ||
			(wfr.Frame.Bounds.StartBound.BoundType == OffsetFollowing && p.headPeerGroupNum-wfr.CurRowPeerGroupNum > int(MustBeDInt(wfr.StartBoundOffset)))) {
		// With default frame, ROWS or RANGE mode, we want to "discard" the only
		// peer group that we're storing information about. In GROUPS mode, with
		// start bound of type:
		// - OFFSET_PRECEDING we want to start discarding the "earliest" peer group
		//   only when the number of current row's peer group differs from the
		//   number of the earliest one by more than offset
		// - CURRENT_ROW we want to discard the earliest peer group
		// - OFFSET_FOLLOWING we want to start discarding the "earliest" peer group
		//	 only when the number of current row's peer group differs from the
		//	 number of the earliest one by more than offset
		p.groups.RemoveHead()
		p.headPeerGroupNum++
	}

	if p.allRowsProcessed {
		// No more peer groups to process.
		return
	}

	// Compute the next peer group that is just entering the frame.
	p.groups.Add(&IndexedValue{Idx: nextPeerGroupStartIdx})
	p.tailPeerGroupRowCount = 1
	for ; nextPeerGroupStartIdx+p.tailPeerGroupRowCount < wfr.PartitionSize(); p.tailPeerGroupRowCount++ {
		idx := nextPeerGroupStartIdx + p.tailPeerGroupRowCount
		if !p.peerGrouper.InSameGroup(idx, idx-1) {
			break
		}
	}
	if nextPeerGroupStartIdx+p.tailPeerGroupRowCount == wfr.PartitionSize() {
		p.allRowsProcessed = true
	}
}

// Reset allows us to reuse the same helper for all partitions when computing
// a particular window functions.
func (p *PeerGroupsIndicesHelper) Reset(unboundedFollowing int) {
	p.groups.reset()
	p.headPeerGroupNum = 0
	p.tailPeerGroupRowCount = 0
	p.allPeerGroupsSkipped = false
	p.allRowsProcessed = false
	p.unboundedFollowing = unboundedFollowing
}

// GetFirstPeerIdx returns index of the first peer within peer group of number
// peerGroupNum (counting from 0).
func (p *PeerGroupsIndicesHelper) GetFirstPeerIdx(peerGroupNum int) int {
	if p.allPeerGroupsSkipped {
		// Special case: we have skipped all peer groups in Init, so the frame is
		// always empty. It happens only with frames like GROUPS 100 FOLLOWING
		// which (if we have less than 100 peer groups total) behaves exactly like
		// GROUPS UNBOUNDED FOLLOWING (if it were allowed).
		return p.unboundedFollowing
	}
	posInBuffer := peerGroupNum - p.headPeerGroupNum
	if posInBuffer < 0 || p.groups.Len() < posInBuffer {
		panic("peerGroupNum out of bounds")
	}
	return p.groups.Get(posInBuffer).Idx
}

// GetPeerRowCount returns the number of rows within peer group of number
// peerGroupNum (counting from 0).
func (p *PeerGroupsIndicesHelper) GetPeerRowCount(peerGroupNum int) int {
	if p.allPeerGroupsSkipped {
		// Special case: we have skipped all peer groups in Init, so the frame is
		// always empty. It happens only with frames like GROUPS 100 FOLLOWING
		// if we have less than 100 peer groups total.
		return 0
	}
	posInBuffer := peerGroupNum - p.headPeerGroupNum
	if posInBuffer < 0 || p.groups.Len() < posInBuffer {
		panic("peerGroupNum out of bounds")
	}
	if posInBuffer == p.groups.Len()-1 {
		// Peer group of number peerGroupNum is the tail of the queue.
		return p.tailPeerGroupRowCount
	}
	// Look at the start of the following peer group to figure out the number
	// of rows within the current peer group.
	return p.groups.Get(posInBuffer+1).Idx - p.groups.Get(posInBuffer).Idx
}

// GetLastPeerGroupNum return the number of the last peer group in the queue.
func (p *PeerGroupsIndicesHelper) GetLastPeerGroupNum() int {
	if p.groups.Len() == 0 {
		panic("GetLastPeerGroupNum on empty RingBuffer")
	}
	return p.headPeerGroupNum + p.groups.Len() - 1
}

// IndexedValue combines a value from the row with the index of that row.
type IndexedValue struct {
	Value Datum
	Idx   int
}

// RingBufferInitialSize defines the initial size of the ring buffer.
const RingBufferInitialSize = 8

// RingBuffer is a deque of IndexedValues maintained over a ring buffer.
type RingBuffer struct {
	values []*IndexedValue
	head   int // the index of the front of the deque.
	tail   int // the index of the first position right after the end of the deque.

	nonEmpty bool // indicates whether the deque is empty, necessary to distinguish
	// between an empty deque and a deque that uses all of its capacity.
}

// Len returns number of IndexedValues in the deque.
func (r *RingBuffer) Len() int {
	if !r.nonEmpty {
		return 0
	}
	if r.head < r.tail {
		return r.tail - r.head
	} else if r.head == r.tail {
		return cap(r.values)
	} else {
		return cap(r.values) + r.tail - r.head
	}
}

// Add adds value to the end of the deque
// and doubles it's underlying slice if necessary.
func (r *RingBuffer) Add(value *IndexedValue) {
	if cap(r.values) == 0 {
		r.values = make([]*IndexedValue, RingBufferInitialSize)
		r.values[0] = value
		r.tail = 1
	} else {
		if r.Len() == cap(r.values) {
			newValues := make([]*IndexedValue, 2*cap(r.values))
			if r.head < r.tail {
				copy(newValues[:r.Len()], r.values[r.head:r.tail])
			} else {
				copy(newValues[:cap(r.values)-r.head], r.values[r.head:])
				copy(newValues[cap(r.values)-r.head:r.Len()], r.values[:r.tail])
			}
			r.head = 0
			r.tail = cap(r.values)
			r.values = newValues
		}
		r.values[r.tail] = value
		r.tail = (r.tail + 1) % cap(r.values)
	}
	r.nonEmpty = true
}

// Get returns IndexedValue at position pos in the deque (zero-based).
func (r *RingBuffer) Get(pos int) *IndexedValue {
	if !r.nonEmpty || pos < 0 || pos >= r.Len() {
		panic("unexpected behavior: index out of bounds")
	}
	return r.values[(pos+r.head)%cap(r.values)]
}

// RemoveHead removes a single element from the front of the deque.
func (r *RingBuffer) RemoveHead() {
	if r.Len() == 0 {
		panic("removing head from empty ring buffer")
	}
	r.values[r.head] = nil
	r.head = (r.head + 1) % cap(r.values)
	if r.head == r.tail {
		r.nonEmpty = false
	}
}

// RemoveTail removes a single element from the end of the deque.
func (r *RingBuffer) RemoveTail() {
	if r.Len() == 0 {
		panic("removing tail from empty ring buffer")
	}
	lastPos := (cap(r.values) + r.tail - 1) % cap(r.values)
	r.values[lastPos] = nil
	r.tail = lastPos
	if r.tail == r.head {
		r.nonEmpty = false
	}
}

// reset makes RingBuffer treat its underlying memory if it were empty. This
// allows for reusing the same memory again without explicitly removing old
// values.
func (r *RingBuffer) reset() {
	r.head = 0
	r.tail = 0
	r.nonEmpty = false
}
