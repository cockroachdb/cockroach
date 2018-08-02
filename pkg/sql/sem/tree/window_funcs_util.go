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

import (
	"github.com/cockroachdb/cockroach/pkg/util"
)

// PeerGroupChecker can check if a pair of row indices within a partition are
// in the same peer group.
type PeerGroupChecker interface {
	InSameGroup(i, j int) bool
}

// PeerGroup contains information about a single peer group.
type PeerGroup struct {
	FirstPeerIdx int
	RowCount     int
}

// PeerGroupsIndicesHelper computes peer groups using the given
// PeerGroupChecker. In ROWS and RANGE modes, it processes one peer group at
// a time and stores information only about single peer group. In GROUPS mode,
// it's behavior depends on the frame bounds; in the worst case, it stores
// max(F, O) peer groups at the same time, where F is the maximum number of
// peer groups within the frame at any point and O is the maximum of two
// offsets if we have OFFSET_FOLLOWING type of bound (both F and O are
// upper-bounded by total number of peer groups).
type PeerGroupsIndicesHelper struct {
	groups               util.RingBuffer // queue of peer groups' starting indices
	peerGrouper          PeerGroupChecker
	headPeerGroupNum     int  // number of the peer group at the head of the queue
	allPeerGroupsSkipped bool // in GROUP mode, indicates whether all peer groups were skipped during Init
	allRowsProcessed     bool // indicates whether peer groups for all rows within partition have been already computed
	unboundedFollowing   int  // index of the first row after all rows of the partition
}

// Init computes all peer groups necessary to perform calculations of a window
// function over the first row of the partition.
func (p *PeerGroupsIndicesHelper) Init(wfr *WindowFrameRun, peerGrouper PeerGroupChecker) {
	// We first reset the helper to reuse the same one for all partitions when
	// computing a particular window function.
	p.groups.Reset()
	p.headPeerGroupNum = 0
	p.allPeerGroupsSkipped = false
	p.allRowsProcessed = false
	p.unboundedFollowing = wfr.unboundedFollowing()

	var peerGroup *PeerGroup
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
		peerGroup = &PeerGroup{FirstPeerIdx: 0, RowCount: 1}
		for peerGroup.FirstPeerIdx < wfr.PartitionSize() && p.groups.Len() < peerGroupOffset {
			p.groups.AddLast(peerGroup)
			for ; peerGroup.FirstPeerIdx+peerGroup.RowCount < wfr.PartitionSize(); peerGroup.RowCount++ {
				idx := peerGroup.FirstPeerIdx + peerGroup.RowCount
				if !p.peerGrouper.InSameGroup(idx, idx-1) {
					break
				}
			}
			peerGroup = &PeerGroup{FirstPeerIdx: peerGroup.FirstPeerIdx + peerGroup.RowCount, RowCount: 1}
		}

		if peerGroup.FirstPeerIdx == wfr.PartitionSize() {
			// Frame starts after all peer groups of the partition.
			p.allPeerGroupsSkipped = true
			return
		}

		startIdxOfFirstPeerGroupWithinFrame = peerGroup.FirstPeerIdx
	}

	// Compute the first peer group that is within the frame.
	peerGroup = &PeerGroup{FirstPeerIdx: startIdxOfFirstPeerGroupWithinFrame, RowCount: 1}
	p.groups.AddLast(peerGroup)
	for ; peerGroup.FirstPeerIdx+peerGroup.RowCount < wfr.PartitionSize(); peerGroup.RowCount++ {
		idx := peerGroup.FirstPeerIdx + peerGroup.RowCount
		if !p.peerGrouper.InSameGroup(idx, idx-1) {
			break
		}
	}
	if peerGroup.FirstPeerIdx+peerGroup.RowCount == wfr.PartitionSize() {
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
		peerGroup = &PeerGroup{FirstPeerIdx: peerGroup.FirstPeerIdx + peerGroup.RowCount, RowCount: 1}
		for peerGroup.FirstPeerIdx < wfr.PartitionSize() && p.groups.Len() <= peerGroupOffset {
			p.groups.AddLast(peerGroup)
			for ; peerGroup.FirstPeerIdx+peerGroup.RowCount < wfr.PartitionSize(); peerGroup.RowCount++ {
				idx := peerGroup.FirstPeerIdx + peerGroup.RowCount
				if !p.peerGrouper.InSameGroup(idx, idx-1) {
					break
				}
			}
			peerGroup = &PeerGroup{FirstPeerIdx: peerGroup.FirstPeerIdx + peerGroup.RowCount, RowCount: 1}
		}
		if peerGroup.FirstPeerIdx == wfr.PartitionSize() {
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
	lastPeerGroup := p.groups.GetLast().(*PeerGroup)
	nextPeerGroupStartIdx := lastPeerGroup.FirstPeerIdx + lastPeerGroup.RowCount

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
		p.groups.RemoveFirst()
		p.headPeerGroupNum++
	}

	if p.allRowsProcessed {
		// No more peer groups to process.
		return
	}

	// Compute the next peer group that is just entering the frame.
	peerGroup := &PeerGroup{FirstPeerIdx: nextPeerGroupStartIdx, RowCount: 1}
	p.groups.AddLast(peerGroup)
	for ; peerGroup.FirstPeerIdx+peerGroup.RowCount < wfr.PartitionSize(); peerGroup.RowCount++ {
		idx := peerGroup.FirstPeerIdx + peerGroup.RowCount
		if !p.peerGrouper.InSameGroup(idx, idx-1) {
			break
		}
	}
	if peerGroup.FirstPeerIdx+peerGroup.RowCount == wfr.PartitionSize() {
		p.allRowsProcessed = true
	}
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
	return p.groups.Get(posInBuffer).(*PeerGroup).FirstPeerIdx
}

// GetRowCount returns the number of rows within peer group of number
// peerGroupNum (counting from 0).
func (p *PeerGroupsIndicesHelper) GetRowCount(peerGroupNum int) int {
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
	return p.groups.Get(posInBuffer).(*PeerGroup).RowCount
}

// GetLastPeerGroupNum returns the number of the last peer group in the queue.
func (p *PeerGroupsIndicesHelper) GetLastPeerGroupNum() int {
	if p.groups.Len() == 0 {
		panic("GetLastPeerGroupNum on empty RingBuffer")
	}
	return p.headPeerGroupNum + p.groups.Len() - 1
}
