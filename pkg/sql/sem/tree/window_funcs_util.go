// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treewindow"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
)

// PeerGroupChecker can check if a pair of row indices within a partition are
// in the same peer group. It also returns an error if it occurs while checking
// the peer groups.
type PeerGroupChecker interface {
	InSameGroup(i, j int) (bool, error)
}

// peerGroup contains information about a single peer group.
type peerGroup struct {
	firstPeerIdx int
	rowCount     int
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
	groups               ring.Buffer // queue of peer groups
	peerGrouper          PeerGroupChecker
	headPeerGroupNum     int  // number of the peer group at the head of the queue
	allPeerGroupsSkipped bool // in GROUP mode, indicates whether all peer groups were skipped during Init
	allRowsProcessed     bool // indicates whether peer groups for all rows within partition have been already computed
	unboundedFollowing   int  // index of the first row after all rows of the partition
}

// Init computes all peer groups necessary to perform calculations of a window
// function over the first row of the partition. It returns any error if it
// occurs.
func (p *PeerGroupsIndicesHelper) Init(wfr *WindowFrameRun, peerGrouper PeerGroupChecker) error {
	// We first reset the helper to reuse the same one for all partitions when
	// computing a particular window function.
	p.groups.Reset()
	p.headPeerGroupNum = 0
	p.allPeerGroupsSkipped = false
	p.allRowsProcessed = false
	p.unboundedFollowing = wfr.unboundedFollowing()

	var group *peerGroup
	p.peerGrouper = peerGrouper
	startIdxOfFirstPeerGroupWithinFrame := 0
	if wfr.Frame != nil && wfr.Frame.Mode == treewindow.GROUPS && wfr.Frame.Bounds.StartBound.BoundType == treewindow.OffsetFollowing {
		// In GROUPS mode with OFFSET_FOLLOWING as a start bound, 'peerGroupOffset'
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
		group = &peerGroup{firstPeerIdx: 0, rowCount: 1}
		for group.firstPeerIdx < wfr.PartitionSize() && p.groups.Len() < peerGroupOffset {
			p.groups.AddLast(group)
			for ; group.firstPeerIdx+group.rowCount < wfr.PartitionSize(); group.rowCount++ {
				idx := group.firstPeerIdx + group.rowCount
				if sameGroup, err := p.peerGrouper.InSameGroup(idx-1, idx); err != nil {
					return err
				} else if !sameGroup {
					break
				}
			}
			group = &peerGroup{firstPeerIdx: group.firstPeerIdx + group.rowCount, rowCount: 1}
		}

		if group.firstPeerIdx == wfr.PartitionSize() {
			// Frame starts after all peer groups of the partition.
			p.allPeerGroupsSkipped = true
			return nil
		}

		startIdxOfFirstPeerGroupWithinFrame = group.firstPeerIdx
	}

	// Compute the first peer group that is within the frame.
	group = &peerGroup{firstPeerIdx: startIdxOfFirstPeerGroupWithinFrame, rowCount: 1}
	p.groups.AddLast(group)
	for ; group.firstPeerIdx+group.rowCount < wfr.PartitionSize(); group.rowCount++ {
		idx := group.firstPeerIdx + group.rowCount
		if sameGroup, err := p.peerGrouper.InSameGroup(idx-1, idx); err != nil {
			return err
		} else if !sameGroup {
			break
		}
	}
	if group.firstPeerIdx+group.rowCount == wfr.PartitionSize() {
		p.allRowsProcessed = true
		return nil
	}

	if wfr.Frame != nil && wfr.Frame.Mode == treewindow.GROUPS && wfr.Frame.Bounds.EndBound != nil && wfr.Frame.Bounds.EndBound.BoundType == treewindow.OffsetFollowing {
		// In GROUPS mode, 'peerGroupOffset' number of peer groups need to be
		// processed upfront because they are within the frame of the first row.
		// If end bound is of type:
		// - UNBOUNDED_PRECEDING - invalid as an end bound
		// - OFFSET_PRECEDING - no need to process any peer groups upfront
		// - CURRENT_ROW - no need to process any more peer groups upfront
		// - OFFSET_FOLLOWING - processing is done here
		// - UNBOUNDED_FOLLOWING - we don't use this helper at all
		peerGroupOffset := int(MustBeDInt(wfr.EndBoundOffset))
		group = &peerGroup{firstPeerIdx: group.firstPeerIdx + group.rowCount, rowCount: 1}
		for group.firstPeerIdx < wfr.PartitionSize() && p.groups.Len() <= peerGroupOffset {
			p.groups.AddLast(group)
			for ; group.firstPeerIdx+group.rowCount < wfr.PartitionSize(); group.rowCount++ {
				idx := group.firstPeerIdx + group.rowCount
				if sameGroup, err := p.peerGrouper.InSameGroup(idx-1, idx); err != nil {
					return err
				} else if !sameGroup {
					break
				}
			}
			group = &peerGroup{firstPeerIdx: group.firstPeerIdx + group.rowCount, rowCount: 1}
		}
		if group.firstPeerIdx == wfr.PartitionSize() {
			p.allRowsProcessed = true
		}
	}
	return nil
}

// Update should be called after a window function has been computed over all
// rows in wfr.CurRowPeerGroupNum peer group. If not all rows have been already
// processed, it computes the next peer group. It returns any error if it
// occurs.
func (p *PeerGroupsIndicesHelper) Update(wfr *WindowFrameRun) error {
	if p.allPeerGroupsSkipped {
		// No peer groups to process.
		return nil
	}

	// nextPeerGroupStartIdx is the index of the first row that we haven't
	// computed peer group for.
	lastPeerGroup := p.groups.GetLast().(*peerGroup)
	nextPeerGroupStartIdx := lastPeerGroup.firstPeerIdx + lastPeerGroup.rowCount

	if (wfr.Frame == nil || wfr.Frame.Mode == treewindow.ROWS || wfr.Frame.Mode == treewindow.RANGE) ||
		(wfr.Frame.Bounds.StartBound.BoundType == treewindow.OffsetPreceding && wfr.CurRowPeerGroupNum-p.headPeerGroupNum > int(MustBeDInt(wfr.StartBoundOffset)) ||
			wfr.Frame.Bounds.StartBound.BoundType == treewindow.CurrentRow ||
			(wfr.Frame.Bounds.StartBound.BoundType == treewindow.OffsetFollowing && p.headPeerGroupNum-wfr.CurRowPeerGroupNum > int(MustBeDInt(wfr.StartBoundOffset)))) {
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
		return nil
	}

	// Compute the next peer group that is just entering the frame.
	peerGroup := &peerGroup{firstPeerIdx: nextPeerGroupStartIdx, rowCount: 1}
	p.groups.AddLast(peerGroup)
	for ; peerGroup.firstPeerIdx+peerGroup.rowCount < wfr.PartitionSize(); peerGroup.rowCount++ {
		idx := peerGroup.firstPeerIdx + peerGroup.rowCount
		if sameGroup, err := p.peerGrouper.InSameGroup(idx-1, idx); err != nil {
			return err
		} else if !sameGroup {
			break
		}
	}
	if peerGroup.firstPeerIdx+peerGroup.rowCount == wfr.PartitionSize() {
		p.allRowsProcessed = true
	}
	return nil
}

// GetFirstPeerIdx returns index of the first peer within peer group of number
// peerGroupNum (counting from 0).
func (p *PeerGroupsIndicesHelper) GetFirstPeerIdx(peerGroupNum int) int {
	posInBuffer := peerGroupNum - p.headPeerGroupNum
	if posInBuffer < 0 || p.groups.Len() < posInBuffer {
		panic("peerGroupNum out of bounds")
	}
	return p.groups.Get(posInBuffer).(*peerGroup).firstPeerIdx
}

// GetRowCount returns the number of rows within peer group of number
// peerGroupNum (counting from 0).
func (p *PeerGroupsIndicesHelper) GetRowCount(peerGroupNum int) int {
	posInBuffer := peerGroupNum - p.headPeerGroupNum
	if posInBuffer < 0 || p.groups.Len() < posInBuffer {
		panic("peerGroupNum out of bounds")
	}
	return p.groups.Get(posInBuffer).(*peerGroup).rowCount
}

// GetLastPeerGroupNum returns the number of the last peer group in the queue.
func (p *PeerGroupsIndicesHelper) GetLastPeerGroupNum() int {
	if p.groups.Len() == 0 {
		panic("GetLastPeerGroupNum on empty RingBuffer")
	}
	return p.headPeerGroupNum + p.groups.Len() - 1
}
