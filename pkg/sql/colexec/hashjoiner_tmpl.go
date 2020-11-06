// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build execgen_template

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// execgen:template<useSel>
func collectProbeOuter(
	hj *hashJoiner, batchSize int, nResults int, batch coldata.Batch, sel []int, useSel bool,
) int {
	// Early bounds checks.
	_ = hj.ht.probeScratch.headID[batchSize-1]
	if useSel {
		_ = sel[batchSize-1]
	}
	for i := hj.probeState.prevBatchResumeIdx; i < batchSize; i++ {
		currentID := hj.ht.probeScratch.headID[i]

		for {
			if nResults >= coldata.BatchSize() {
				hj.probeState.prevBatch = batch
				hj.probeState.prevBatchResumeIdx = i
				return nResults
			}

			hj.probeState.probeRowUnmatched[nResults] = currentID == 0
			if currentID > 0 {
				hj.probeState.buildIdx[nResults] = int(currentID - 1)
			} else {
				// If currentID == 0, then probeRowUnmatched will have been set - and
				// we set the corresponding buildIdx to zero so that (as long as the
				// build hash table has at least one row) we can copy the values vector
				// without paying attention to probeRowUnmatched.
				hj.probeState.buildIdx[nResults] = 0
			}
			hj.probeState.probeIdx[nResults] = getIdx(i, sel, useSel)
			currentID = hj.ht.same[currentID]
			hj.ht.probeScratch.headID[i] = currentID
			nResults++

			if currentID == 0 {
				break
			}
		}
	}
	return nResults
}

// execgen:template<useSel>
func collectProbeNoOuter(
	hj *hashJoiner, batchSize int, nResults int, batch coldata.Batch, sel []int, useSel bool,
) int {
	// Early bounds checks.
	_ = hj.ht.probeScratch.headID[batchSize-1]
	if useSel {
		_ = sel[batchSize-1]
	}
	for i := hj.probeState.prevBatchResumeIdx; i < batchSize; i++ {
		currentID := hj.ht.probeScratch.headID[i]
		for currentID != 0 {
			if nResults >= coldata.BatchSize() {
				hj.probeState.prevBatch = batch
				hj.probeState.prevBatchResumeIdx = i
				return nResults
			}

			hj.probeState.buildIdx[nResults] = int(currentID - 1)
			hj.probeState.probeIdx[nResults] = getIdx(i, sel, useSel)
			currentID = hj.ht.same[currentID]
			hj.ht.probeScratch.headID[i] = currentID
			nResults++
		}
	}
	return nResults
}

// This code snippet collects the "matches" for LEFT ANTI and EXCEPT ALL joins.
// "Matches" are in quotes because we're actually interested in non-matches
// from the left side.
// execgen:template<useSel>
func collectLeftAnti(
	hj *hashJoiner, batchSize int, nResults int, batch coldata.Batch, sel []int, useSel bool,
) int {
	// Early bounds checks.
	_ = hj.ht.probeScratch.headID[batchSize-1]
	if useSel {
		_ = sel[batchSize-1]
	}
	for i := int(0); i < batchSize; i++ {
		currentID := hj.ht.probeScratch.headID[i]
		if currentID == 0 {
			// currentID of 0 indicates that ith probing row didn't have a match, so
			// we include it into the output.
			hj.probeState.probeIdx[nResults] = getIdx(i, sel, useSel)
			nResults++
		}
	}
	return nResults
}

// collectRightSemiAnti processes all matches for right semi/anti joins. Note
// that during the probing phase we do not emit any output for these joins and
// are simply tracking whether build rows had a match. The output will be
// populated when in hjEmittingRight state.
func collectRightSemiAnti(hj *hashJoiner, batchSize int) {
	// Early bounds checks.
	_ = hj.ht.probeScratch.headID[batchSize-1]
	for i := int(0); i < batchSize; i++ {
		currentID := hj.ht.probeScratch.headID[i]
		for currentID != 0 {
			hj.probeState.buildRowMatched[currentID-1] = true
			currentID = hj.ht.same[currentID]
		}
	}
}

// execgen:template<useSel>
func distinctCollectProbeOuter(hj *hashJoiner, batchSize int, sel []int, useSel bool) {
	// Early bounds checks.
	_ = hj.ht.probeScratch.groupID[batchSize-1]
	_ = hj.probeState.probeRowUnmatched[batchSize-1]
	_ = hj.probeState.buildIdx[batchSize-1]
	_ = hj.probeState.probeIdx[batchSize-1]
	if useSel {
		_ = sel[batchSize-1]
	}
	for i := int(0); i < batchSize; i++ {
		// Index of keys and outputs in the hash table is calculated as ID - 1.
		id := hj.ht.probeScratch.groupID[i]
		rowUnmatched := id == 0
		hj.probeState.probeRowUnmatched[i] = rowUnmatched
		if !rowUnmatched {
			hj.probeState.buildIdx[i] = int(id - 1)
		}
		hj.probeState.probeIdx[i] = getIdx(i, sel, useSel)
	}
}

// execgen:template<useSel>
func distinctCollectProbeNoOuter(
	hj *hashJoiner, batchSize int, nResults int, sel []int, useSel bool,
) int {
	// Early bounds checks.
	_ = hj.ht.probeScratch.groupID[batchSize-1]
	_ = hj.probeState.buildIdx[batchSize-1]
	_ = hj.probeState.probeIdx[batchSize-1]
	if useSel {
		_ = sel[batchSize-1]
	}
	for i := int(0); i < batchSize; i++ {
		if hj.ht.probeScratch.groupID[i] != 0 {
			// Index of keys and outputs in the hash table is calculated as ID - 1.
			hj.probeState.buildIdx[nResults] = int(hj.ht.probeScratch.groupID[i] - 1)
			hj.probeState.probeIdx[nResults] = getIdx(i, sel, useSel)
			nResults++
		}
	}
	return nResults
}

// collect prepares the buildIdx and probeIdx arrays where the buildIdx and
// probeIdx at each index are joined to make an output row. The total number of
// resulting rows is returned.
func (hj *hashJoiner) collect(batch coldata.Batch, batchSize int, sel []int) int {
	nResults := int(0)

	if hj.spec.joinType == descpb.RightSemiJoin || hj.spec.joinType == descpb.RightAntiJoin {
		collectRightSemiAnti(hj, batchSize)
		return 0
	}

	if hj.spec.left.outer {
		if sel != nil {
			nResults = collectProbeOuter(hj, batchSize, nResults, batch, sel, true)
		} else {
			nResults = collectProbeOuter(hj, batchSize, nResults, batch, sel, false)
		}
	} else {
		if sel != nil {
			switch hj.spec.joinType {
			case descpb.LeftAntiJoin, descpb.ExceptAllJoin:
				nResults = collectLeftAnti(hj, batchSize, nResults, batch, sel, true)
			default:
				nResults = collectProbeNoOuter(hj, batchSize, nResults, batch, sel, true)
			}
		} else {
			switch hj.spec.joinType {
			case descpb.LeftAntiJoin, descpb.ExceptAllJoin:
				nResults = collectLeftAnti(hj, batchSize, nResults, batch, sel, false)
			default:
				nResults = collectProbeNoOuter(hj, batchSize, nResults, batch, sel, false)
			}
		}
	}

	return nResults
}

// distinctCollect prepares the batch with the joined output columns where the build
// row index for each probe row is given in the groupID slice. This function
// requires assumes a N-1 hash join.
func (hj *hashJoiner) distinctCollect(batch coldata.Batch, batchSize int, sel []int) int {
	nResults := int(0)

	if hj.spec.joinType == descpb.RightSemiJoin || hj.spec.joinType == descpb.RightAntiJoin {
		collectRightSemiAnti(hj, batchSize)
		return 0
	}

	if hj.spec.left.outer {
		nResults = batchSize

		if sel != nil {
			distinctCollectProbeOuter(hj, batchSize, sel, true)
		} else {
			distinctCollectProbeOuter(hj, batchSize, sel, false)
		}
	} else {
		if sel != nil {
			switch hj.spec.joinType {
			case descpb.LeftAntiJoin, descpb.ExceptAllJoin:
				// For LEFT ANTI and EXCEPT ALL joins we don't care whether the build
				// (right) side was distinct, so we only have single variation of COLLECT
				// method.
				nResults = collectLeftAnti(hj, batchSize, nResults, batch, sel, true)
			default:
				nResults = distinctCollectProbeNoOuter(hj, batchSize, nResults, sel, true)
			}
		} else {
			switch hj.spec.joinType {
			case descpb.LeftAntiJoin, descpb.ExceptAllJoin:
				// For LEFT ANTI and EXCEPT ALL joins we don't care whether the build
				// (right) side was distinct, so we only have single variation of COLLECT
				// method.
				nResults = collectLeftAnti(hj, batchSize, nResults, batch, sel, false)
			default:
				nResults = distinctCollectProbeNoOuter(hj, batchSize, nResults, sel, false)
			}
		}
	}

	return nResults
}

// execgen:template<useSel>
// execgen:inline
func getIdx(i int, sel []int, useSel bool) int {
	if useSel {
		return sel[i]
	} else {
		return i
	}
}
