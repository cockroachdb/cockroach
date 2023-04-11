// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build execgen_template
// +build execgen_template

package colexecjoin

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexechash"
)

// execgen:template<useSel>
func collectProbeOuter(
	hj *hashJoiner, batchSize int, nResults int, batch coldata.Batch, sel []int, useSel bool,
) int {
	// Early bounds checks.
	// Capture the slices in order for BCE to occur.
	HeadIDs := hj.ht.ProbeScratch.HeadID
	startIdx := hj.probeState.prevBatchResumeIdx
	_ = HeadIDs[startIdx]
	_ = HeadIDs[batchSize-1]
	if useSel {
		_ = sel[batchSize-1]
	}
	maxResults := len(hj.probeState.BuildIdx)
	buildIdx := hj.probeState.BuildIdx
	probeIdx := hj.probeState.ProbeIdx
	_ = buildIdx[nResults]
	_ = probeIdx[nResults]
	_ = buildIdx[maxResults-1]
	_ = probeIdx[maxResults-1]
	for i := startIdx; i < batchSize; i++ {
		//gcassert:bce
		currentID := HeadIDs[i]

		for ; nResults < maxResults; nResults++ {
			rowUnmatched := currentID == 0
			// For some reason, BCE doesn't occur for ProbeRowUnmatched slice.
			// TODO(yuzefovich): figure it out.
			hj.probeState.ProbeRowUnmatched[nResults] = rowUnmatched
			if rowUnmatched {
				// The row is unmatched, and we set the corresponding BuildIdx
				// to zero so that (as long as the build hash table has at least
				// one row) we can copy the values vector without paying
				// attention to ProbeRowUnmatched.
				//gcassert:bce
				buildIdx[nResults] = 0
			} else {
				//gcassert:bce
				buildIdx[nResults] = int(currentID - 1)
			}
			pIdx := getIdx(i, sel, useSel)
			//gcassert:bce
			probeIdx[nResults] = pIdx
			currentID = hj.ht.Same[currentID]
			//gcassert:bce
			HeadIDs[i] = currentID

			if currentID == 0 {
				nResults++
				break
			}
		}

		if nResults == maxResults {
			// We have collected the maximum number of results that fit into the
			// current output batch.
			if currentID != 0 {
				// We haven't finished probing the ith tuple of the current
				// probing batch, so we'll need to resume from the same state.
				hj.probeState.prevBatch = batch
				hj.probeState.prevBatchResumeIdx = i
			} else {
				// We're done probing the ith tuple.
				if i+1 < batchSize {
					// But we're not done probing the batch yet.
					hj.probeState.prevBatch = batch
					hj.probeState.prevBatchResumeIdx = i + 1
				}
			}
			return nResults
		}
	}
	return nResults
}

// execgen:template<useSel>
func collectProbeNoOuter(
	hj *hashJoiner, batchSize int, nResults int, batch coldata.Batch, sel []int, useSel bool,
) int {
	// Early bounds checks.
	// Capture the slices in order for BCE to occur.
	HeadIDs := hj.ht.ProbeScratch.HeadID
	startIdx := hj.probeState.prevBatchResumeIdx
	_ = HeadIDs[startIdx]
	_ = HeadIDs[batchSize-1]
	if useSel {
		_ = sel[batchSize-1]
	}
	maxResults := len(hj.probeState.BuildIdx)
	probeIdx := hj.probeState.ProbeIdx
	_ = probeIdx[nResults]
	_ = probeIdx[maxResults-1]
	for i := startIdx; i < batchSize; i++ {
		//gcassert:bce
		currentID := HeadIDs[i]
		for ; currentID != 0 && nResults < maxResults; nResults++ {
			// For some reason, BCE doesn't occur for BuildIdx slice.
			// TODO(yuzefovich): figure it out.
			hj.probeState.BuildIdx[nResults] = int(currentID - 1)
			pIdx := getIdx(i, sel, useSel)
			//gcassert:bce
			probeIdx[nResults] = pIdx
			currentID = hj.ht.Same[currentID]
			//gcassert:bce
			HeadIDs[i] = currentID
		}

		if nResults == maxResults {
			// We have collected the maximum number of results that fit into the
			// current output batch.
			if currentID != 0 {
				// We haven't finished probing the ith tuple of the current
				// probing batch, so we'll need to resume from the same state.
				hj.probeState.prevBatch = batch
				hj.probeState.prevBatchResumeIdx = i
			} else {
				// We're done probing the ith tuple.
				if i+1 < batchSize {
					// But we're not done probing the batch yet.
					hj.probeState.prevBatch = batch
					hj.probeState.prevBatchResumeIdx = i + 1
				}
			}
			return nResults
		}
	}
	return nResults
}

// This code snippet collects the "matches" for LEFT ANTI, EXCEPT ALL, and
// INTERSECT ALL joins. "Matches" are in quotes because we're actually
// interested in non-matches from the left side if matchZeroCurrentID is true.
// execgen:template<matchZeroCurrentID,useSel>
func collectSingleMatch(
	hj *hashJoiner,
	batchSize int,
	nResults int,
	batch coldata.Batch,
	sel []int,
	matchZeroCurrentID bool,
	useSel bool,
) int {
	// Early bounds checks.
	// Capture the slice in order for BCE to occur.
	HeadIDs := hj.ht.ProbeScratch.HeadID
	_ = HeadIDs[batchSize-1]
	if useSel {
		_ = sel[batchSize-1]
	}
	for i := 0; i < batchSize; i++ {
		//gcassert:bce
		currentID := HeadIDs[i]
		if matchZeroCurrentID {
			if currentID == 0 {
				// {{/*
				//     currentID of 0 indicates that ith probing row didn't have
				//     a match, so we include it into the output.
				// */}}
				hj.probeState.ProbeIdx[nResults] = getIdx(i, sel, useSel)
				nResults++
			}
		} else {
			if currentID != 0 {
				// {{/*
				//     Non-zero currentID indicates that ith probing row had a
				//     match AND that match wasn't previously "visited", so we
				//     include it into the output.
				// */}}
				hj.probeState.ProbeIdx[nResults] = getIdx(i, sel, useSel)
				nResults++
			}
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
	// Capture the slice in order for BCE to occur.
	HeadIDs := hj.ht.ProbeScratch.HeadID
	_ = HeadIDs[batchSize-1]
	for i := 0; i < batchSize; i++ {
		//gcassert:bce
		currentID := HeadIDs[i]
		for currentID != 0 {
			hj.probeState.buildRowMatched[currentID-1] = true
			currentID = hj.ht.Same[currentID]
		}
	}
}

// collect prepares the BuildIdx and ProbeIdx arrays where the BuildIdx and
// ProbeIdx at each index are joined to make an output row. The total number of
// resulting rows is returned.
func (hj *hashJoiner) collect(
	_ *colexechash.JoinProbeState, batch coldata.Batch, batchSize int, sel []int,
) int {
	nResults := 0

	if !hj.spec.JoinType.ShouldIncludeLeftColsInOutput() {
		collectRightSemiAnti(hj, batchSize)
		return 0
	}

	if hj.spec.JoinType.IsLeftOuterOrFullOuter() {
		if sel != nil {
			nResults = collectProbeOuter(hj, batchSize, nResults, batch, sel, true)
		} else {
			nResults = collectProbeOuter(hj, batchSize, nResults, batch, sel, false)
		}
	} else {
		if sel != nil {
			if hj.spec.JoinType.IsLeftAntiOrExceptAll() {
				nResults = collectSingleMatch(hj, batchSize, nResults, batch, sel, true, true)
			} else if hj.spec.JoinType == descpb.IntersectAllJoin {
				nResults = collectSingleMatch(hj, batchSize, nResults, batch, sel, false, true)
			} else {
				nResults = collectProbeNoOuter(hj, batchSize, nResults, batch, sel, true)
			}
		} else {
			if hj.spec.JoinType.IsLeftAntiOrExceptAll() {
				nResults = collectSingleMatch(hj, batchSize, nResults, batch, sel, true, false)
			} else if hj.spec.JoinType == descpb.IntersectAllJoin {
				nResults = collectSingleMatch(hj, batchSize, nResults, batch, sel, false, false)
			} else {
				nResults = collectProbeNoOuter(hj, batchSize, nResults, batch, sel, false)
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
