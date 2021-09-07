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

import "github.com/cockroachdb/cockroach/pkg/col/coldata"

// execgen:template<useSel>
func collectProbeOuter(
	hj *HashJoiner, batchSize int, nResults int, batch coldata.Batch, sel []int, useSel bool,
) int {
	// Early bounds checks.
	// Capture the slices in order for BCE to occur.
	HeadIDs := hj.Ht.ProbeScratch.HeadID
	startIdx := hj.ProbeState.prevBatchResumeIdx
	_ = HeadIDs[startIdx]
	_ = HeadIDs[batchSize-1]
	if useSel {
		_ = sel[batchSize-1]
	}
	maxResults := len(hj.ProbeState.BuildIdx)
	buildIdx := hj.ProbeState.BuildIdx
	probeIdx := hj.ProbeState.ProbeIdx
	_ = buildIdx[nResults]
	_ = probeIdx[nResults]
	_ = buildIdx[maxResults-1]
	_ = probeIdx[maxResults-1]
	for i := startIdx; i < batchSize; i++ {
		//gcassert:bce
		currentID := HeadIDs[i]

		for ; nResults < maxResults; nResults++ {
			rowUnmatched := currentID == 0
			// For some reason, BCE doesn't occur for probeRowUnmatched slice.
			// TODO(yuzefovich): figure it out.
			hj.ProbeState.probeRowUnmatched[nResults] = rowUnmatched
			if rowUnmatched {
				// The row is unmatched, and we set the corresponding BuildIdx
				// to zero so that (as long as the build hash table has at least
				// one row) we can copy the values vector without paying
				// attention to probeRowUnmatched.
				//gcassert:bce
				buildIdx[nResults] = 0
			} else {
				//gcassert:bce
				buildIdx[nResults] = int(currentID - 1)
			}
			pIdx := getIdx(i, sel, useSel)
			//gcassert:bce
			probeIdx[nResults] = pIdx
			currentID = hj.Ht.Same[currentID]
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
				hj.ProbeState.prevBatch = batch
				hj.ProbeState.prevBatchResumeIdx = i
			} else {
				// We're done probing the ith tuple.
				if i+1 < batchSize {
					// But we're not done probing the batch yet.
					hj.ProbeState.prevBatch = batch
					hj.ProbeState.prevBatchResumeIdx = i + 1
				}
			}
			return nResults
		}
	}
	return nResults
}

// execgen:template<useSel>
func collectProbeNoOuter(
	hj *HashJoiner, batchSize int, nResults int, batch coldata.Batch, sel []int, useSel bool,
) int {
	// Early bounds checks.
	// Capture the slices in order for BCE to occur.
	HeadIDs := hj.Ht.ProbeScratch.HeadID
	startIdx := hj.ProbeState.prevBatchResumeIdx
	_ = HeadIDs[startIdx]
	_ = HeadIDs[batchSize-1]
	if useSel {
		_ = sel[batchSize-1]
	}
	maxResults := len(hj.ProbeState.BuildIdx)
	probeIdx := hj.ProbeState.ProbeIdx
	_ = probeIdx[nResults]
	_ = probeIdx[maxResults-1]
	for i := startIdx; i < batchSize; i++ {
		//gcassert:bce
		currentID := HeadIDs[i]
		for ; currentID != 0 && nResults < maxResults; nResults++ {
			// For some reason, BCE doesn't occur for BuildIdx slice.
			// TODO(yuzefovich): figure it out.
			hj.ProbeState.BuildIdx[nResults] = int(currentID - 1)
			pIdx := getIdx(i, sel, useSel)
			//gcassert:bce
			probeIdx[nResults] = pIdx
			currentID = hj.Ht.Same[currentID]
			//gcassert:bce
			HeadIDs[i] = currentID
		}

		if nResults == maxResults {
			// We have collected the maximum number of results that fit into the
			// current output batch.
			if currentID != 0 {
				// We haven't finished probing the ith tuple of the current
				// probing batch, so we'll need to resume from the same state.
				hj.ProbeState.prevBatch = batch
				hj.ProbeState.prevBatchResumeIdx = i
			} else {
				// We're done probing the ith tuple.
				if i+1 < batchSize {
					// But we're not done probing the batch yet.
					hj.ProbeState.prevBatch = batch
					hj.ProbeState.prevBatchResumeIdx = i + 1
				}
			}
			return nResults
		}
	}
	return nResults
}

// This code snippet collects the "matches" for LEFT ANTI and EXCEPT ALL joins.
// "Matches" are in quotes because we're actually interested in non-matches
// from the left side.
// execgen:template<useSel>
func collectLeftAnti(
	hj *HashJoiner, batchSize int, nResults int, batch coldata.Batch, sel []int, useSel bool,
) int {
	// Early bounds checks.
	// Capture the slice in order for BCE to occur.
	HeadIDs := hj.Ht.ProbeScratch.HeadID
	_ = HeadIDs[batchSize-1]
	if useSel {
		_ = sel[batchSize-1]
	}
	for i := 0; i < batchSize; i++ {
		//gcassert:bce
		currentID := HeadIDs[i]
		if currentID == 0 {
			// currentID of 0 indicates that ith probing row didn't have a match, so
			// we include it into the output.
			hj.ProbeState.ProbeIdx[nResults] = getIdx(i, sel, useSel)
			nResults++
		}
	}
	return nResults
}

// collectRightSemiAnti processes all matches for right semi/anti joins. Note
// that during the probing phase we do not emit any output for these joins and
// are simply tracking whether build rows had a match. The output will be
// populated when in hjEmittingRight state.
func collectRightSemiAnti(hj *HashJoiner, batchSize int) {
	// Early bounds checks.
	// Capture the slice in order for BCE to occur.
	HeadIDs := hj.Ht.ProbeScratch.HeadID
	_ = HeadIDs[batchSize-1]
	for i := 0; i < batchSize; i++ {
		//gcassert:bce
		currentID := HeadIDs[i]
		for currentID != 0 {
			hj.ProbeState.buildRowMatched[currentID-1] = true
			currentID = hj.Ht.Same[currentID]
		}
	}
}

// execgen:template<useSel>
func distinctCollectProbeOuter(hj *HashJoiner, batchSize int, sel []int, useSel bool) {
	// Early bounds checks.
	// Capture the slices in order for BCE to occur.
	groupIDs := hj.Ht.ProbeScratch.GroupID
	probeRowUnmatched := hj.ProbeState.probeRowUnmatched
	buildIdx := hj.ProbeState.BuildIdx
	probeIdx := hj.ProbeState.ProbeIdx
	_ = groupIDs[batchSize-1]
	_ = probeRowUnmatched[batchSize-1]
	_ = buildIdx[batchSize-1]
	_ = probeIdx[batchSize-1]
	if useSel {
		_ = sel[batchSize-1]
	}
	for i := 0; i < batchSize; i++ {
		// Index of keys and outputs in the hash table is calculated as ID - 1.
		//gcassert:bce
		id := groupIDs[i]
		rowUnmatched := id == 0
		//gcassert:bce
		probeRowUnmatched[i] = rowUnmatched
		if rowUnmatched {
			// The row is unmatched, and we set the corresponding BuildIdx
			// to zero so that (as long as the build hash table has at least
			// one row) we can copy the values vector without paying
			// attention to probeRowUnmatched.
			//gcassert:bce
			buildIdx[i] = 0
		} else {
			//gcassert:bce
			buildIdx[i] = int(id - 1)
		}
		pIdx := getIdx(i, sel, useSel)
		//gcassert:bce
		probeIdx[i] = pIdx
	}
}

// execgen:template<useSel>
func distinctCollectProbeNoOuter(
	hj *HashJoiner, batchSize int, nResults int, sel []int, useSel bool,
) int {
	// Early bounds checks.
	// Capture the slice in order for BCE to occur.
	groupIDs := hj.Ht.ProbeScratch.GroupID
	_ = groupIDs[batchSize-1]
	if useSel {
		_ = sel[batchSize-1]
	}
	for i := 0; i < batchSize; i++ {
		//gcassert:bce
		id := groupIDs[i]
		if id != 0 {
			// Index of keys and outputs in the hash table is calculated as ID - 1.
			hj.ProbeState.BuildIdx[nResults] = int(id - 1)
			hj.ProbeState.ProbeIdx[nResults] = getIdx(i, sel, useSel)
			nResults++
		}
	}
	return nResults
}

// collect prepares the BuildIdx and ProbeIdx arrays where the BuildIdx and
// ProbeIdx at each index are joined to make an output row. The total number of
// resulting rows is returned.
func (hj *HashJoiner) collect(batch coldata.Batch, batchSize int, sel []int) int {
	nResults := 0

	if hj.Spec.JoinType.IsRightSemiOrRightAnti() {
		collectRightSemiAnti(hj, batchSize)
		return 0
	}

	if hj.Spec.JoinType.IsLeftOuterOrFullOuter() {
		if sel != nil {
			nResults = collectProbeOuter(hj, batchSize, nResults, batch, sel, true)
		} else {
			nResults = collectProbeOuter(hj, batchSize, nResults, batch, sel, false)
		}
	} else {
		if sel != nil {
			if hj.Spec.JoinType.IsLeftAntiOrExceptAll() {
				nResults = collectLeftAnti(hj, batchSize, nResults, batch, sel, true)
			} else {
				nResults = collectProbeNoOuter(hj, batchSize, nResults, batch, sel, true)
			}
		} else {
			if hj.Spec.JoinType.IsLeftAntiOrExceptAll() {
				nResults = collectLeftAnti(hj, batchSize, nResults, batch, sel, false)
			} else {
				nResults = collectProbeNoOuter(hj, batchSize, nResults, batch, sel, false)
			}
		}
	}

	return nResults
}

// distinctCollect prepares the batch with the joined output columns where the build
// row index for each probe row is given in the GroupID slice. This function
// requires assumes a N-1 hash join.
func (hj *HashJoiner) distinctCollect(batch coldata.Batch, batchSize int, sel []int) int {
	nResults := 0

	if hj.Spec.JoinType.IsRightSemiOrRightAnti() {
		collectRightSemiAnti(hj, batchSize)
		return 0
	}

	if hj.Spec.JoinType.IsLeftOuterOrFullOuter() {
		nResults = batchSize

		if sel != nil {
			distinctCollectProbeOuter(hj, batchSize, sel, true)
		} else {
			distinctCollectProbeOuter(hj, batchSize, sel, false)
		}
	} else {
		if sel != nil {
			if hj.Spec.JoinType.IsLeftAntiOrExceptAll() {
				// For LEFT ANTI and EXCEPT ALL joins we don't care whether the build
				// (right) side was distinct, so we only have single variation of COLLECT
				// method.
				nResults = collectLeftAnti(hj, batchSize, nResults, batch, sel, true)
			} else {
				nResults = distinctCollectProbeNoOuter(hj, batchSize, nResults, sel, true)
			}
		} else {
			if hj.Spec.JoinType.IsLeftAntiOrExceptAll() {
				// For LEFT ANTI and EXCEPT ALL joins we don't care whether the build
				// (right) side was distinct, so we only have single variation of COLLECT
				// method.
				nResults = collectLeftAnti(hj, batchSize, nResults, batch, sel, false)
			} else {
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
