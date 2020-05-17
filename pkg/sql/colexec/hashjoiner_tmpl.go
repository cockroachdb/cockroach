// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
// +build execgen_template
//
// This file is the execgen template for hashjoiner.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// {{/*

func _COLLECT_PROBE_OUTER(
	hj *hashJoiner, batchSize int, nResults int, batch coldata.Batch, _USE_SEL bool,
) int { // */}}
	// {{define "collectProbeOuter" -}}
	// Early bounds checks.
	_ = hj.ht.probeScratch.headID[batchSize-1]
	// {{if .UseSel}}
	_ = sel[batchSize-1]
	// {{end}}
	for i := hj.probeState.prevBatchResumeIdx; i < batchSize; i++ {
		currentID := hj.ht.probeScratch.headID[i]

		for {
			if nResults >= hj.outputBatchSize {
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
			// {{if .UseSel}}
			hj.probeState.probeIdx[nResults] = sel[i]
			// {{else}}
			hj.probeState.probeIdx[nResults] = i
			// {{end}}
			currentID = hj.ht.same[currentID]
			hj.ht.probeScratch.headID[i] = currentID
			nResults++

			if currentID == 0 {
				break
			}
		}
	}
	// {{end}}
	// {{/*
	// Dummy return value that is never used.
	return 0
}

func _COLLECT_PROBE_NO_OUTER(
	hj *hashJoiner, batchSize int, nResults int, batch coldata.Batch, _USE_SEL bool,
) int { // */}}
	// {{define "collectProbeNoOuter" -}}
	// Early bounds checks.
	_ = hj.ht.probeScratch.headID[batchSize-1]
	// {{if .UseSel}}
	_ = sel[batchSize-1]
	// {{end}}
	for i := hj.probeState.prevBatchResumeIdx; i < batchSize; i++ {
		currentID := hj.ht.probeScratch.headID[i]
		for currentID != 0 {
			if nResults >= hj.outputBatchSize {
				hj.probeState.prevBatch = batch
				hj.probeState.prevBatchResumeIdx = i
				return nResults
			}

			hj.probeState.buildIdx[nResults] = int(currentID - 1)
			// {{if .UseSel}}
			hj.probeState.probeIdx[nResults] = sel[i]
			// {{else}}
			hj.probeState.probeIdx[nResults] = i
			// {{end}}
			currentID = hj.ht.same[currentID]
			hj.ht.probeScratch.headID[i] = currentID
			nResults++
		}
	}
	// {{end}}
	// {{/*
	// Dummy return value that is never used.
	return 0
}

// This code snippet collects the "matches" for LEFT ANTI and EXCEPT ALL joins.
// "Matches" are in quotes because we're actually interested in non-matches
// from the left side.
func _COLLECT_ANTI(
	hj *hashJoiner, batchSize int, nResults int, batch coldata.Batch, _USE_SEL bool,
) int { // */}}
	// {{define "collectAnti" -}}
	// Early bounds checks.
	_ = hj.ht.probeScratch.headID[batchSize-1]
	// {{if .UseSel}}
	_ = sel[batchSize-1]
	// {{end}}
	for i := int(0); i < batchSize; i++ {
		currentID := hj.ht.probeScratch.headID[i]
		if currentID == 0 {
			// currentID of 0 indicates that ith probing row didn't have a match, so
			// we include it into the output.
			// {{if .UseSel}}
			hj.probeState.probeIdx[nResults] = sel[i]
			// {{else}}
			hj.probeState.probeIdx[nResults] = i
			// {{end}}
			nResults++
		}
	}
	// {{end}}
	// {{/*
	// Dummy return value that is never used.
	return 0
}

func _DISTINCT_COLLECT_PROBE_OUTER(hj *hashJoiner, batchSize int, _USE_SEL bool) { // */}}
	// {{define "distinctCollectProbeOuter" -}}
	// Early bounds checks.
	_ = hj.ht.probeScratch.groupID[batchSize-1]
	_ = hj.probeState.probeRowUnmatched[batchSize-1]
	_ = hj.probeState.buildIdx[batchSize-1]
	_ = hj.probeState.probeIdx[batchSize-1]
	// {{if .UseSel}}
	_ = sel[batchSize-1]
	// {{end}}
	for i := int(0); i < batchSize; i++ {
		// Index of keys and outputs in the hash table is calculated as ID - 1.
		id := hj.ht.probeScratch.groupID[i]
		rowUnmatched := id == 0
		hj.probeState.probeRowUnmatched[i] = rowUnmatched
		if !rowUnmatched {
			hj.probeState.buildIdx[i] = int(id - 1)
		}
		// {{if .UseSel}}
		hj.probeState.probeIdx[i] = sel[i]
		// {{else}}
		hj.probeState.probeIdx[i] = i
		// {{end}}
	}
	// {{end}}
	// {{/*
}

func _DISTINCT_COLLECT_PROBE_NO_OUTER(hj *hashJoiner, batchSize int, nResults int, _USE_SEL bool) { // */}}
	// {{define "distinctCollectProbeNoOuter" -}}
	// Early bounds checks.
	_ = hj.ht.probeScratch.groupID[batchSize-1]
	_ = hj.probeState.buildIdx[batchSize-1]
	_ = hj.probeState.probeIdx[batchSize-1]
	// {{if .UseSel}}
	_ = sel[batchSize-1]
	// {{end}}
	for i := int(0); i < batchSize; i++ {
		if hj.ht.probeScratch.groupID[i] != 0 {
			// Index of keys and outputs in the hash table is calculated as ID - 1.
			hj.probeState.buildIdx[nResults] = int(hj.ht.probeScratch.groupID[i] - 1)
			// {{if .UseSel}}
			hj.probeState.probeIdx[nResults] = sel[i]
			// {{else}}
			hj.probeState.probeIdx[nResults] = i
			// {{end}}
			nResults++
		}
	}
	// {{end}}
	// {{/*
}

// */}}

// collect prepares the buildIdx and probeIdx arrays where the buildIdx and
// probeIdx at each index are joined to make an output row. The total number of
// resulting rows is returned.
func (hj *hashJoiner) collect(batch coldata.Batch, batchSize int, sel []int) int {
	nResults := int(0)

	if hj.spec.left.outer {
		if sel != nil {
			_COLLECT_PROBE_OUTER(hj, batchSize, nResults, batch, true)
		} else {
			_COLLECT_PROBE_OUTER(hj, batchSize, nResults, batch, false)
		}
	} else {
		if sel != nil {
			switch hj.spec.joinType {
			case sqlbase.LeftAntiJoin, sqlbase.ExceptAllJoin:
				_COLLECT_ANTI(hj, batchSize, nResults, batch, true)
			default:
				_COLLECT_PROBE_NO_OUTER(hj, batchSize, nResults, batch, true)
			}
		} else {
			switch hj.spec.joinType {
			case sqlbase.LeftAntiJoin, sqlbase.ExceptAllJoin:
				_COLLECT_ANTI(hj, batchSize, nResults, batch, false)
			default:
				_COLLECT_PROBE_NO_OUTER(hj, batchSize, nResults, batch, false)
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

	if hj.spec.left.outer {
		nResults = batchSize

		if sel != nil {
			_DISTINCT_COLLECT_PROBE_OUTER(hj, batchSize, true)
		} else {
			_DISTINCT_COLLECT_PROBE_OUTER(hj, batchSize, false)
		}
	} else {
		if sel != nil {
			switch hj.spec.joinType {
			case sqlbase.LeftAntiJoin, sqlbase.ExceptAllJoin:
				// {{/* For LEFT ANTI and EXCEPT ALL joins we don't care whether the build
				// (right) side was distinct, so we only have single variation of COLLECT
				// method. */}}
				_COLLECT_ANTI(hj, batchSize, nResults, batch, true)
			default:
				_DISTINCT_COLLECT_PROBE_NO_OUTER(hj, batchSize, nResults, true)
			}
		} else {
			switch hj.spec.joinType {
			case sqlbase.LeftAntiJoin, sqlbase.ExceptAllJoin:
				// {{/* For LEFT ANTI and EXCEPT ALL joins we don't care whether the build
				// (right) side was distinct, so we only have single variation of COLLECT
				// method. */}}
				_COLLECT_ANTI(hj, batchSize, nResults, batch, false)
			default:
				_DISTINCT_COLLECT_PROBE_NO_OUTER(hj, batchSize, nResults, false)
			}
		}
	}

	return nResults
}
