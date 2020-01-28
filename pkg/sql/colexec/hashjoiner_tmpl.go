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
	prober *hashJoinProber, batchSize uint16, nResults uint16, batch coldata.Batch, _USE_SEL bool,
) uint16 { // */}}
	// {{define "collectProbeOuter" -}}
	// Early bounds checks.
	_ = prober.ht.headID[batchSize-1]
	// {{if .UseSel}}
	_ = sel[batchSize-1]
	// {{end}}
	for i := prober.prevBatchResumeIdx; i < batchSize; i++ {
		currentID := prober.ht.headID[i]

		for {
			if nResults >= prober.outputBatchSize {
				prober.prevBatch = batch
				prober.prevBatchResumeIdx = i
				return nResults
			}

			prober.probeRowUnmatched[nResults] = currentID == 0
			if currentID > 0 {
				prober.buildIdx[nResults] = currentID - 1
			} else {
				// If currentID == 0, then probeRowUnmatched will have been set - and
				// we set the corresponding buildIdx to zero so that (as long as the
				// build hash table has at least one row) we can copy the values vector
				// without paying attention to probeRowUnmatched.
				prober.buildIdx[nResults] = 0
			}
			// {{if .UseSel}}
			prober.probeIdx[nResults] = sel[i]
			// {{else}}
			prober.probeIdx[nResults] = i
			// {{end}}
			currentID = prober.ht.same[currentID]
			prober.ht.headID[i] = currentID
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
	prober *hashJoinProber, batchSize uint16, nResults uint16, batch coldata.Batch, _USE_SEL bool,
) uint16 { // */}}
	// {{define "collectProbeNoOuter" -}}
	// Early bounds checks.
	_ = prober.ht.headID[batchSize-1]
	// {{if .UseSel}}
	_ = sel[batchSize-1]
	// {{end}}
	for i := prober.prevBatchResumeIdx; i < batchSize; i++ {
		currentID := prober.ht.headID[i]
		for currentID != 0 {
			if nResults >= prober.outputBatchSize {
				prober.prevBatch = batch
				prober.prevBatchResumeIdx = i
				return nResults
			}

			prober.buildIdx[nResults] = currentID - 1
			// {{if .UseSel}}
			prober.probeIdx[nResults] = sel[i]
			// {{else}}
			prober.probeIdx[nResults] = i
			// {{end}}
			currentID = prober.ht.same[currentID]
			prober.ht.headID[i] = currentID
			nResults++
		}
	}
	// {{end}}
	// {{/*
	// Dummy return value that is never used.
	return 0
}

func _COLLECT_LEFT_ANTI(
	prober *hashJoinProber, batchSize uint16, nResults uint16, batch coldata.Batch, _USE_SEL bool,
) uint16 { // */}}
	// {{define "collectLeftAnti" -}}
	// Early bounds checks.
	_ = prober.ht.headID[batchSize-1]
	// {{if .UseSel}}
	_ = sel[batchSize-1]
	// {{end}}
	for i := uint16(0); i < batchSize; i++ {
		currentID := prober.ht.headID[i]
		if currentID == 0 {
			// currentID of 0 indicates that ith probing row didn't have a match, so
			// we include it into the output.
			// {{if .UseSel}}
			prober.probeIdx[nResults] = sel[i]
			// {{else}}
			prober.probeIdx[nResults] = i
			// {{end}}
			nResults++
		}
	}
	// {{end}}
	// {{/*
	// Dummy return value that is never used.
	return 0
}

func _DISTINCT_COLLECT_PROBE_OUTER(prober *hashJoinProber, batchSize uint16, _USE_SEL bool) { // */}}
	// {{define "distinctCollectProbeOuter" -}}
	// Early bounds checks.
	_ = prober.ht.groupID[batchSize-1]
	_ = prober.probeRowUnmatched[batchSize-1]
	_ = prober.buildIdx[batchSize-1]
	_ = prober.probeIdx[batchSize-1]
	// {{if .UseSel}}
	_ = sel[batchSize-1]
	// {{end}}
	for i := uint16(0); i < batchSize; i++ {
		// Index of keys and outputs in the hash table is calculated as ID - 1.
		id := prober.ht.groupID[i]
		rowUnmatched := id == 0
		prober.probeRowUnmatched[i] = rowUnmatched
		if !rowUnmatched {
			prober.buildIdx[i] = id - 1
		}
		// {{if .UseSel}}
		prober.probeIdx[i] = sel[i]
		// {{else}}
		prober.probeIdx[i] = i
		// {{end}}
	}
	// {{end}}
	// {{/*
}

func _DISTINCT_COLLECT_PROBE_NO_OUTER(
	prober *hashJoinProber, batchSize uint16, nResults uint16, _USE_SEL bool,
) { // */}}
	// {{define "distinctCollectProbeNoOuter" -}}
	// Early bounds checks.
	_ = prober.ht.groupID[batchSize-1]
	_ = prober.buildIdx[batchSize-1]
	_ = prober.probeIdx[batchSize-1]
	// {{if .UseSel}}
	_ = sel[batchSize-1]
	// {{end}}
	for i := uint16(0); i < batchSize; i++ {
		if prober.ht.groupID[i] != 0 {
			// Index of keys and outputs in the hash table is calculated as ID - 1.
			prober.buildIdx[nResults] = prober.ht.groupID[i] - 1
			// {{if .UseSel}}
			prober.probeIdx[nResults] = sel[i]
			// {{else}}
			prober.probeIdx[nResults] = i
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
func (prober *hashJoinProber) collect(batch coldata.Batch, batchSize uint16, sel []uint16) uint16 {
	nResults := uint16(0)

	if prober.spec.left.outer {
		if sel != nil {
			_COLLECT_PROBE_OUTER(prober, batchSize, nResults, batch, true)
		} else {
			_COLLECT_PROBE_OUTER(prober, batchSize, nResults, batch, false)
		}
	} else {
		if sel != nil {
			switch prober.spec.joinType {
			case sqlbase.JoinType_LEFT_ANTI:
				_COLLECT_LEFT_ANTI(prober, batchSize, nResults, batch, true)
			default:
				_COLLECT_PROBE_NO_OUTER(prober, batchSize, nResults, batch, true)
			}
		} else {
			switch prober.spec.joinType {
			case sqlbase.JoinType_LEFT_ANTI:
				_COLLECT_LEFT_ANTI(prober, batchSize, nResults, batch, false)
			default:
				_COLLECT_PROBE_NO_OUTER(prober, batchSize, nResults, batch, false)
			}
		}
	}

	return nResults
}

// distinctCollect prepares the batch with the joined output columns where the build
// row index for each probe row is given in the groupID slice. This function
// requires assumes a N-1 hash join.
func (prober *hashJoinProber) distinctCollect(
	batch coldata.Batch, batchSize uint16, sel []uint16,
) uint16 {
	nResults := uint16(0)

	if prober.spec.left.outer {
		nResults = batchSize

		if sel != nil {
			_DISTINCT_COLLECT_PROBE_OUTER(prober, batchSize, true)
		} else {
			_DISTINCT_COLLECT_PROBE_OUTER(prober, batchSize, false)
		}
	} else {
		if sel != nil {
			switch prober.spec.joinType {
			case sqlbase.JoinType_LEFT_ANTI:
				// {{/* For LEFT ANTI join we don't care whether the build (right) side
				// was distinct, so we only have single variation of COLLECT method. */}}
				_COLLECT_LEFT_ANTI(prober, batchSize, nResults, batch, true)
			default:
				_DISTINCT_COLLECT_PROBE_NO_OUTER(prober, batchSize, nResults, true)
			}
		} else {
			switch prober.spec.joinType {
			case sqlbase.JoinType_LEFT_ANTI:
				// {{/* For LEFT ANTI join we don't care whether the build (right) side
				// was distinct, so we only have single variation of COLLECT method. */}}
				_COLLECT_LEFT_ANTI(prober, batchSize, nResults, batch, false)
			default:
				_DISTINCT_COLLECT_PROBE_NO_OUTER(prober, batchSize, nResults, false)
			}
		}
	}

	return nResults
}
