// Copyright 2020 The Cockroach Authors.
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

import "github.com/cockroachdb/cockroach/pkg/col/coldata"

// populateEqChains populates op.scratch.eqChains with indices of tuples from b
// that belong to the same groups. It returns the number of equality chains.
// Passed-in sel is updated to include tuples that are "heads" of the
// corresponding equality chains and op.ht.probeScratch.hashBuffer is adjusted
// accordingly. headToEqChainsID is a scratch space that must contain all
// zeroes and be of at least batchLength length.
// execgen:template<useSel>
func populateEqChains(
	op *hashAggregator, batchLength int, sel []int, headToEqChainsID []int, useSel bool,
) int {
	eqChainsCount := 0
	for i, headID := range op.ht.probeScratch.headID[:batchLength] {
		// Since we're essentially probing the batch against itself, headID
		// cannot be 0, so we don't need to check that. What we have here is
		// the tuple at position i belongs to the same equality chain as the
		// tuple at position headID-1.
		// We will use a similar to keyID encoding for eqChains slot - all
		// tuples that should be included in eqChains[i] chain will have
		// eqChainsID = i + 1. headToEqChainsID is a mapping from headID to
		// eqChainsID that we're currently building in which eqChainsID
		// indicates that the current tuple is the head of its equality chain.
		if eqChainsID := headToEqChainsID[headID-1]; eqChainsID == 0 {
			// This tuple is the head of the new equality chain, so we include
			// it in updated selection vector. We also compact the hash buffer
			// accordingly.
			op.ht.probeScratch.hashBuffer[eqChainsCount] = op.ht.probeScratch.hashBuffer[i]
			if useSel {
				sel[eqChainsCount] = sel[i]
				op.scratch.eqChains[eqChainsCount] = append(op.scratch.eqChains[eqChainsCount], sel[i])
			} else {
				sel[eqChainsCount] = i
				op.scratch.eqChains[eqChainsCount] = append(op.scratch.eqChains[eqChainsCount], i)
			}
			eqChainsCount++
			headToEqChainsID[headID-1] = eqChainsCount
		} else {
			// This tuple is not the head of its equality chain, so we append
			// it to already existing chain.
			if useSel {
				op.scratch.eqChains[eqChainsID-1] = append(op.scratch.eqChains[eqChainsID-1], sel[i])
			} else {
				op.scratch.eqChains[eqChainsID-1] = append(op.scratch.eqChains[eqChainsID-1], i)
			}
		}
	}
	return eqChainsCount
}

// populateEqChains populates op.scratch.eqChains with indices of tuples from b
// that belong to the same groups. It returns the number of equality chains as
// well as a selection vector that contains "heads" of each of the chains. The
// method assumes that op.ht.probeScratch.headID has been populated with keyIDs
// of all tuples.
// NOTE: selection vector of b is modified to include only heads of each of the
// equality chains.
// NOTE: op.ht.probeScratch.headID and op.ht.probeScratch.differs are reset.
func (op *hashAggregator) populateEqChains(
	b coldata.Batch,
) (eqChainsCount int, eqChainsHeadsSel []int) {
	batchLength := b.Length()
	headIDToEqChainsID := op.scratch.intSlice[:batchLength]
	copy(headIDToEqChainsID, zeroIntColumn)
	sel := b.Selection()
	if sel != nil {
		eqChainsCount = populateEqChains(op, batchLength, sel, headIDToEqChainsID, true)
	} else {
		b.SetSelection(true)
		sel = b.Selection()
		eqChainsCount = populateEqChains(op, batchLength, sel, headIDToEqChainsID, false)
	}
	return eqChainsCount, sel
}
