// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build execgen_template

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/errors"
)

// populateEqChains populates op.scratch.eqChains with indices of tuples from b
// that belong to the same groups. It returns the number of equality chains.
// Passed-in sel is updated to include tuples that are "heads" of the
// corresponding equality chains and op.ht.ProbeScratch.HashBuffer is adjusted
// accordingly. headToEqChainsID is a scratch space that must contain all
// zeroes and be of at least batchLength length.
// execgen:template<useSel>
func populateEqChains(
	op *hashAggregator, batchLength int, sel []int, headToEqChainsID []int, useSel bool,
) int {
	eqChainsCount := 0
	// Capture the slices in order for BCE to occur.
	HeadIDs := op.ht.ProbeScratch.HeadID
	hashBuffer := op.ht.ProbeScratch.HashBuffer
	_ = HeadIDs[batchLength-1]
	_ = hashBuffer[batchLength-1]
	if useSel {
		_ = sel[batchLength-1]
	}
	for i := 0; i < batchLength; i++ {
		// Since we're essentially probing the batch against itself, HeadID
		// cannot be 0, so we don't need to check that. What we have here is
		// the tuple at position i belongs to the same equality chain as the
		// tuple at position HeadID-1.
		// We will use a similar to keyID encoding for eqChains slot - all
		// tuples that should be included in eqChains[i] chain will have
		// eqChainsID = i + 1. headToEqChainsID is a mapping from HeadID to
		// eqChainsID that we're currently building in which eqChainsID
		// indicates that the current tuple is the head of its equality chain.
		//gcassert:bce
		HeadID := HeadIDs[i]
		if eqChainsID := headToEqChainsID[HeadID-1]; eqChainsID == 0 {
			// This tuple is the head of the new equality chain, so we include
			// it in updated selection vector. We also compact the hash buffer
			// accordingly.
			//gcassert:bce
			h := hashBuffer[i]
			hashBuffer[eqChainsCount] = h
			if useSel {
				//gcassert:bce
				s := sel[i]
				sel[eqChainsCount] = s
				op.scratch.eqChains[eqChainsCount] = append(op.scratch.eqChains[eqChainsCount], s)
			} else {
				sel[eqChainsCount] = i
				op.scratch.eqChains[eqChainsCount] = append(op.scratch.eqChains[eqChainsCount], i)
			}
			eqChainsCount++
			headToEqChainsID[HeadID-1] = eqChainsCount
		} else {
			// This tuple is not the head of its equality chain, so we append
			// it to already existing chain.
			if useSel {
				//gcassert:bce
				s := sel[i]
				op.scratch.eqChains[eqChainsID-1] = append(op.scratch.eqChains[eqChainsID-1], s)
			} else {
				op.scratch.eqChains[eqChainsID-1] = append(op.scratch.eqChains[eqChainsID-1], i)
			}
		}
	}
	return eqChainsCount
}

var zeroIntColumn = make([]int, coldata.MaxBatchSize)

// populateEqChains populates op.scratch.eqChains with indices of tuples from b
// that belong to the same groups. It returns the number of equality chains as
// well as a selection vector that contains "heads" of each of the chains. The
// method assumes that op.ht.ProbeScratch.HeadID has been populated with keyIDs
// of all tuples.
// NOTE: selection vector of b is modified to include only heads of each of the
// equality chains.
// NOTE: op.ht.ProbeScratch.HeadID and op.ht.ProbeScratch.differs are reset.
func (op *hashAggregator) populateEqChains(
	b coldata.Batch,
) (eqChainsCount int, eqChainsHeadsSel []int) {
	batchLength := b.Length()
	if batchLength == 0 {
		return
	}
	HeadIDToEqChainsID := op.scratch.intSlice[:batchLength]
	copy(HeadIDToEqChainsID, zeroIntColumn)
	sel := b.Selection()
	if sel != nil {
		eqChainsCount = populateEqChains(op, batchLength, sel, HeadIDToEqChainsID, true)
	} else {
		b.SetSelection(true)
		sel = b.Selection()
		eqChainsCount = populateEqChains(op, batchLength, sel, HeadIDToEqChainsID, false)
	}
	return eqChainsCount, sel
}

// findSplit returns true if there's a distinct group in op.distinctOutput, as
// well as the index of the first distinct group found, in the inclusive range
// [start, end].
//
// useSel is true if the selection vector sel should be used.
//
// ascending indicates the direction in which we should look for the first
// distinct group. If true, we look at increasing index values, if false, we
// look at decreasing index values.
// execgen:template<useSel, ascending>
// execgen:inline
func findSplit(
	op *hashAggregator, start int, end int, sel []int, useSel bool, ascending bool,
) (bool, int) {
	if ascending {
		for i := start; i <= end; i++ {
			if useSel {
				idx := sel[i]
				if op.distinctOutput[idx] {
					return true, i
				}
			} else {
				if op.distinctOutput[i] {
					return true, i
				}
			}
		}
	} else {
		for i := start; i >= end; i-- {
			if useSel {
				idx := sel[i]
				if op.distinctOutput[idx] {
					return true, i
				}
			} else {
				if op.distinctOutput[i] {
					return true, i
				}
			}
		}
	}
	return false, 0
}

// getNext provides the next batch of aggregated tuples. It is a finite state
// machine that buffers incoming batches of tuples, aggregates them according to
// the grouping column(s), and emits the aggregated tuples. The buffer may be
// larger than the batch size in order to amortize the aggregation cost.
//
// If partialOrder is true, one or more grouping columns are ordered, so the
// input is chunked into distinct groups. When enough distinct groups are found
// to fill the input buffer, getNext aggregates them. If the last group is
// complete, then the aggregated groups are emitted and getNext goes back to
// buffering more incoming tuples.
//
// If partialOrder is false, there are no guarantees on input ordering and all
// input tuples are processed before emitting any data.
// execgen:template<partialOrder>
func getNext(op *hashAggregator, partialOrder bool) coldata.Batch {
	for {
		switch op.state {
		case hashAggregatorBuffering:
			if op.bufferingState.pendingBatch != nil && op.bufferingState.unprocessedIdx < op.bufferingState.pendingBatch.Length() {
				if partialOrder {
					if op.bufferingState.splitGroup {
						// Check if the group split in the last buffered batch is completed
						// in the pendingBatch.
						var hasDistinct bool
						var idx int
						sel := op.bufferingState.pendingBatch.Selection()
						if sel != nil {
							hasDistinct, idx = findSplit(op, op.bufferingState.unprocessedIdx,
								op.bufferingState.pendingBatch.Length()-1, sel,
								true /* useSel */, true /* ascending */)
						} else {
							hasDistinct, idx = findSplit(op, op.bufferingState.unprocessedIdx,
								op.bufferingState.pendingBatch.Length()-1, sel,
								false /* useSel */, true /* ascending */)
						}
						if hasDistinct {
							op.bufferingState.splitGroup = false
							if op.bufferingState.unprocessedIdx < idx {
								op.bufferingState.tuples.AppendTuples(
									op.bufferingState.pendingBatch, op.bufferingState.unprocessedIdx, idx,
								)
								op.bufferingState.unprocessedIdx = idx
								op.state = hashAggregatorAggregating
							} else {
								// The first tuple in the pendingBatch is a new group, so we
								// can emit the already aggregated tuples.
								op.state = hashAggregatorOutputting
							}
							continue
						}
					}
				}
				op.bufferingState.tuples.AppendTuples(
					op.bufferingState.pendingBatch, op.bufferingState.unprocessedIdx, op.bufferingState.pendingBatch.Length(),
				)
			}

			op.bufferingState.pendingBatch, op.bufferingState.unprocessedIdx = op.Input.Next(), 0
			op.bufferingState.unprocessedIdx = 0
			if partialOrder {
				op.distincterInput.SetBatch(op.bufferingState.pendingBatch)
				op.distincter.Next()
			}
			n := op.bufferingState.pendingBatch.Length()
			if op.inputTrackingState.tuples != nil {
				op.inputTrackingState.tuples.Enqueue(op.Ctx, op.bufferingState.pendingBatch)
				op.inputTrackingState.zeroBatchEnqueued = n == 0
			}
			if n == 0 {
				// This is the last input batch.
				if op.bufferingState.tuples.Length() == 0 {
					// There are currently no buffered tuples to perform the
					// aggregation on.
					if len(op.buckets) == 0 {
						// We don't have any buckets which means that there were
						// no input tuples whatsoever, so we can transition to
						// finished state right away.
						op.state = hashAggregatorDone
					} else {
						// There are some buckets, so we proceed to the
						// outputting state.
						op.state = hashAggregatorOutputting
					}
				} else {
					// There are some buffered tuples on which we need to run
					// the aggregation.
					op.state = hashAggregatorAggregating
				}
				continue
			}

			toBuffer := n
			if op.bufferingState.tuples.Length()+toBuffer > hashAggregatorMaxBuffered {
				toBuffer = hashAggregatorMaxBuffered - op.bufferingState.tuples.Length()
			}
			if partialOrder {
				hasDistinct := false
				// Search for last distinct group in each batch. We need to process the
				// batches in order.
				if toBuffer > 0 && (op.bufferingState.splitGroup || op.bufferingState.tuples.Length()+toBuffer >= hashAggregatorMaxBuffered) {
					// See if there is a complete group in the last batch.
					// Make sure there is at least one tuple either buffered or already
					// aggregated.
					lowerBound := 0
					if op.bufferingState.tuples.Length() == 0 && len(op.buckets) == 0 {
						lowerBound = 1
					}
					var idx int
					sel := op.bufferingState.pendingBatch.Selection()
					if sel != nil {
						hasDistinct, idx = findSplit(op, toBuffer-1, lowerBound, sel,
							true /* useSel */, false /* ascending */)
					} else {
						hasDistinct, idx = findSplit(op, toBuffer-1, lowerBound, sel,
							false /* useSel */, false /* ascending */)
					}
					if hasDistinct {
						op.bufferingState.splitGroup = false
						toBuffer = idx
						if op.bufferingState.tuples.Length() == 0 && toBuffer == 0 {
							// The last batch was split, but this is a distinct group, so we
							// can emit the last aggregated batch.
							op.state = hashAggregatorOutputting
							continue
						}
					} else {
						op.bufferingState.splitGroup = true
					}
				} else if toBuffer == 0 {
					op.bufferingState.splitGroup = true
				}
			}
			if toBuffer > 0 {
				op.bufferingState.tuples.AppendTuples(op.bufferingState.pendingBatch, 0 /* startIdx */, toBuffer)
				op.bufferingState.unprocessedIdx = toBuffer
			}
			if op.bufferingState.tuples.Length() == hashAggregatorMaxBuffered {
				op.state = hashAggregatorAggregating
				continue
			}
			if partialOrder {
				if hasDistinct {
					op.state = hashAggregatorAggregating
					continue
				}
			}

		case hashAggregatorAggregating:
			op.inputArgsConverter.ConvertBatch(op.bufferingState.tuples)
			op.onlineAgg(op.bufferingState.tuples)
			if op.bufferingState.pendingBatch.Length() == 0 {
				if len(op.buckets) == 0 {
					op.state = hashAggregatorDone
				} else {
					op.state = hashAggregatorOutputting
				}
				continue
			}
			if partialOrder {
				if !op.bufferingState.splitGroup {
					// The aggregated groups are complete, so we can emit them before
					// buffering more tuples.
					op.bufferingState.tuples.ResetInternalBatch()
					op.state = hashAggregatorOutputting
					continue
				}
			}
			op.bufferingState.tuples.ResetInternalBatch()
			op.state = hashAggregatorBuffering

		case hashAggregatorOutputting:
			// Note that ResetMaybeReallocate truncates the requested capacity
			// at coldata.BatchSize(), so we can just try asking for
			// len(op.buckets)-op.curOutputBucketIdx (the number of remaining
			// output tuples) capacity.
			op.output, _ = op.accountingHelper.ResetMaybeReallocate(
				op.outputTypes, op.output, len(op.buckets)-op.curOutputBucketIdx,
			)
			curOutputIdx := 0
			for batchDone := false; op.curOutputBucketIdx < len(op.buckets) && !batchDone; {
				bucket := op.buckets[op.curOutputBucketIdx]
				for fnIdx, fn := range bucket.fns {
					fn.SetOutput(op.output.ColVec(fnIdx))
					fn.Flush(curOutputIdx)
				}
				batchDone = op.accountingHelper.AccountForSet(curOutputIdx)
				curOutputIdx++
				op.curOutputBucketIdx++
			}
			if op.curOutputBucketIdx >= len(op.buckets) {
				if partialOrder {
					if l := op.bufferingState.pendingBatch.Length(); l > 0 {
						// Clear the buckets.
						op.state = hashAggregatorBuffering
						op.resetBucketsAndTrackingState(op.Ctx)
						// Add back unprocessed tuples from the pending batch to the input
						// tracking state that were not emitted. We do this by modifying or
						// adding a selection vector to pending batch that only contains the
						// remaining pending tuples. We can use this modified pending batch
						// in the buffering state, since it only contains tuples that still
						// need to be aggregated, so we do not need to reset to the original
						// batch state.
						if op.inputTrackingState.tuples != nil && op.bufferingState.unprocessedIdx < l {
							sel := op.bufferingState.pendingBatch.Selection()
							if sel != nil {
								copy(sel, sel[op.bufferingState.unprocessedIdx:l])
								op.bufferingState.pendingBatch.SetLength(l - op.bufferingState.unprocessedIdx)
							} else {
								colexecutils.UpdateBatchState(
									op.bufferingState.pendingBatch, l-op.bufferingState.unprocessedIdx, true, /* usesSel */
									colexecutils.DefaultSelectionVector[op.bufferingState.unprocessedIdx:l],
								)
							}
							op.inputTrackingState.tuples.Enqueue(op.Ctx, op.bufferingState.pendingBatch)
							// We modified pendingBatch to only contain unprocessed
							// tuples, so we need to reset the unprocessedIdx to 0.
							op.bufferingState.unprocessedIdx = 0
						}
					} else {
						op.state = hashAggregatorDone
					}
				} else {
					op.state = hashAggregatorDone
				}
			}
			op.output.SetLength(curOutputIdx)
			return op.output

		case hashAggregatorDone:
			return coldata.ZeroBatch

		default:
			colexecerror.InternalError(errors.AssertionFailedf("hash aggregator in unhandled state"))
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
	}
}

func (op *hashAggregator) Next() coldata.Batch {
	if len(op.spec.OrderedGroupCols) > 0 {
		return getNext(op, true)
	} else {
		return getNext(op, false)
	}
}
