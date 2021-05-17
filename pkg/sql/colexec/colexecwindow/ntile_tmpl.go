// Copyright 2021 The Cockroach Authors.
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
// This file is the execgen template for ntile.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexecwindow

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// TODO(yuzefovich): add benchmarks.
// TODO(drewk): consider adding a version optimized for the case where no
//  ORDER BY is specified, since the bucket value can be calculated simply by
//  taking the row number modulo the number of buckets.

// NewNTileOperator creates a new Operator that computes window function NTILE.
// outputColIdx specifies in which coldata.Vec the operator should put its
// output (if there is no such column, a new column is appended).
func NewNTileOperator(
	unlimitedAllocator *colmem.Allocator,
	memoryLimit int64,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	input colexecop.Operator,
	inputTypes []*types.T,
	outputColIdx int,
	partitionColIdx int,
	argIdx int,
	diskAcc *mon.BoundAccount,
) colexecop.Operator {
	input = colexecutils.NewVectorTypeEnforcer(unlimitedAllocator, input, types.Int, outputColIdx)
	base := nTileBase{
		nTileInitFields: nTileInitFields{
			OneInputNode:    colexecop.NewOneInputNode(input),
			allocator:       unlimitedAllocator,
			memoryLimit:     memoryLimit,
			diskQueueCfg:    diskQueueCfg,
			fdSemaphore:     fdSemaphore,
			inputTypes:      inputTypes,
			diskAcc:         diskAcc,
			outputColIdx:    outputColIdx,
			partitionColIdx: partitionColIdx,
			argIdx:          argIdx,
		},
	}
	if partitionColIdx == -1 {
		return &nTileNoPartitionOp{base}
	}
	return &nTileWithPartitionOp{base}
}

type nTileState int

// Transition graph for nTileState:
//     ┌─────────────────┬────────────┐
//     │                 │nTileLoading├────────────────┐
//     │    ┌───────────►└──────┬─────┘                │
//     │    │                   │                      │
//     │    │                   │                      │
//     │    │                   │                      │
//     │    │                   │                      │
//     │    │                   │                      │
//     ▼    │                   ▼                      ▼
// ┌────────┴───┐ ◄─────┬───────────────┐       ┌─────────────┐
// │nTileSeeking│       │nTileProcessing├─────► │nTileFinished│
// └────────────┴──────►└──┬────────────┘       └─────────────┘
//                         │        ▲
//                         │        │
//                         │        │
//                         └────────┘

const (
	// nTileLoading is the state in which the ntile operators load an additional
	// input batch into the currentBatch field. If necessary, the old value of
	// currentBatch will be pushed to the buffer queue.
	//
	// nTileLoading transitions to nTileSeeking unless the end of the input has
	// been reached, in which case the next state is nTileProcessing or
	// nTileFinished depending on whether all rows have already been emitted.
	nTileLoading nTileState = iota
	// nTileSeeking is the state in which the ntile operators seek to the index of
	// the next partition and retrieve the number of ntile buckets for the current
	// partition, as well as the number of tuples in the partition.
	//
	// nTileSeeking transitions to nTileLoading or nTileProcessing, depending on
	// whether the end of the current partition has been found.
	nTileSeeking
	// nTileProcessing is the state in which the ntile operators fill in the
	// current partition for all loaded batches with ntile bucket values. Batches
	// are emitted as soon as the ntile column has been filled.
	//
	// nTileProcessing transitions to itself when a batch has been emitted but the
	// partition is not fully processed. It transitions to nTileSeeking when a
	// partition has been fully processed. Finally, nTileProcessing will
	// transition to nTileFinished when all batches have been fully processed and
	// emitted.
	nTileProcessing
	// nTileFinished is the state in which the ntile operators close any
	// non-closed disk resources and emit the zero-length batch. nTileFinished is
	// the terminal state.
	nTileFinished
)

// nTileInitFields extracts common initializations of two variations of nTile
// operators. Note that it is not an operator itself and should not be used
// directly.
type nTileInitFields struct {
	colexecop.OneInputNode
	colexecop.InitHelper

	allocator    *colmem.Allocator
	memoryLimit  int64
	diskQueueCfg colcontainer.DiskQueueCfg
	fdSemaphore  semaphore.Semaphore
	inputTypes   []*types.T
	outputTypes  []*types.T
	diskAcc      *mon.BoundAccount

	outputColIdx    int
	partitionColIdx int
	argIdx          int
}

// nTileBase extracts common fields of two variations of nTile operators. Note
// that it is not an operator itself and should not be used directly.
type nTileBase struct {
	colexecop.CloserHelper
	nTileInitFields
	nTileComputeFields

	// state is used to ensure that the nTile operators can continue processing
	// where they left off after emitting fully processed batches in streaming
	// fashion.
	state nTileState

	// currentBatch is the batch that is currently being probed to determine the
	// size of the current partition.
	currentBatch coldata.Batch

	// bufferQueue stores batches that are waiting to be fully processed and
	// emitted. Note that since processed batches are emitted once the end of a
	// partition is reached, all batches in bufferQueue with the possible
	// exception of the head belong to the same partition.
	bufferQueue *colexecutils.SpillingQueue

	// nextPartitionIdx is the index of the start of the next partition within the
	// current batch. If the next partition does not start in the current batch,
	// nextPartitionIdx is set to the length of the current batch.
	nextPartitionIdx int

	// processingIdx is an index into all tuples currently buffered, beginning
	// with those in the queue and ending with those in currentBatch. It is used
	// to preserve state when the nTileProcessing state spans more than one batch.
	processingIdx int

	// seekIdx is an index into the current batch used to preserve state when the
	// nTileSeeking stage spans more than one batch.
	seekIdx int
}

// nTileComputeFields extracts the fields that are used to calculate ntile
// bucket values.
type nTileComputeFields struct {
	// boundary is the number of rows that should be in the current bucket.
	boundary int

	// currBucketCount is the row number of the current bucket.
	currBucketCount int

	// partitionSize tracks the number of tuples in the current partition.
	partitionSize int

	// numBuckets is the number of buckets across which to distribute the rows of
	// the current partition. It is not necessarily the same value for different
	// partitions.
	numBuckets int

	// hasArg indicates whether a non-null value has been found for numBuckets in
	// the current partition.
	hasArg bool

	// nTile is the bucket value to which the row currently being processed is
	// assigned. It is reset once processing of a new partition begins.
	nTile int64
}

func (r *nTileBase) Init(ctx context.Context) {
	if !r.InitHelper.Init(ctx) {
		return
	}
	r.Input.Init(r.Ctx)
	r.state = nTileLoading
	r.outputTypes = make([]*types.T, len(r.inputTypes), len(r.inputTypes)+1)
	copy(r.outputTypes, r.inputTypes)
	r.outputTypes = append(r.outputTypes, types.Int)
	r.bufferQueue = colexecutils.NewSpillingQueue(
		&colexecutils.NewSpillingQueueArgs{
			UnlimitedAllocator: r.allocator,
			Types:              r.outputTypes,
			MemoryLimit:        r.memoryLimit,
			DiskQueueCfg:       r.diskQueueCfg,
			FDSemaphore:        r.fdSemaphore,
			DiskAcc:            r.diskAcc,
		},
	)
}

// {{range .}}

type _NTILE_STRINGOp struct {
	nTileBase
}

var _ colexecop.Operator = &_NTILE_STRINGOp{}

func (r *_NTILE_STRINGOp) Next() coldata.Batch {
	var err error
	for {
		switch r.state {
		case nTileLoading:
			batch := r.Input.Next()
			if batch.Length() == 0 {
				// We have reached the end of the input.
				if !r.bufferQueue.Empty() || (r.currentBatch != nil && r.currentBatch.Length() > 0) {
					// There are still tuples that need to be processed.
					r.transitionToProcessing()
					break
				}
				// The last batch (if any) was already emitted.
				r.state = nTileFinished
				break
			}
			// Load the next batch into currentBatch. If currentBatch still has data,
			// move it into the queue.
			if r.currentBatch != nil && r.currentBatch.Length() > 0 {
				r.bufferQueue.Enqueue(r.Ctx, r.currentBatch)
			}
			// We have to copy the input batch data because calling Next on the input
			// may invalidate the contents of the last Batch returned by Next. Note
			// that the batch will be densely copied, so currentBatch will never have
			// a selection vector.
			// TODO(drewk): the output column probably doesn't need to be deep-copied.
			n := batch.Length()
			sel := batch.Selection()
			r.currentBatch, _ = r.allocator.ResetMaybeReallocate(
				r.outputTypes, r.currentBatch, batch.Length(), math.MaxInt64)
			r.allocator.PerformOperation(r.currentBatch.ColVecs(), func() {
				for colIdx, vec := range r.currentBatch.ColVecs() {
					vec.Copy(
						coldata.CopySliceArgs{
							SliceArgs: coldata.SliceArgs{
								Src:       batch.ColVec(colIdx),
								Sel:       sel,
								SrcEndIdx: n,
							},
						},
					)
				}
				r.currentBatch.SetLength(n)
			})
			r.seekIdx = 0
			r.state = nTileSeeking
		case nTileSeeking:
			n := r.currentBatch.Length()
			nTileVec := r.currentBatch.ColVec(r.outputColIdx)
			argVec := r.currentBatch.ColVec(r.argIdx)
			argNulls := argVec.Nulls()
			argCol := argVec.Int64()
			// {{if .HasPartition}}
			partitionCol := r.currentBatch.ColVec(r.partitionColIdx).Bool()
			_ = partitionCol[n-1]
			// {{end}}
			i := r.seekIdx
			_ = argCol[n-1]
			if !r.hasArg {
				// Scan to the first non-null num_buckets value. If it is not found,
				// scan to the beginning of the next partition.
				_ = argCol[i]
				for ; i < n; i++ {
					// {{if .HasPartition}}
					//gcassert:bce
					if partitionCol[i] {
						// Don't break for the start of the current partition.
						if i != r.seekIdx || !r.bufferQueue.Empty() {
							break
						}
					}
					// {{end}}
					if !argNulls.NullAt(i) {
						// We have found the first row in the current partition for which
						// the argument is non-null.
						//gcassert:bce
						r.numBuckets = int(argCol[i])
						r.hasArg = true
						break
					}
				}
				// For all leading rows in a partition whose argument is null, the ntile
				// bucket value is also null. Set these rows to null, then increment
				// processingIdx to indicate that bucket values are not to be calculated
				// for these rows.
				nTileVec.Nulls().SetNullRange(r.seekIdx, i)
				r.processingIdx += i - r.seekIdx
			}
			// {{if .HasPartition}}
			// Pick up where the last loop left off to find the location of the start
			// of the next partition (and the end of the current one).
			if i < n {
				_ = partitionCol[i]
				for ; i < n; i++ {
					//gcassert:bce
					if partitionCol[i] {
						// Don't break for the start of the current partition.
						if i != r.seekIdx || !r.bufferQueue.Empty() {
							break
						}
					}
				}
			}
			r.partitionSize += i - r.seekIdx
			r.nextPartitionIdx = i
			// {{else}}
			// There is only one partition, so it includes the entirety of this batch.
			r.partitionSize += n
			r.nextPartitionIdx = n
			// {{end}}
			if r.hasArg && r.numBuckets <= 0 {
				colexecerror.ExpectedError(builtins.ErrInvalidArgumentForNtile)
			}
			if r.nextPartitionIdx >= n {
				// The start of the next partition is not located in the current batch.
				r.state = nTileLoading
				break
			}
			// The number of buckets and partition size have been found (or the number
			// of buckets is null for the entire partition), so the ntile bucket
			// values can be calculated for this partition.
			r.transitionToProcessing()
		case nTileProcessing:
			if !r.bufferQueue.Empty() {
				var output coldata.Batch
				if output, err = r.bufferQueue.Dequeue(r.Ctx); err != nil {
					colexecerror.InternalError(err)
				}
				// Set all the ntile bucket values that remain unset, then emit this
				// batch. Note that the beginning of the next partition will always be
				// located in currentBatch, so the current partition will always end
				// beyond any batch that is stored in the queue (thus we call setNTile
				// with the length of the batch).
				r.setNTile(output, output.Length())
				r.processingIdx -= output.Length()
				if r.processingIdx < 0 {
					// processingIdx was located somewhere within this batch, meaning the
					// first of the tuples yet to be processed was somewhere in the batch.
					r.processingIdx = 0
				}
				return output
			}
			if r.currentBatch.Length() > 0 {
				r.setNTile(r.currentBatch, r.nextPartitionIdx)
				if r.nextPartitionIdx >= r.currentBatch.Length() {
					// This was the last batch and it has been entirely filled.
					r.state = nTileFinished
					return r.currentBatch
				}
				// The next partition begins within this batch. Set processingIdx to the
				// beginning of the next partition and seek to the end of the next
				// partition.
				r.processingIdx = r.nextPartitionIdx
				r.seekIdx = r.nextPartitionIdx
				r.state = nTileSeeking
				r.partitionSize = 0
				r.hasArg = false
				break
			}
			colexecerror.InternalError(
				errors.AssertionFailedf("ntile operator in processing state without buffered rows"))
		case nTileFinished:
			if err = r.Close(); err != nil {
				colexecerror.InternalError(err)
			}
			return coldata.ZeroBatch
		default:
			colexecerror.InternalError(errors.AssertionFailedf("ntile operator in unhandled state"))
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
	}
}

// {{end}}

func (r *nTileBase) transitionToProcessing() {
	r.state = nTileProcessing
	if !r.hasArg {
		// Since a non-null value for the number of buckets was not found, the
		// output column values have already been set to null, so no additional
		// calculations are necessary.
		return
	}
	if r.numBuckets <= 0 {
		colexecerror.InternalError(
			errors.AssertionFailedf("ntile operator calculating bucket value with invalid argument"))
	}
	r.nTile = 1
	r.currBucketCount = 0
	r.boundary = r.partitionSize / r.numBuckets
	if r.boundary <= 0 {
		r.boundary = 1
	} else {
		// If the total number is not divisible, add 1 row to leading buckets.
		if r.partitionSize%r.numBuckets != 0 {
			r.boundary++
		}
	}
}

// setNTile sets the ntile bucket values for the given batch from
// r.processingIdx up to the given endIdx. It expects that batch is not nil, and
// that the loading and seeking states have run to completion for the current
// partition.
func (r *nTileBase) setNTile(batch coldata.Batch, endIdx int) {
	if r.processingIdx >= endIdx {
		// No processing needs to be done for this portion of the current partition.
		// This can happen when the num_buckets value for a partition was null up to
		// the end of a batch.
		return
	}
	if r.numBuckets <= 0 {
		colexecerror.InternalError(
			errors.AssertionFailedf("ntile operator calculating bucket value with invalid argument"))
	}
	remainder := r.partitionSize % r.numBuckets
	nTileVec := batch.ColVec(r.outputColIdx)
	nTileCol := nTileVec.Int64()
	_ = nTileCol[r.processingIdx]
	_ = nTileCol[endIdx-1]
	for i := r.processingIdx; i < endIdx; i++ {
		r.currBucketCount++
		if r.boundary < r.currBucketCount {
			// Move to next ntile bucket.
			if remainder != 0 && int(r.nTile) == remainder {
				r.boundary--
				remainder = 0
			}
			r.nTile++
			r.currBucketCount = 1
		}
		//gcassert:bce
		nTileCol[i] = r.nTile
	}
}

func (r *nTileBase) Close() error {
	if !r.CloserHelper.Close() {
		return nil
	}
	return r.bufferQueue.Close(r.nTileInitFields.GetNonNilCtx())
}
