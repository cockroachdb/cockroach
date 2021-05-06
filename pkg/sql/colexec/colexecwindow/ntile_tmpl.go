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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// TODO(yuzefovich): add benchmarks.

// NewNTileOperator creates a new Operator that computes window function
// NTILE. outputColIdx specifies in which coldata.Vec the operator should
// put its output (if there is no such column, a new column is appended).
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

const (
	// nTileLoading is the state in which ntile operators load an additional
	// input batch into the currentBatch field. If necessary, the old value of
	// currentBatch will be pushed to the buffer queue.
	nTileLoading nTileState = iota
	// nTileSeeking is the state in which ntile operators seek to the index of the
	// next partition. And retrieve the number of ntile buckets for the current
	// partition.
	nTileSeeking
	// nTileProcessing is the state in which ntile operators fill in the current
	// partition for all loaded batches with ntile bucket values. Batches are
	// emitted as soon as the ntile column has been filled.
	nTileProcessing
	// nTileFinished is the state in which ntile operators close any non-closed
	// disk resources and emit the zero-length batch.
	nTileFinished
)

// nTileBase extracts common initializations of two variations of nTile
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

	// state is used to ensure that nTile operators can continue processing
	// where they left off after emitting fully processed batches in streaming
	// fashion.
	state nTileState

	// currentBatch is the batch that is currently being probed to determine the
	// size of the current partition.
	currentBatch coldata.Batch

	// resetCurrentBatch indicates that the current batch needs to be reset.
	resetCurrentBatch bool

	// bufferQueue stores batches that are waiting to be fully processed and
	// emitted.
	bufferQueue *colexecutils.SpillingQueue

	// nextPartitionIdx is the index of the start of the next partition within the
	// current batch. If the next partition does not start in the current batch,
	// nextPartitionIdx is the length of the current batch.
	nextPartitionIdx int

	// processingIdx is the index of the first row of the current partition that has a
	// non-null argument value within the scope of all tuples encountered so far
	// (including those emitted or queued).
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

// {{/*
// _GET_NUM_BUCKETS is a code snippet that retrieves the first non-null value to
// which the argument column evaluates. It also looks at i'th partitionCol value
// to check whether a new partition begins at index i, and if so, it records the
// size of the current partition.
func _GET_NUM_BUCKETS(_HAS_SEL bool) { // */}}
	// {{define "getNumBuckets" -}}
	// {{if .HasPartition}}
	// {{if not $.HasSel}}
	//gcassert:bce
	// {{end}}
	if partitionCol[i] {
		// Don't break for the start of the current partition.
		// {{if $.HasSel}}
		if i != r.seekIdx || !r.bufferQueue.Empty() {
			break
		}
		// {{else}}
		if i != r.seekIdx || !r.bufferQueue.Empty() {
			break
		}
		// {{end}}
	}
	// {{end}}
	if !argNulls.NullAt(i) {
		// We have found the first row in the current partition for which the
		// argument is non-null.
		// {{if not $.HasSel}}
		//gcassert:bce
		// {{end}}
		r.numBuckets = int(argCol[i])
		r.hasArg = true
		break
	}
	// {{end}}
	// {{/*
} // */}}

// {{/*
// _COMPUTE_PARTITION_SIZE is a code snippet that computes the sizes of
// partitions. It looks at i'th partitionCol value to check whether a new
// partition begins at index i, and if so, it records the size of the
// current partition.
func _COMPUTE_PARTITION_SIZE(_HAS_SEL bool) { // */}}
	// {{define "computePartitionSize" -}}
	// {{if not $.HasSel}}
	//gcassert:bce
	// {{end}}
	if partitionCol[i] {
		// Don't break for the start of the current partition.
		// {{if $.HasSel}}
		if i != r.seekIdx || !r.bufferQueue.Empty() {
			break
		}
		// {{else}}
		if i != r.seekIdx || !r.bufferQueue.Empty() {
			break
		}
		// {{end}}
	}
	// {{end}}
	// {{/*
} // */}}

// {{/*
// _COMPUTE_NTILE is a code snippet that computes the ntile bucket value
// for a single tuple at index i.
func _COMPUTE_NTILE(_HAS_SEL) { // */}}
	// {{define "computeNTile" -}}
	r.currBucketCount++
	if r.boundary < r.currBucketCount {
		// Move to next ntile bucket.
		remainder := r.partitionSize % r.numBuckets
		if remainder != 0 && int(r.nTile) == remainder {
			r.boundary--
		}
		r.nTile++
		r.currBucketCount = 1
	}
	// {{if not $.HasSel}}
	//gcassert:bce
	// {{end}}
	nTileCol[i] = r.nTile
	// {{end}}
	// {{/*
} // */}}

var errInvalidArgumentForNtile = pgerror.Newf(
	pgcode.InvalidParameterValue, "argument of ntile() must be greater than zero")

// {{range .}}

type _NTILE_STRINGOp struct {
	nTileBase
}

var _ colexecop.Operator = &_NTILE_STRINGOp{}

func (r *_NTILE_STRINGOp) Init(ctx context.Context) {
	if !r.InitHelper.Init(ctx) {
		return
	}
	r.Input.Init(r.Ctx)
	r.state = nTileLoading
	r.bufferQueue = colexecutils.NewSpillingQueue(
		&colexecutils.NewSpillingQueueArgs{
			UnlimitedAllocator: r.allocator,
			Types:              append(r.inputTypes, types.Int),
			MemoryLimit:        int64(r.memoryLimit),
			DiskQueueCfg:       r.diskQueueCfg,
			FDSemaphore:        r.fdSemaphore,
			DiskAcc:            r.diskAcc,
		},
	)
	r.currentBatch = r.allocator.NewMemBatchWithFixedCapacity(
		append(r.inputTypes, types.Int), coldata.BatchSize())
}

func (r *_NTILE_STRINGOp) Next() coldata.Batch {
	var err error
	for {
		switch r.state {
		case nTileLoading:
			if r.resetCurrentBatch {
				r.resetCurrentBatch = false
				r.currentBatch.ResetInternalBatch()
			}
			batch := r.Input.Next()
			if batch.Length() == 0 {
				// We have reached the end of the input.
				if r.hasArg {
					r.transitionToProcessing()
					break
				}
				// A non-null argument value was not found, so the last batch was
				// already emitted.
				r.state = nTileFinished
				break
			}
			// Load the next batch into currentBatch. If currentBatch still has data,
			// move it into the queue.
			if r.currentBatch.Length() > 0 {
				r.bufferQueue.Enqueue(r.Ctx, r.currentBatch)
				r.currentBatch.ResetInternalBatch()
			}
			// We have to copy the input batch data because calling Next on the input
			// may invalidate the contents of the last Batch returned by Next.
			n := batch.Length()
			sel := batch.Selection()
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
			// {{end}}

			sel := r.currentBatch.Selection()
			if sel != nil {
				var i int
				selIdx := r.seekIdx
				if !r.hasArg {
					// We must first find the first row for which num_buckets is not null.
					for selIdx, i = range sel[selIdx:n] {
						_GET_NUM_BUCKETS(true)
					}
					nTileVec.Nulls().SetNullRange(r.seekIdx, selIdx)
					r.processingIdx = selIdx
				}
				// {{if .HasPartition}}
				// Pick up where the last loop left off to find the start of the next
				// partition.
				for selIdx, i = range sel[selIdx:n] {
					_COMPUTE_PARTITION_SIZE(true)
				}
				r.partitionSize += selIdx - r.seekIdx
				r.nextPartitionIdx = selIdx
				// {{else}}
				r.partitionSize += len(sel[:n])
				r.nextPartitionIdx = n
				// {{end}}
			} else {
				i := r.seekIdx
				// {{if .HasPartition}}
				_ = partitionCol[n-1]
				// {{end}}
				if !r.hasArg {
					// We must first find the first row for which num_buckets is not null.
					// {{if .HasPartition}}
					_ = partitionCol[i]
					// {{end}}
					for ; i < n; i++ {
						_GET_NUM_BUCKETS(false)
					}
					nTileVec.Nulls().SetNullRange(r.seekIdx, i)
					r.processingIdx = i
				}
				// {{if .HasPartition}}
				// Pick up where the last loop left off to find the start of the next
				// partition.
				if i < n {
					_ = partitionCol[i]
					for ; i < n; i++ {
						_COMPUTE_PARTITION_SIZE(false)
					}
				}
				r.partitionSize += i - r.seekIdx
				r.nextPartitionIdx = i
				// {{else}}
				r.partitionSize += n
				r.nextPartitionIdx = n
				// {{end}}
			}
			if r.hasArg {
				if r.numBuckets <= 0 {
					colexecerror.ExpectedError(errInvalidArgumentForNtile)
				}
				if r.nextPartitionIdx == n {
					// The start of the next partition is not located in the current batch.
					r.state = nTileLoading
					break
				}
			}
			// The number of buckets and partition size have been found (or the number
			// of buckets is null for the entire batch), so the ntile bucket values
			// can be calculated for this partition.
			r.transitionToProcessing()
		case nTileProcessing:
			if !r.bufferQueue.Empty() {
				var output coldata.Batch
				if output, err = r.bufferQueue.Dequeue(r.Ctx); err != nil {
					colexecerror.InternalError(err)
				}
				if r.processingIdx >= output.Length() {
					// This batch has already been fully processed. Subtract the offset
					// for this batch from processingIdx and return the batch.
					r.processingIdx -= output.Length()
					return output
				}
				// Set all the ntile bucket values, then emit this batch.
				r.setNTile(output, output.Length())
				r.processingIdx = 0
				return output
			}
			if r.currentBatch.Length() > 0 {
				r.setNTile(r.currentBatch, r.nextPartitionIdx)
				if r.nextPartitionIdx >= r.currentBatch.Length() {
					if r.hasArg {
						// This was the last batch and it has been entirely filled.
						r.state = nTileFinished
					} else {
						// This partition was filled with nulls up to the end of the
						// partition. Emit it, but don't begin a new partition.
						r.processingIdx = 0
						r.state = nTileLoading
						r.resetCurrentBatch = true
					}
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
			// There are no tuples. This can happen if the first batch returned by the
			// input is the zero batch.
			r.state = nTileFinished
		case nTileFinished:
			if err = r.Close(r.Ctx); err != nil {
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

func (r *_NTILE_STRINGOp) transitionToProcessing() {
	r.state = nTileProcessing
	if !r.hasArg {
		return
	}
	r.nTile = 1
	r.currBucketCount = 0
	r.boundary = r.partitionSize / r.numBuckets
	if r.boundary <= 0 {
		r.boundary = 1
	} else {
		// If the total number is not divisible, add 1 row to leading buckets.
		if r.partitionSize % r.numBuckets != 0 {
			r.boundary++
		}
	}
}

// setNTile sets the ntile bucket values for the given batch from r.processingIdx
// up to the given endIdx. It expects that batch is not nil, and that the
// loading and seeking states have run to completion for the current partition
// (represented by the interval [r.processingIdx, endIdx)).
func (r *_NTILE_STRINGOp) setNTile(batch coldata.Batch, endIdx int) {
	if endIdx <= 0 || r.processingIdx >= batch.Length() {
		// No processing needs to be done for this portion of the current partition.
		return
	}
	nTileVec := batch.ColVec(r.outputColIdx)
	nTileCol := nTileVec.Int64()
	sel := batch.Selection()
	if sel != nil {
		for _, i := range sel[r.processingIdx:endIdx] {
			_COMPUTE_NTILE(true)
		}
	} else {
		_ = nTileCol[r.processingIdx]
		_ = nTileCol[endIdx-1]
		for i := r.processingIdx; i < endIdx; i++ {
			_COMPUTE_NTILE(false)
		}
	}
}


func (r *_NTILE_STRINGOp) Close(ctx context.Context) error {
	if !r.CloserHelper.Close() {
		return nil
	}
	var lastErr error
	if err := r.bufferQueue.Close(ctx); err != nil {
		lastErr = err
	}
	return lastErr
}

// {{end}}
