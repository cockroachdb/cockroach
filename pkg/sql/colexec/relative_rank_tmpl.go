// Copyright 2020 The Cockroach Authors.
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
// This file is the execgen template for relative_rank.eg.go. It's formatted in
// a special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// TODO(yuzefovich): add benchmarks.

// NewRelativeRankOperator creates a new Operator that computes window
// functions PERCENT_RANK or CUME_DIST (depending on the passed in windowFn).
// outputColIdx specifies in which coldata.Vec the operator should put its
// output (if there is no such column, a new column is appended).
func NewRelativeRankOperator(
	unlimitedAllocator *Allocator,
	memoryLimit int64,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	input Operator,
	inputTypes []coltypes.T,
	windowFn execinfrapb.WindowerSpec_WindowFunc,
	orderingCols []execinfrapb.Ordering_Column,
	outputColIdx int,
	partitionColIdx int,
	peersColIdx int,
	diskAcc *mon.BoundAccount,
) (Operator, error) {
	if len(orderingCols) == 0 {
		constValue := float64(0)
		if windowFn == execinfrapb.WindowerSpec_CUME_DIST {
			constValue = 1
		}
		return NewConstOp(unlimitedAllocator, input, coltypes.Float64, constValue, outputColIdx)
	}
	rrInitFields := relativeRankInitFields{
		rankInitFields: rankInitFields{
			OneInputNode:    NewOneInputNode(input),
			allocator:       unlimitedAllocator,
			outputColIdx:    outputColIdx,
			partitionColIdx: partitionColIdx,
			peersColIdx:     peersColIdx,
		},
		memoryLimit:  memoryLimit,
		diskQueueCfg: diskQueueCfg,
		fdSemaphore:  fdSemaphore,
		inputTypes:   inputTypes,
		diskAcc:      diskAcc,
	}
	switch windowFn {
	case execinfrapb.WindowerSpec_PERCENT_RANK:
		if partitionColIdx != columnOmitted {
			return &percentRankWithPartitionOp{
				relativeRankInitFields: rrInitFields,
			}, nil
		}
		return &percentRankNoPartitionOp{
			relativeRankInitFields: rrInitFields,
		}, nil
	case execinfrapb.WindowerSpec_CUME_DIST:
		if partitionColIdx != columnOmitted {
			return &cumeDistWithPartitionOp{
				relativeRankInitFields: rrInitFields,
			}, nil
		}
		return &cumeDistNoPartitionOp{
			relativeRankInitFields: rrInitFields,
		}, nil
	default:
		return nil, errors.Errorf("unsupported relative rank type %s", windowFn)
	}
}

// NOTE: in the context of window functions "partitions" mean a different thing
// from "partition" in the context of external algorithms and some disk
// infrastructure: here, "partitions" are sets of tuples that are not distinct
// on the columns specified in PARTITION BY clause of the window function. If
// such clause is omitted, then all tuples from the input belong to the same
// partition.

type relativeRankState int

const (
	// relativeRankBuffering is the state in which relativeRank operators fully
	// buffer their input using spillingQueue. Additionally, the operators will
	// be computing the sizes of the partitions and peer groups (if needed)
	// using separate spillingQueues for each. Once a zero-length batch is
	// received, the operator transitions to relativeRankEmitting state.
	relativeRankBuffering relativeRankState = iota
	// relativeRankEmitting is the state in which relativeRank operators emit
	// the output. The output batch is populated by copying the next batch from
	// the "buffered tuples" spilling queue and manually computing the output
	// column for the window function using the already computed sizes of
	// partitions and peer groups. Once a zero-length batch is dequeued from
	// the "buffered tuples" queue, the operator transitions to
	// relativeRankFinished state.
	relativeRankEmitting
	// relativeRankFinished is the state in which relativeRank operators close
	// any non-closed disk resources and emit the zero-length batch.
	relativeRankFinished
)

// {{/*
// _COMPUTE_PARTITIONS_SIZES is a code snippet that computes the sizes of
// partitions. It looks at i'th partitionCol value to check whether a new
// partition begins at index i, and if so, it records the already computed
// size of the previous partition into partitionsState.runningSizes vector.
func _COMPUTE_PARTITIONS_SIZES() { // */}}
	// {{define "computePartitionsSizes" -}}
	if partitionCol[i] {
		// We have encountered a start of a new partition, so we
		// need to save the computed size of the previous one
		// (if there was one).
		if r.partitionsState.runningSizes == nil {
			// TODO(yuzefovich): do not instantiate a new batch here once
			// spillingQueues actually copy the batches when those are kept
			// in-memory.
			r.partitionsState.runningSizes = r.allocator.NewMemBatch([]coltypes.T{coltypes.Int64})
			runningPartitionsSizesCol = r.partitionsState.runningSizes.ColVec(0).Int64()
		}
		if r.numTuplesInPartition > 0 {
			runningPartitionsSizesCol[r.partitionsState.idx] = r.numTuplesInPartition
			r.numTuplesInPartition = 0
			r.partitionsState.idx++
			if r.partitionsState.idx == coldata.BatchSize() {
				// We need to flush the vector of partitions sizes.
				r.partitionsState.runningSizes.SetLength(coldata.BatchSize())
				if err := r.partitionsState.enqueue(ctx, r.partitionsState.runningSizes); err != nil {
					execerror.VectorizedInternalPanic(err)
				}
				r.partitionsState.runningSizes = nil
				r.partitionsState.idx = 0
			}
		}
	}
	r.numTuplesInPartition++
	// {{end}}
	// {{/*
} // */}}

// {{/*
// _COMPUTE_PEER_GROUPS_SIZES is a code snippet that computes the sizes of
// peer groups. It looks at i'th peersCol value to check whether a new
// peer group begins at index i, and if so, it records the already computed
// size of the previous peer group into peerGroupsState.runningSizes vector.
func _COMPUTE_PEER_GROUPS_SIZES() { // */}}
	// {{define "computePeerGroupsSizes" -}}
	if peersCol[i] {
		// We have encountered a start of a new peer group, so we
		// need to save the computed size of the previous one
		// (if there was one).
		if r.peerGroupsState.runningSizes == nil {
			// TODO(yuzefovich): do not instantiate a new batch here once
			// spillingQueues actually copy the batches when those are kept
			// in-memory.
			r.peerGroupsState.runningSizes = r.allocator.NewMemBatch([]coltypes.T{coltypes.Int64})
			runningPeerGroupsSizesCol = r.peerGroupsState.runningSizes.ColVec(0).Int64()
		}
		if r.numPeers > 0 {
			runningPeerGroupsSizesCol[r.peerGroupsState.idx] = r.numPeers
			r.numPeers = 0
			r.peerGroupsState.idx++
			if r.peerGroupsState.idx == coldata.BatchSize() {
				// We need to flush the vector of peer group sizes.
				r.peerGroupsState.runningSizes.SetLength(coldata.BatchSize())
				if err := r.peerGroupsState.enqueue(ctx, r.peerGroupsState.runningSizes); err != nil {
					execerror.VectorizedInternalPanic(err)
				}
				r.peerGroupsState.runningSizes = nil
				r.peerGroupsState.idx = 0
			}
		}
	}
	r.numPeers++
	// {{end}}
	// {{/*
} // */}}

type relativeRankInitFields struct {
	rankInitFields

	closed       bool
	state        relativeRankState
	memoryLimit  int64
	diskQueueCfg colcontainer.DiskQueueCfg
	fdSemaphore  semaphore.Semaphore
	inputTypes   []coltypes.T

	diskAcc *mon.BoundAccount
}

type relativeRankSizesState struct {
	*spillingQueue

	// runningSizes is a batch consisting of a single int64 vector that stores
	// sizes while we're computing them. Once all coldata.BatchSize() slots are
	// filled, it will be flushed to the spillingQueue.
	runningSizes coldata.Batch
	// dequeuedSizes is a batch of already computed sizes that is dequeued
	// from the spillingQueue.
	dequeuedSizes coldata.Batch
	// idx stores the index of the current slot in one of the batches above
	// that we're currently working with.
	idx int
}

// relativeRankUtilityQueueMemLimitFraction defines the fraction of the memory
// limit that will be given to the "utility" spillingQueues of relativeRank
// operators (i.e. non "buffered tuples" queues).
const relativeRankUtilityQueueMemLimitFraction = 0.1

// {{range .}}

type _RELATIVE_RANK_STRINGOp struct {
	relativeRankInitFields

	// {{if .IsPercentRank}}
	// rank indicates which rank should be assigned to the next tuple.
	rank int64
	// rankIncrement indicates by how much rank should be incremented when a
	// tuple distinct from the previous one on the ordering columns is seen.
	rankIncrement int64
	// {{end}}

	// {{if .IsCumeDist}}
	peerGroupsState relativeRankSizesState
	// numPrecedingTuples stores the number of tuples preceding to the first
	// peer of the current tuple in the current partition.
	numPrecedingTuples int64
	// numPeers stores the number of tuples that are peers with the current
	// tuple.
	numPeers int64
	// {{end}}

	// {{if .HasPartition}}
	partitionsState relativeRankSizesState
	// {{end}}
	// numTuplesInPartition contains the number of tuples in the current
	// partition.
	numTuplesInPartition int64

	bufferedTuples *spillingQueue
	scratch        coldata.Batch
	output         coldata.Batch
}

var _ closableOperator = &_RELATIVE_RANK_STRINGOp{}

func (r *_RELATIVE_RANK_STRINGOp) Init() {
	r.Input().Init()
	r.state = relativeRankBuffering
	usedMemoryLimitFraction := 0.0
	// {{if .HasPartition}}
	r.partitionsState.spillingQueue = newSpillingQueue(
		r.allocator, []coltypes.T{coltypes.Int64},
		int64(float64(r.memoryLimit)*relativeRankUtilityQueueMemLimitFraction),
		r.diskQueueCfg, r.fdSemaphore, coldata.BatchSize(), r.diskAcc,
	)
	usedMemoryLimitFraction += relativeRankUtilityQueueMemLimitFraction
	// {{end}}
	// {{if .IsCumeDist}}
	r.peerGroupsState.spillingQueue = newSpillingQueue(
		r.allocator, []coltypes.T{coltypes.Int64},
		int64(float64(r.memoryLimit)*relativeRankUtilityQueueMemLimitFraction),
		r.diskQueueCfg, r.fdSemaphore, coldata.BatchSize(), r.diskAcc,
	)
	usedMemoryLimitFraction += relativeRankUtilityQueueMemLimitFraction
	// {{end}}
	r.bufferedTuples = newSpillingQueue(
		r.allocator, r.inputTypes,
		int64(float64(r.memoryLimit)*(1.0-usedMemoryLimitFraction)),
		r.diskQueueCfg, r.fdSemaphore, coldata.BatchSize(), r.diskAcc,
	)
	r.output = r.allocator.NewMemBatch(append(r.inputTypes, coltypes.Float64))
	// {{if .IsPercentRank}}
	// All rank functions start counting from 1. Before we assign the rank to a
	// tuple in the batch, we first increment r.rank, so setting this
	// rankIncrement to 1 will update r.rank to 1 on the very first tuple (as
	// desired).
	r.rankIncrement = 1
	// {{end}}
}

func (r *_RELATIVE_RANK_STRINGOp) Next(ctx context.Context) coldata.Batch {
	var err error
	for {
		switch r.state {
		case relativeRankBuffering:
			// The outline of what we need to do in "buffering" state:
			//
			// 1. we need to buffer the tuples that we read from the input.
			// These are simply copied into r.bufferedTuples spillingQueue.
			//
			// 2. (if we have PARTITION BY clause) we need to compute the sizes of
			// partitions. These sizes are stored in r.partitionsState.runningSizes
			// batch (that consists of a single vector) and r.partitionsState.idx
			// points at the next slot in that vector to write to. Once it
			// reaches coldata.BatchSize(), the batch is "flushed" to the
			// corresponding spillingQueue. The "running" value of the current
			// partition size is stored in r.numTuplesInPartition.
			//
			// 3. (if we have CUME_DIST function) we need to compute the sizes
			// of peer groups. These sizes are stored in r.peerGroupsState.runningSizes
			// batch (that consists of a single vector) and r.peerGroupsState.idx
			// points at the next slot in that vector to write to. Once it
			// reaches coldata.BatchSize(), the batch is "flushed" to the
			// corresponding spillingQueue. The "running" value of the current
			// peer group size is stored in r.numPeers.
			//
			// For example, if we have the following setup:
			//   partitionCol = {true, false, false, true, false, false, false, false}
			//   peersCol     = {true, false, true, true, false, false, true, false}
			// we want this as the result:
			//   partitionsSizes = {3, 5}
			//   peerGroupsSizes = {2, 1, 3, 2}.
			// This example also shows why we need to use two different queues
			// (since every partition can have multiple peer groups, the
			// schedule of "flushing" is different).
			batch := r.Input().Next(ctx)
			n := batch.Length()
			if n == 0 {
				if err := r.bufferedTuples.enqueue(ctx, coldata.ZeroBatch); err != nil {
					execerror.VectorizedInternalPanic(err)
				}
				// {{if .HasPartition}}
				// We need to flush the last vector of the running partitions
				// sizes, including the very last partition.
				if r.partitionsState.runningSizes == nil {
					// TODO(yuzefovich): do not instantiate a new batch here once
					// spillingQueues actually copy the batches when those are kept
					// in-memory.
					r.partitionsState.runningSizes = r.allocator.NewMemBatch([]coltypes.T{coltypes.Int64})
				}
				runningPartitionsSizesCol := r.partitionsState.runningSizes.ColVec(0).Int64()
				runningPartitionsSizesCol[r.partitionsState.idx] = r.numTuplesInPartition
				r.partitionsState.idx++
				r.partitionsState.runningSizes.SetLength(r.partitionsState.idx)
				if err := r.partitionsState.enqueue(ctx, r.partitionsState.runningSizes); err != nil {
					execerror.VectorizedInternalPanic(err)
				}
				if err := r.partitionsState.enqueue(ctx, coldata.ZeroBatch); err != nil {
					execerror.VectorizedInternalPanic(err)
				}
				// {{end}}
				// {{if .IsCumeDist}}
				// We need to flush the last vector of the running peer groups
				// sizes, including the very last peer group.
				if r.peerGroupsState.runningSizes == nil {
					// TODO(yuzefovich): do not instantiate a new batch here once
					// spillingQueues actually copy the batches when those are kept
					// in-memory.
					r.peerGroupsState.runningSizes = r.allocator.NewMemBatch([]coltypes.T{coltypes.Int64})
				}
				runningPeerGroupsSizesCol := r.peerGroupsState.runningSizes.ColVec(0).Int64()
				runningPeerGroupsSizesCol[r.peerGroupsState.idx] = r.numPeers
				r.peerGroupsState.idx++
				r.peerGroupsState.runningSizes.SetLength(r.peerGroupsState.idx)
				if err := r.peerGroupsState.enqueue(ctx, r.peerGroupsState.runningSizes); err != nil {
					execerror.VectorizedInternalPanic(err)
				}
				if err := r.peerGroupsState.enqueue(ctx, coldata.ZeroBatch); err != nil {
					execerror.VectorizedInternalPanic(err)
				}
				// {{end}}
				// We have fully consumed the input, so now we can populate the output.
				r.state = relativeRankEmitting
				continue
			}

			// {{if .HasPartition}}
			// For simplicity, we will fully consume the input before we start
			// producing the output.
			// TODO(yuzefovich): we could be emitting output once we see that a new
			// partition has begun.
			// {{else}}
			// All tuples belong to the same partition, so we need to fully consume
			// the input before we can proceed.
			// {{end}}

			sel := batch.Selection()
			// First, we buffer up all of the tuples.
			// TODO(yuzefovich): do not instantiate a new batch here once
			// spillingQueues actually copy the batches when those are kept
			// in-memory.
			r.scratch = r.allocator.NewMemBatchWithSize(r.inputTypes, n)
			r.allocator.PerformOperation(r.scratch.ColVecs(), func() {
				for colIdx, vec := range r.scratch.ColVecs() {
					vec.Append(
						coldata.SliceArgs{
							ColType:   r.inputTypes[colIdx],
							Src:       batch.ColVec(colIdx),
							Sel:       sel,
							SrcEndIdx: n,
						},
					)
				}
				r.scratch.SetLength(n)
			})
			if err := r.bufferedTuples.enqueue(ctx, r.scratch); err != nil {
				execerror.VectorizedInternalPanic(err)
			}

			// Then, we need to update the sizes of the partitions.
			// {{if .HasPartition}}
			partitionCol := batch.ColVec(r.partitionColIdx).Bool()
			var runningPartitionsSizesCol []int64
			if sel != nil {
				for _, i := range sel[:n] {
					_COMPUTE_PARTITIONS_SIZES()
				}
			} else {
				for i := 0; i < n; i++ {
					_COMPUTE_PARTITIONS_SIZES()
				}
			}
			// {{else}}
			// There is a single partition in the whole input.
			r.numTuplesInPartition += int64(n)
			// {{end}}

			// {{if .IsCumeDist}}
			// Next, we need to update the sizes of the peer groups.
			peersCol := batch.ColVec(r.peersColIdx).Bool()
			var runningPeerGroupsSizesCol []int64
			if sel != nil {
				for _, i := range sel[:n] {
					_COMPUTE_PEER_GROUPS_SIZES()
				}
			} else {
				for i := 0; i < n; i++ {
					_COMPUTE_PEER_GROUPS_SIZES()
				}
			}
			// {{end}}
			continue

		case relativeRankEmitting:
			if r.scratch, err = r.bufferedTuples.dequeue(ctx); err != nil {
				execerror.VectorizedInternalPanic(err)
			}
			n := r.scratch.Length()
			if n == 0 {
				r.state = relativeRankFinished
				continue
			}
			// {{if .HasPartition}}
			// Get the next batch of partition sizes if we haven't already.
			if r.partitionsState.dequeuedSizes == nil {
				if r.partitionsState.dequeuedSizes, err = r.partitionsState.dequeue(ctx); err != nil {
					execerror.VectorizedInternalPanic(err)
				}
				r.partitionsState.idx = 0
				r.numTuplesInPartition = 0
			}
			// {{end}}
			// {{if .IsCumeDist}}
			// Get the next batch of peer group sizes if we haven't already.
			if r.peerGroupsState.dequeuedSizes == nil {
				if r.peerGroupsState.dequeuedSizes, err = r.peerGroupsState.dequeue(ctx); err != nil {
					execerror.VectorizedInternalPanic(err)
				}
				r.peerGroupsState.idx = 0
				r.numPeers = 0
			}
			// {{end}}

			r.output.ResetInternalBatch()
			// First, we copy over the buffered up columns.
			r.allocator.PerformOperation(r.output.ColVecs()[:r.outputColIdx], func() {
				for colIdx, vec := range r.output.ColVecs()[:r.outputColIdx] {
					vec.Append(
						coldata.SliceArgs{
							ColType:   r.inputTypes[colIdx],
							Src:       r.scratch.ColVec(colIdx),
							SrcEndIdx: n,
						},
					)
				}
			})

			// Now we will populate the output column.
			relativeRankOutputCol := r.output.ColVec(r.outputColIdx).Float64()
			// {{if .HasPartition}}
			partitionCol := r.scratch.ColVec(r.partitionColIdx).Bool()
			// {{end}}
			peersCol := r.scratch.ColVec(r.peersColIdx).Bool()
			// We don't need to think about the selection vector since all the
			// buffered up tuples have been "deselected" during the buffering
			// stage.
			for i := range relativeRankOutputCol[:n] {
				// We need to set r.numTuplesInPartition to the size of the
				// partition that i'th tuple belongs to (which we have already
				// computed).
				// {{if .HasPartition}}
				if partitionCol[i] {
					if r.partitionsState.idx == r.partitionsState.dequeuedSizes.Length() {
						if r.partitionsState.dequeuedSizes, err = r.partitionsState.dequeue(ctx); err != nil {
							execerror.VectorizedInternalPanic(err)
						}
						r.partitionsState.idx = 0
					}
					r.numTuplesInPartition = r.partitionsState.dequeuedSizes.ColVec(0).Int64()[r.partitionsState.idx]
					r.partitionsState.idx++
					// {{if .IsPercentRank}}
					// We need to reset the internal state because of the new
					// partition.
					r.rank = 0
					r.rankIncrement = 1
					// {{end}}
					// {{if .IsCumeDist}}
					// We need to reset the number of preceding tuples because of the
					// new partition.
					r.numPrecedingTuples = 0
					r.numPeers = 0
					// {{end}}
				}
				// {{else}}
				// There is a single partition in the whole input, and
				// r.numTuplesInPartition already contains the correct number.
				// {{end}}

				if peersCol[i] {
					// {{if .IsPercentRank}}
					r.rank += r.rankIncrement
					r.rankIncrement = 0
					// {{end}}
					// {{if .IsCumeDist}}
					// We have encountered a new peer group, and we need to update the
					// number of preceding tuples and get the number of tuples in
					// this peer group.
					r.numPrecedingTuples += r.numPeers
					if r.peerGroupsState.idx == r.peerGroupsState.dequeuedSizes.Length() {
						if r.peerGroupsState.dequeuedSizes, err = r.peerGroupsState.dequeue(ctx); err != nil {
							execerror.VectorizedInternalPanic(err)
						}
						r.peerGroupsState.idx = 0
					}
					r.numPeers = r.peerGroupsState.dequeuedSizes.ColVec(0).Int64()[r.peerGroupsState.idx]
					r.peerGroupsState.idx++
					// {{end}}
				}

				// Now we can compute the value of the window function for i'th
				// tuple.
				// {{if .IsPercentRank}}
				if r.numTuplesInPartition == 1 {
					// There is a single tuple in the partition, so we return 0, per spec.
					relativeRankOutputCol[i] = 0
				} else {
					relativeRankOutputCol[i] = float64(r.rank-1) / float64(r.numTuplesInPartition-1)
				}
				r.rankIncrement++
				// {{end}}
				// {{if .IsCumeDist}}
				relativeRankOutputCol[i] = float64(r.numPrecedingTuples+r.numPeers) / float64(r.numTuplesInPartition)
				// {{end}}
			}
			r.output.SetLength(n)
			return r.output

		case relativeRankFinished:
			if err := r.Close(ctx); err != nil {
				execerror.VectorizedInternalPanic(err)
			}
			return coldata.ZeroBatch

		default:
			execerror.VectorizedInternalPanic("percent rank operator in unhandled state")
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
	}
}

func (r *_RELATIVE_RANK_STRINGOp) Close(ctx context.Context) error {
	if r.closed {
		return nil
	}
	var lastErr error
	if err := r.bufferedTuples.close(ctx); err != nil {
		lastErr = err
	}
	// {{if .HasPartition}}
	if err := r.partitionsState.close(ctx); err != nil {
		lastErr = err
	}
	// {{end}}
	// {{if .IsCumeDist}}
	if err := r.peerGroupsState.close(ctx); err != nil {
		lastErr = err
	}
	// {{end}}
	r.closed = true
	return lastErr
}

// {{end}}
