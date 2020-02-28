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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/errors"
)

// TODO(yuzefovich): add benchmarks.

// NewRelativeRankOperator creates a new Operator that computes window
// functions PERCENT_RANK or CUME_DIST (depending on the passed in windowFn).
// outputColIdx specifies in which coldata.Vec the operator should put its
// output (if there is no such column, a new column is appended).
func NewRelativeRankOperator(
	allocator *Allocator,
	input Operator,
	inputTypes []coltypes.T,
	windowFn execinfrapb.WindowerSpec_WindowFunc,
	orderingCols []execinfrapb.Ordering_Column,
	outputColIdx int,
	partitionColIdx int,
	peersColIdx int,
) (Operator, error) {
	if len(orderingCols) == 0 {
		constValue := float64(0)
		if windowFn == execinfrapb.WindowerSpec_CUME_DIST {
			constValue = 1
		}
		return NewConstOp(allocator, input, coltypes.Float64, constValue, outputColIdx)
	}
	initFields := rankInitFields{
		OneInputNode:    NewOneInputNode(input),
		allocator:       allocator,
		outputColIdx:    outputColIdx,
		partitionColIdx: partitionColIdx,
		peersColIdx:     peersColIdx,
	}
	switch windowFn {
	case execinfrapb.WindowerSpec_PERCENT_RANK:
		if partitionColIdx != columnOmitted {
			return &percentRankWithPartitionOp{
				rankInitFields: initFields,
				inputTypes:     inputTypes,
			}, nil
		}
		return &percentRankNoPartitionOp{
			rankInitFields: initFields,
			inputTypes:     inputTypes,
		}, nil
	case execinfrapb.WindowerSpec_CUME_DIST:
		if partitionColIdx != columnOmitted {
			return &cumeDistWithPartitionOp{
				rankInitFields: initFields,
				inputTypes:     inputTypes,
			}, nil
		}
		return &cumeDistNoPartitionOp{
			rankInitFields: initFields,
			inputTypes:     inputTypes,
		}, nil
	default:
		return nil, errors.Errorf("unsupported relative rank type %s", windowFn)
	}
}

type relativeRankState int

const (
	// relativeRankBuffering is the state in which relativeRank operators fully
	// buffer their input. Once a zero-length batch is received, the operator
	// transitions to relativeRankEmitting state.
	relativeRankBuffering relativeRankState = iota
	// relativeRankEmitting is the state in which relativeRank operators emit the
	// output. The output batch is populated by copying a "chunk" of all buffered
	// tuples and then populating the rank column.
	relativeRankEmitting
)

// {{/*
// _COMPUTE_NUM_PEERS is a code snippet that computes the number of tuples in
// the peer group. It should be called when a new peer group is encountered.
func _COMPUTE_NUM_PEERS() { // */}}
	// {{define "computeNumPeers" -}}
	r.numPeers = 1
	for j := r.resumeTupleIdx + i + 1; j < r.bufferedTuples.Length(); j++ {
		if peersCol[j] {
			break
		}
		r.numPeers++
	}
	// {{end}}
	// {{/*
} // */}}

// {{/*
// _COMPUTE_CUME_DIST is a code snippet that computes the value of cume_dist
// window function. The numbers of preceding tuples and of peers as well as
// the partition size *must* have already been computed.
func _COMPUTE_CUME_DIST() { // */}}
	// {{define "computeCumeDist" -}}
	relativeRankOutputCol[i] = float64(r.numPrecedingTuples+r.numPeers) / float64(r.numTuplesInPartition)
	// {{end}}
	// {{/*
} // */}}

// {{range .}}

type _RELATIVE_RANK_STRINGOp struct {
	rankInitFields

	state      relativeRankState
	inputTypes []coltypes.T
	// {{if .IsPercentRank}}
	// rank indicates which rank should be assigned to the next tuple.
	rank int64
	// rankIncrement indicates by how much rank should be incremented when a
	// tuple distinct from the previous one on the ordering columns is seen.
	rankIncrement int64
	// {{end}}
	// {{if .IsCumeDist}}
	// numPrecedingTuples stores the number of tuples preceding to the first
	// peer of the current tuple in the current partition. This number will be
	// computed once a new peer group is encountered.
	numPrecedingTuples int
	// numPeers stores the number of tuples that are peers with the current
	// tuple. This number will be computed once a new peer group is encountered.
	numPeers int
	// {{end}}
	// bufferedTuples store all the buffered up tuples because relativeRank needs
	// to know the number of tuples in the partition, so it needs to buffer up
	// the tuples before it can return the output.
	bufferedTuples coldata.Batch
	// resumeTupleIdx stores the index of the first tuple to be emitted on the
	// next call to Next().
	resumeTupleIdx int
	// numTuplesInPartition contains the number of tuples in the current
	// partition.
	numTuplesInPartition int

	output coldata.Batch
}

var _ Operator = &_RELATIVE_RANK_STRINGOp{}

func (r *_RELATIVE_RANK_STRINGOp) Init() {
	r.Input().Init()
	r.state = relativeRankBuffering
	r.bufferedTuples = r.allocator.NewMemBatchWithSize(r.inputTypes, 0 /* size */)
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
	for {
		switch r.state {
		case relativeRankBuffering:
			batch := r.Input().Next(ctx)
			n := batch.Length()
			if n == 0 {
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
			r.allocator.PerformOperation(r.bufferedTuples.ColVecs(), func() {
				numBufferedTuples := r.bufferedTuples.Length()
				for colIdx, vec := range r.bufferedTuples.ColVecs() {
					vec.Append(
						coldata.SliceArgs{
							ColType:   r.inputTypes[colIdx],
							Src:       batch.ColVec(colIdx),
							Sel:       batch.Selection(),
							DestIdx:   numBufferedTuples,
							SrcEndIdx: n,
						},
					)
				}
				r.bufferedTuples.SetLength(numBufferedTuples + n)
			})
			continue

		case relativeRankEmitting:
			if r.resumeTupleIdx == r.bufferedTuples.Length() {
				return coldata.ZeroBatch
			}
			r.output.ResetInternalBatch()
			toCopy := r.bufferedTuples.Length() - r.resumeTupleIdx
			if toCopy > coldata.BatchSize() {
				toCopy = coldata.BatchSize()
			}
			// First, we copy over the appropriate slices of all buffered up columns.
			r.allocator.PerformOperation(r.output.ColVecs()[:r.outputColIdx], func() {
				for colIdx, vec := range r.output.ColVecs()[:r.outputColIdx] {
					vec.Append(
						coldata.SliceArgs{
							ColType:     r.inputTypes[colIdx],
							Src:         r.bufferedTuples.ColVec(colIdx),
							DestIdx:     0,
							SrcStartIdx: r.resumeTupleIdx,
							SrcEndIdx:   r.resumeTupleIdx + toCopy,
						},
					)
				}
			})
			// Now we will populate the output column.
			peersCol := r.bufferedTuples.ColVec(r.peersColIdx).Bool()
			relativeRankOutputCol := r.output.ColVec(r.outputColIdx).Float64()
			if r.bufferedTuples.Length() == 1 {
				// {{if .IsPercentRank}}
				// There is only a single tuple in the whole input, so we return
				// zero, per spec.
				relativeRankOutputCol[0] = 0
				// {{end}}
				// {{if .IsCumeDist}}
				// There is only a single tuple in the whole input, so we return 1.
				// {{/*
				// Note: we could have avoided special casing in this scenario, but
				// that would make the template less pleasing to the eye.
				// */}}
				relativeRankOutputCol[0] = 1
				// {{end}}
			} else {
				// {{if .HasPartition}}
				partitionCol := r.bufferedTuples.ColVec(r.partitionColIdx).Bool()
				// {{end}}
				// We don't need to think about the selection vector since all the
				// buffered up tuples have been "deselected" during the buffering
				// stage.
				for i := range relativeRankOutputCol[:toCopy] {
					// {{if .HasPartition}}
					if partitionCol[r.resumeTupleIdx+i] {
						// We have encountered a start of a new partition. Before we can
						// populate the output for the consequent tuples, we'll need to
						// know the size of the partition.
						r.numTuplesInPartition = 1
						for j := r.resumeTupleIdx + i + 1; j < r.bufferedTuples.Length(); j++ {
							if partitionCol[j] {
								break
							}
							r.numTuplesInPartition++
						}
						// {{if .IsPercentRank}}
						// We need to reset the internal state because of the new
						// partition.
						r.rank = 1
						r.rankIncrement = 1
						relativeRankOutputCol[i] = 0
						// {{end}}
						// {{if .IsCumeDist}}
						// We need to reset the number of preceding tuples because of the
						// new partition.
						r.numPrecedingTuples = 0
						// Because we have encountered a start of a new partition, we have
						// encountered a new peer group as well, and we need to compute the
						// number of tuples in this peer group.
						_COMPUTE_NUM_PEERS()
						_COMPUTE_CUME_DIST()
						// {{end}}
						continue
					}
					// {{else}}
					// There is a single partition in the whole input, so we already
					// know all the necessary information to populate the output.
					r.numTuplesInPartition = r.bufferedTuples.Length()
					// {{end}}
					if peersCol[r.resumeTupleIdx+i] {
						// {{if .IsPercentRank}}
						r.rank += r.rankIncrement
						r.rankIncrement = 0
						// {{end}}
						// {{if .IsCumeDist}}
						// We have encountered a new peer group, and we need to update the
						// number of preceding tuples and compute the number of tuples in
						// this peer group.
						r.numPrecedingTuples += r.numPeers
						_COMPUTE_NUM_PEERS()
						// {{end}}
					}
					// {{if .IsPercentRank}}
					relativeRankOutputCol[i] = float64(r.rank-1) / float64(r.numTuplesInPartition-1)
					r.rankIncrement++
					// {{end}}
					// {{if .IsCumeDist}}
					_COMPUTE_CUME_DIST()
					// {{end}}
				}
			}
			r.resumeTupleIdx += toCopy
			r.output.SetLength(toCopy)
			return r.output
		default:
			execerror.VectorizedInternalPanic("percent rank operator in unhandled state")
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
	}
}

// {{end}}
