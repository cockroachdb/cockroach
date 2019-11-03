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
// This file is the execgen template for percent_rank.eg.go. It's formatted in
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
)

// TODO(yuzefovich): add benchmarks.

type percentRankState int

const (
	// percentRankBuffering is the state in which percentRank operators fully
	// buffer their input. Once a zero-length batch is received, the operator
	// transitions to percentRankEmitting state.
	percentRankBuffering percentRankState = iota
	// percentRankEmitting is the state in which percentRank operators emit the
	// output. The output batch is populated by copying a "chunk" of all buffered
	// tuples and then populating the rank column.
	percentRankEmitting
)

// {{range .}}

type percentRank_STRINGOp struct {
	rankInitFields

	state percentRankState
	// rank indicates which rank should be assigned to the next tuple.
	rank int64
	// rankIncrement indicates by how much rank should be incremented when a
	// tuple distinct from the previous one on the ordering columns is seen.
	rankIncrement int64
	// bufferedTuples store all the buffered up tuples because percentRank needs
	// to know the number of tuples in the partition, so it needs to buffer up
	// the tuples before it can return the output.
	bufferedTuples *bufferedBatch
	// resumeTupleIdx stores the index of the first tuple to be emitted on the
	// next call to Next().
	resumeTupleIdx uint64
	// numTuplesInPartition contains the number of tuples in the current
	// partition.
	numTuplesInPartition uint64

	output coldata.Batch
}

var _ Operator = &percentRank_STRINGOp{}

func (r *percentRank_STRINGOp) Init() {
	r.Input().Init()
	r.state = percentRankBuffering
	// All rank functions start counting from 1. Before we assign the rank to a
	// tuple in the batch, we first increment r.rank, so setting this
	// rankIncrement to 1 will update r.rank to 1 on the very first tuple (as
	// desired).
	r.rankIncrement = 1
	r.bufferedTuples = newBufferedBatch(r.allocator, r.inputTypes, 0 /* initialSize */)
	r.output = r.allocator.NewMemBatch(append(r.inputTypes, coltypes.Float64))
}

func (r *percentRank_STRINGOp) Next(ctx context.Context) coldata.Batch {
	for {
		switch r.state {
		case percentRankBuffering:
			batch := r.Input().Next(ctx)
			n := batch.Length()
			if n == 0 {
				// We have fully consumed the input, so now we can populate the output.
				r.state = percentRankEmitting
				continue
			}
			// {{if .HasPartition}}
			// For simplicity, we will fully consume the input before we start
			// producing the output.
			// TODO(yuzefovich): we could be emitting output once we see that a new
			// partition has begun.
			// {{else}}
			// All rows belong to the same partition, so we need to fully consume the
			// input before we can proceed.
			// {{end}}
			r.allocator.PerformOperation(r.bufferedTuples.ColVecs(), func() {
				numBufferedTuples := r.bufferedTuples.length
				for colIdx, vec := range r.bufferedTuples.ColVecs() {
					vec.Append(
						coldata.SliceArgs{
							ColType:   r.inputTypes[colIdx],
							Src:       batch.ColVec(colIdx),
							Sel:       batch.Selection(),
							DestIdx:   numBufferedTuples,
							SrcEndIdx: uint64(n),
						},
					)
				}
				r.bufferedTuples.length += uint64(n)
			})
			continue

		case percentRankEmitting:
			if r.resumeTupleIdx == r.bufferedTuples.length {
				return coldata.ZeroBatch
			}
			r.output.ResetInternalBatch()
			toCopy := r.bufferedTuples.length - r.resumeTupleIdx
			if toCopy > uint64(coldata.BatchSize()) {
				toCopy = uint64(coldata.BatchSize())
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
			rankOutputCol := r.output.ColVec(r.outputColIdx).Float64()
			if r.bufferedTuples.length == 1 {
				// There is only a single tuple in the whole input, so we return
				// zero, per spec.
				rankOutputCol[0] = 0
			} else {
				// {{if .HasPartition}}
				partitionCol := r.bufferedTuples.ColVec(r.partitionColIdx).Bool()
				// {{end}}
				// We don't need to think about the selection vector since all the
				// buffered up tuples have been "deselected" during the buffering
				// stage.
				for i := range rankOutputCol[:toCopy] {
					// {{if .HasPartition}}
					if partitionCol[r.resumeTupleIdx+uint64(i)] {
						// We have encountered a start of a new partition. Before we can
						// populate the output for the consequent tuples, we'll need to
						// know the size of the partition.
						r.numTuplesInPartition = 1
						for j := r.resumeTupleIdx + uint64(i) + 1; j < r.bufferedTuples.length; j++ {
							if partitionCol[j] {
								break
							}
							r.numTuplesInPartition++
						}
						// We need to reset the internal state because of the new
						// partition.
						r.rank = 1
						r.rankIncrement = 1
						rankOutputCol[i] = 0
						continue
					}
					// {{else}}
					// There is a single partition in the whole input, so we already
					// know all the necessary information to populate the output.
					r.numTuplesInPartition = r.bufferedTuples.length
					// {{end}}
					if peersCol[r.resumeTupleIdx+uint64(i)] {
						r.rank += r.rankIncrement
						r.rankIncrement = 1
						rankOutputCol[i] = float64(r.rank-1) / float64(r.numTuplesInPartition-1)
					} else {
						rankOutputCol[i] = float64(r.rank-1) / float64(r.numTuplesInPartition-1)
						r.rankIncrement++
					}
				}
			}
			r.resumeTupleIdx += toCopy
			r.output.SetLength(uint16(toCopy))
			return r.output
		default:
			execerror.VectorizedInternalPanic("percent rank operator in unhandled state")
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
	}
}

// {{end}}
