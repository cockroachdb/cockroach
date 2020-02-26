// Copyright 2019 The Cockroach Authors.
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
// This file is the execgen template for rank.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
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

// Use execerror package to remove unused import warning.
var _ = execerror.VectorizedInternalPanic

// TODO(yuzefovich): add benchmarks.

// NewRankOperator creates a new Operator that computes window function RANK or
// DENSE_RANK. dense distinguishes between the two functions. input *must*
// already be ordered on orderingCols (which should not be empty). outputColIdx
// specifies in which coldata.Vec the operator should put its output (if there
// is no such column, a new column is appended).
func NewRankOperator(
	allocator *Allocator,
	input Operator,
	inputTyps []coltypes.T,
	dense bool,
	orderingCols []uint32,
	outputColIdx int,
	partitionColIdx int,
) (Operator, error) {
	if len(orderingCols) == 0 {
		return NewConstOp(allocator, input, coltypes.Int64, int64(1), outputColIdx)
	}
	op, outputCol, err := OrderedDistinctColsToOperators(input, orderingCols, inputTyps)
	if err != nil {
		return nil, err
	}
	initFields := rankInitFields{
		OneInputNode:    NewOneInputNode(op),
		allocator:       allocator,
		distinctCol:     outputCol,
		outputColIdx:    outputColIdx,
		partitionColIdx: partitionColIdx,
	}
	if dense {
		if partitionColIdx != -1 {
			return &denseRankWithPartitionOp{rankInitFields: initFields}, nil
		}
		return &denseRankNoPartitionOp{rankInitFields: initFields}, nil
	}
	if partitionColIdx != -1 {
		return &rankWithPartitionOp{rankInitFields: initFields}, nil
	}
	return &rankNoPartitionOp{rankInitFields: initFields}, nil
}

// {{/*

// _UPDATE_RANK is the template function for updating the state of rank
// operators.
func _UPDATE_RANK() {
	execerror.VectorizedInternalPanic("")
}

// _UPDATE_RANK_INCREMENT is the template function for updating the state of
// rank operators.
func _UPDATE_RANK_INCREMENT() {
	execerror.VectorizedInternalPanic("")
}

// */}}

type rankInitFields struct {
	OneInputNode
	allocator *Allocator
	// distinctCol is the output column of the chain of ordered distinct
	// operators in which true will indicate that a new rank needs to be assigned
	// to the corresponding tuple.
	distinctCol     []bool
	outputColIdx    int
	partitionColIdx int
}

// {{range .}}

type _RANK_STRINGOp struct {
	rankInitFields

	// rank indicates which rank should be assigned to the next tuple.
	rank int64
	// rankIncrement indicates by how much rank should be incremented when a
	// tuple distinct from the previous one on the ordering columns is seen. It
	// is used only in case of a regular rank function (i.e. not dense).
	rankIncrement int64
}

var _ Operator = &_RANK_STRINGOp{}

func (r *_RANK_STRINGOp) Init() {
	r.Input().Init()
	// RANK and DENSE_RANK start counting from 1. Before we assign the rank to a
	// tuple in the batch, we first increment r.rank, so setting this
	// rankIncrement to 1 will update r.rank to 1 on the very first tuple (as
	// desired).
	r.rankIncrement = 1
}

func (r *_RANK_STRINGOp) Next(ctx context.Context) coldata.Batch {
	batch := r.Input().Next(ctx)
	if batch.Length() == 0 {
		return coldata.ZeroBatch
	}
	// {{ if .HasPartition }}
	r.allocator.MaybeAddColumn(batch, coltypes.Bool, r.partitionColIdx)
	// {{ end }}
	r.allocator.MaybeAddColumn(batch, coltypes.Int64, r.outputColIdx)

	// {{ if .HasPartition }}
	partitionCol := batch.ColVec(r.partitionColIdx).Bool()
	// {{ end }}
	rankCol := batch.ColVec(r.outputColIdx).Int64()
	sel := batch.Selection()
	// TODO(yuzefovich): template out sel vs non-sel cases.
	if sel != nil {
		for i := uint16(0); i < batch.Length(); i++ {
			// {{ if .HasPartition }}
			if partitionCol[sel[i]] {
				r.rank = 1
				r.rankIncrement = 1
				rankCol[sel[i]] = 1
				continue
			}
			// {{end}}
			if r.distinctCol[sel[i]] {
				_UPDATE_RANK()
				rankCol[sel[i]] = r.rank
			} else {
				rankCol[sel[i]] = r.rank
				_UPDATE_RANK_INCREMENT()
			}
		}
	} else {
		for i := uint16(0); i < batch.Length(); i++ {
			// {{ if .HasPartition }}
			if partitionCol[i] {
				r.rank = 1
				r.rankIncrement = 1
				rankCol[i] = 1
				continue
			}
			// {{end}}
			if r.distinctCol[i] {
				_UPDATE_RANK()
				rankCol[i] = r.rank
			} else {
				rankCol[i] = r.rank
				_UPDATE_RANK_INCREMENT()
			}
		}
	}
	return batch
}

// {{end}}
