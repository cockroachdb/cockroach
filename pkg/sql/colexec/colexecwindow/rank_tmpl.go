// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

//
// This file is the execgen template for rank.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexecwindow

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Remove unused warning.
var _ = colexecerror.InternalError

// TODO(yuzefovich): add benchmarks.

// NewRankOperator creates a new Operator that computes window functions RANK
// or DENSE_RANK (depending on the passed in windowFn).
// outputColIdx specifies in which coldata.Vec the operator should put its
// output (if there is no such column, a new column is appended).
func NewRankOperator(
	args *WindowArgs,
	windowFn execinfrapb.WindowerSpec_WindowFunc,
	orderingCols []execinfrapb.Ordering_Column,
) (colexecop.Operator, error) {
	if len(orderingCols) == 0 {
		return colexecbase.NewConstOp(
			args.MainAllocator, args.Input, types.Int, int64(1), args.OutputColIdx)
	}
	input := colexecutils.NewVectorTypeEnforcer(
		args.MainAllocator, args.Input, types.Int, args.OutputColIdx)
	initFields := rankInitFields{
		OneInputNode:    colexecop.NewOneInputNode(input),
		allocator:       args.MainAllocator,
		outputColIdx:    args.OutputColIdx,
		partitionColIdx: args.PartitionColIdx,
		peersColIdx:     args.PeersColIdx,
	}
	switch windowFn {
	case execinfrapb.WindowerSpec_RANK:
		if args.PartitionColIdx != tree.NoColumnIdx {
			return &rankWithPartitionOp{rankInitFields: initFields}, nil
		}
		return &rankNoPartitionOp{rankInitFields: initFields}, nil
	case execinfrapb.WindowerSpec_DENSE_RANK:
		if args.PartitionColIdx != tree.NoColumnIdx {
			return &denseRankWithPartitionOp{rankInitFields: initFields}, nil
		}
		return &denseRankNoPartitionOp{rankInitFields: initFields}, nil
	default:
		return nil, errors.AssertionFailedf("unsupported rank type %s", windowFn)
	}
}

// {{/*

// _UPDATE_RANK is the template function for updating the state of rank
// operators.
func _UPDATE_RANK() {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// _UPDATE_RANK_INCREMENT is the template function for updating the state of
// rank operators.
func _UPDATE_RANK_INCREMENT() {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

type rankInitFields struct {
	colexecop.OneInputNode
	colexecop.InitHelper

	allocator       *colmem.Allocator
	outputColIdx    int
	partitionColIdx int
	peersColIdx     int
}

// {{/*
// _COMPUTE_RANK is a code snippet that computes the rank for a single tuple at
// index i.
func _COMPUTE_RANK(_HAS_SEL bool) { // */}}
	// {{define "computeRank" -}}
	// {{if $.HasPartition}}
	// {{if not $.HasSel}}
	//gcassert:bce
	// {{end}}
	if partitionCol[i] {
		// We need to reset the internal state because of the new partition.
		// Note that the beginning of new partition necessarily starts a new
		// peer group, so peersCol[i] *must* be true, and we will correctly
		// update the rank before setting it to rankCol.
		r.rank = 0
		r.rankIncrement = 1
	}
	// {{end}}
	// {{if not $.HasSel}}
	//gcassert:bce
	// {{end}}
	if peersCol[i] {
		_UPDATE_RANK()
		// {{if not $.HasSel}}
		//gcassert:bce
		// {{end}}
		rankCol[i] = r.rank
	} else {
		// {{if not $.HasSel}}
		//gcassert:bce
		// {{end}}
		rankCol[i] = r.rank
		_UPDATE_RANK_INCREMENT()
	}
	// {{end}}
	// {{/*
} // */}}

// {{range .}}

type _RANK_STRINGOp struct {
	rankInitFields

	// rank indicates which rank should be assigned to the next tuple.
	rank int64
	// rankIncrement indicates by how much rank should be incremented when a
	// tuple distinct from the previous one on the ordering columns is seen.
	rankIncrement int64
}

var _ colexecop.Operator = &_RANK_STRINGOp{}

func (r *_RANK_STRINGOp) Init(ctx context.Context) {
	if !r.InitHelper.Init(ctx) {
		return
	}
	r.Input.Init(r.Ctx)
	// All rank functions start counting from 1. Before we assign the rank to a
	// tuple in the batch, we first increment r.rank, so setting this
	// rankIncrement to 1 will update r.rank to 1 on the very first tuple (as
	// desired).
	r.rankIncrement = 1
}

func (r *_RANK_STRINGOp) Next() coldata.Batch {
	batch := r.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	// {{if .HasPartition}}
	partitionCol := batch.ColVec(r.partitionColIdx).Bool()
	// {{end}}
	peersCol := batch.ColVec(r.peersColIdx).Bool()
	rankVec := batch.ColVec(r.outputColIdx)
	rankCol := rankVec.Int64()
	sel := batch.Selection()
	if sel != nil {
		for _, i := range sel[:n] {
			_COMPUTE_RANK(true)
		}
	} else {
		// {{if .HasPartition}}
		_ = partitionCol[n-1]
		// {{end}}
		_ = peersCol[n-1]
		_ = rankCol[n-1]
		for i := 0; i < n; i++ {
			_COMPUTE_RANK(false)
		}
	}
	return batch
}

// {{end}}
