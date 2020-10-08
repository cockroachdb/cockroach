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
// This file is the execgen template for row_number.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// TODO(yuzefovich): add benchmarks.

// NewRowNumberOperator creates a new Operator that computes window function
// ROW_NUMBER. outputColIdx specifies in which coldata.Vec the operator should
// put its output (if there is no such column, a new column is appended).
func NewRowNumberOperator(
	allocator *colmem.Allocator, input colexecbase.Operator, outputColIdx int, partitionColIdx int,
) colexecbase.Operator {
	input = newVectorTypeEnforcer(allocator, input, types.Int, outputColIdx)
	base := rowNumberBase{
		OneInputNode:    NewOneInputNode(input),
		allocator:       allocator,
		outputColIdx:    outputColIdx,
		partitionColIdx: partitionColIdx,
	}
	if partitionColIdx == -1 {
		return &rowNumberNoPartitionOp{base}
	}
	return &rowNumberWithPartitionOp{base}
}

// rowNumberBase extracts common fields and common initialization of two
// variations of row number operators. Note that it is not an operator itself
// and should not be used directly.
type rowNumberBase struct {
	OneInputNode
	allocator       *colmem.Allocator
	outputColIdx    int
	partitionColIdx int

	rowNumber int64
}

func (r *rowNumberBase) Init() {
	r.Input().Init()
}

// {{/*
// _COMPUTE_ROW_NUMBER is a code snippet that computes the row number value
// for a single tuple at index i as an increment from the previous tuple's row
// number. If a new partition begins, then the running 'rowNumber' variable is
// reset.
func _COMPUTE_ROW_NUMBER() { // */}}
	// {{define "computeRowNumber" -}}
	// {{if $.HasPartition}}
	if partitionCol[i] {
		r.rowNumber = 0
	}
	// {{end}}
	r.rowNumber++
	rowNumberCol[i] = r.rowNumber
	// {{end}}
	// {{/*
} // */}}

// {{range .}}

type _ROW_NUMBER_STRINGOp struct {
	rowNumberBase
}

var _ colexecbase.Operator = &_ROW_NUMBER_STRINGOp{}

func (r *_ROW_NUMBER_STRINGOp) Next(ctx context.Context) coldata.Batch {
	batch := r.Input().Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}

	// {{if .HasPartition}}
	partitionCol := batch.ColVec(r.partitionColIdx).Bool()
	// {{end}}
	rowNumberVec := batch.ColVec(r.outputColIdx)
	if rowNumberVec.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		rowNumberVec.Nulls().UnsetNulls()
	}
	rowNumberCol := rowNumberVec.Int64()
	sel := batch.Selection()
	if sel != nil {
		for _, i := range sel[:n] {
			_COMPUTE_ROW_NUMBER()
		}
	} else {
		for i := range rowNumberCol[:n] {
			_COMPUTE_ROW_NUMBER()
		}
	}
	return batch
}

// {{end}}
