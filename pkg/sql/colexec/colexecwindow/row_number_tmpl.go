// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

//
// This file is the execgen template for row_number.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexecwindow

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// TODO(yuzefovich): add benchmarks.

// NewRowNumberOperator creates a new Operator that computes window function
// ROW_NUMBER. outputColIdx specifies in which coldata.Vec the operator should
// put its output (if there is no such column, a new column is appended).
func NewRowNumberOperator(args *WindowArgs) colexecop.Operator {
	input := colexecutils.NewVectorTypeEnforcer(
		args.MainAllocator, args.Input, types.Int, args.OutputColIdx)
	base := rowNumberBase{
		OneInputHelper:  colexecop.MakeOneInputHelper(input),
		allocator:       args.MainAllocator,
		outputColIdx:    args.OutputColIdx,
		partitionColIdx: args.PartitionColIdx,
	}
	if args.PartitionColIdx == -1 {
		return &rowNumberNoPartitionOp{base}
	}
	return &rowNumberWithPartitionOp{base}
}

// rowNumberBase extracts common fields and common initialization of two
// variations of row number operators. Note that it is not an operator itself
// and should not be used directly.
type rowNumberBase struct {
	colexecop.OneInputHelper
	allocator       *colmem.Allocator
	outputColIdx    int
	partitionColIdx int

	rowNumber int64
}

// {{/*
// _COMPUTE_ROW_NUMBER is a code snippet that computes the row number value
// for a single tuple at index i as an increment from the previous tuple's row
// number. If a new partition begins, then the running 'rowNumber' variable is
// reset.
func _COMPUTE_ROW_NUMBER(_HAS_SEL bool) { // */}}
	// {{define "computeRowNumber" -}}
	// {{if $.HasPartition}}
	// {{if not $.HasSel}}
	//gcassert:bce
	// {{end}}
	if partitionCol[i] {
		r.rowNumber = 0
	}
	// {{end}}
	r.rowNumber++
	// {{if not $.HasSel}}
	//gcassert:bce
	// {{end}}
	rowNumberCol[i] = r.rowNumber
	// {{end}}
	// {{/*
} // */}}

// {{range .}}

type _ROW_NUMBER_STRINGOp struct {
	rowNumberBase
}

var _ colexecop.Operator = &_ROW_NUMBER_STRINGOp{}

func (r *_ROW_NUMBER_STRINGOp) Next() coldata.Batch {
	batch := r.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}

	// {{if .HasPartition}}
	partitionCol := batch.ColVec(r.partitionColIdx).Bool()
	// {{end}}
	rowNumberVec := batch.ColVec(r.outputColIdx)
	rowNumberCol := rowNumberVec.Int64()
	sel := batch.Selection()
	if sel != nil {
		for _, i := range sel[:n] {
			_COMPUTE_ROW_NUMBER(true)
		}
	} else {
		// {{if .HasPartition}}
		_ = partitionCol[n-1]
		// {{end}}
		_ = rowNumberCol[n-1]
		for i := 0; i < n; i++ {
			_COMPUTE_ROW_NUMBER(false)
		}
	}
	return batch
}

// {{end}}
