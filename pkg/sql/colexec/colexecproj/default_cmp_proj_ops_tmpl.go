// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

//
// This file is the execgen template for default_cmp_proj_op.eg.go and
// default_cmp_proj_const_op.eg.go. It's formatted in a special way, so it's
// both valid Go and a valid text/template input. This permits editing this file
// with editor support.
//
// */}}

package colexecproj

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexeccmp"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execreleasable"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// {{define "defaultCmpProjOp"}}

type defaultCmp_KINDProjOp struct {
	// {{if .IsRightConst}}
	projConstOpBase
	constArg tree.Datum
	// {{else}}
	projOpBase
	// {{end}}

	adapter             colexeccmp.ComparisonExprAdapter
	toDatumConverter    *colconv.VecToDatumConverter
	datumToVecConverter func(tree.Datum) interface{}
}

var _ colexecop.Operator = &defaultCmp_KINDProjOp{}
var _ execreleasable.Releasable = &defaultCmp_KINDProjOp{}

func (d *defaultCmp_KINDProjOp) Next() coldata.Batch {
	batch := d.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	sel := batch.Selection()
	output := batch.ColVec(d.outputIdx)
	d.allocator.PerformOperation([]*coldata.Vec{output}, func() {
		d.toDatumConverter.ConvertBatchAndDeselect(batch)
		// {{if .IsRightConst}}
		nonConstColumn := d.toDatumConverter.GetDatumColumn(d.colIdx)
		_ = nonConstColumn[n-1]
		// {{else}}
		leftColumn := d.toDatumConverter.GetDatumColumn(d.col1Idx)
		rightColumn := d.toDatumConverter.GetDatumColumn(d.col2Idx)
		_ = leftColumn[n-1]
		_ = rightColumn[n-1]
		// {{end}}
		if sel != nil {
			_ = sel[n-1]
		}
		for i := 0; i < n; i++ {
			// Note that we performed a conversion with deselection, so there
			// is no need to check whether sel is non-nil.
			// {{if .IsRightConst}}
			//gcassert:bce
			res, err := d.adapter.Eval(d.Ctx, nonConstColumn[i], d.constArg)
			// {{else}}
			//gcassert:bce
			res, err := d.adapter.Eval(d.Ctx, leftColumn[i], rightColumn[i])
			// {{end}}
			if err != nil {
				colexecerror.ExpectedError(err)
			}
			rowIdx := i
			if sel != nil {
				rowIdx = sel[i]
			}
			// Convert the datum into a physical type and write it out.
			// TODO(yuzefovich): this code block is repeated in several places.
			// Refactor it.
			if res == tree.DNull {
				output.Nulls().SetNull(rowIdx)
			} else {
				converted := d.datumToVecConverter(res)
				coldata.SetValueAt(output, converted, rowIdx)
			}
		}
	})
	return batch
}

func (d *defaultCmp_KINDProjOp) Release() {
	d.toDatumConverter.Release()
}

// {{end}}
