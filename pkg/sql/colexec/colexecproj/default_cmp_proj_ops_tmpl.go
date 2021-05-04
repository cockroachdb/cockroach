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
// This file is the execgen template for default_cmp_proj_ops.eg.go. It's
// formatted in a special way, so it's both valid Go and a valid text/template
// input. This permits editing this file with editor support.
//
// */}}

package colexecproj

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexeccmp"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// {{range .}}

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
var _ execinfra.Releasable = &defaultCmp_KINDProjOp{}

func (d *defaultCmp_KINDProjOp) Next() coldata.Batch {
	batch := d.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	sel := batch.Selection()
	output := batch.ColVec(d.outputIdx)
	d.allocator.PerformOperation([]coldata.Vec{output}, func() {
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
			res, err := d.adapter.Eval(nonConstColumn[i], d.constArg)
			// {{else}}
			//gcassert:bce
			res, err := d.adapter.Eval(leftColumn[i], rightColumn[i])
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
	// Although we didn't change the length of the batch, it is necessary to set
	// the length anyway (this helps maintaining the invariant of flat bytes).
	batch.SetLength(n)
	return batch
}

func (d *defaultCmp_KINDProjOp) Release() {
	d.toDatumConverter.Release()
}

// {{end}}
