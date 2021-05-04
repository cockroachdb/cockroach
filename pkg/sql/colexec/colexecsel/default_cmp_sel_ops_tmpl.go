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
// This file is the execgen template for default_cmp_sel_ops.eg.go. It's
// formatted in a special way, so it's both valid Go and a valid text/template
// input. This permits editing this file with editor support.
//
// */}}

package colexecsel

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

type defaultCmp_KINDSelOp struct {
	// {{if .HasConst}}
	selConstOpBase
	constArg tree.Datum
	// {{else}}
	selOpBase
	// {{end}}

	adapter          colexeccmp.ComparisonExprAdapter
	toDatumConverter *colconv.VecToDatumConverter
}

var _ colexecop.Operator = &defaultCmp_KINDSelOp{}
var _ execinfra.Releasable = &defaultCmp_KINDSelOp{}

func (d *defaultCmp_KINDSelOp) Next() coldata.Batch {
	for {
		batch := d.Input.Next()
		n := batch.Length()
		if n == 0 {
			return coldata.ZeroBatch
		}
		d.toDatumConverter.ConvertBatchAndDeselect(batch)
		// {{if .HasConst}}
		leftColumn := d.toDatumConverter.GetDatumColumn(d.colIdx)
		_ = leftColumn[n-1]
		// {{else}}
		leftColumn := d.toDatumConverter.GetDatumColumn(d.col1Idx)
		rightColumn := d.toDatumConverter.GetDatumColumn(d.col2Idx)
		_ = leftColumn[n-1]
		_ = rightColumn[n-1]
		// {{end}}
		var idx int
		hasSel := batch.Selection() != nil
		batch.SetSelection(true)
		sel := batch.Selection()
		_ = sel[n-1]
		for i := 0; i < n; i++ {
			// Note that we performed a conversion with deselection, so there
			// is no need to check whether hasSel is true.
			// {{if .HasConst}}
			//gcassert:bce
			res, err := d.adapter.Eval(leftColumn[i], d.constArg)
			// {{else}}
			//gcassert:bce
			res, err := d.adapter.Eval(leftColumn[i], rightColumn[i])
			// {{end}}
			if err != nil {
				colexecerror.ExpectedError(err)
			}
			if res == tree.DBoolTrue {
				rowIdx := i
				if hasSel {
					//gcassert:bce
					rowIdx = sel[i]
				}
				sel[idx] = rowIdx
				idx++
			}
		}
		if idx > 0 {
			batch.SetLength(idx)
			return batch
		}
	}
}

func (d *defaultCmp_KINDSelOp) Release() {
	d.toDatumConverter.Release()
}

// {{end}}
