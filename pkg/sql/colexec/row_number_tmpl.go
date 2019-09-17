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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
)

// {{ range . }}

type _ROW_NUMBER_STRINGOp struct {
	rowNumberBase
}

var _ Operator = &_ROW_NUMBER_STRINGOp{}

func (r *_ROW_NUMBER_STRINGOp) Next(ctx context.Context) coldata.Batch {
	batch := r.Input().Next(ctx)
	// {{ if .HasPartition }}
	if r.partitionColIdx == batch.Width() {
		batch.AppendCol(coltypes.Bool)
	} else if r.partitionColIdx > batch.Width() {
		execerror.VectorizedInternalPanic("unexpected: column partitionColIdx is neither present nor the next to be appended")
	}
	partitionCol := batch.ColVec(r.partitionColIdx).Bool()
	// {{ end }}

	if r.outputColIdx == batch.Width() {
		batch.AppendCol(coltypes.Int64)
	} else if r.outputColIdx > batch.Width() {
		execerror.VectorizedInternalPanic("unexpected: column outputColIdx is neither present nor the next to be appended")
	}
	if batch.Length() == 0 {
		return batch
	}
	rowNumberCol := batch.ColVec(r.outputColIdx).Int64()
	sel := batch.Selection()
	if sel != nil {
		for i := uint16(0); i < batch.Length(); i++ {
			// {{ if .HasPartition }}
			if partitionCol[sel[i]] {
				r.rowNumber = 1
			}
			// {{ end }}
			r.rowNumber++
			rowNumberCol[sel[i]] = r.rowNumber
		}
	} else {
		for i := uint16(0); i < batch.Length(); i++ {
			// {{ if .HasPartition }}
			if partitionCol[i] {
				r.rowNumber = 0
			}
			// {{ end }}
			r.rowNumber++
			rowNumberCol[i] = r.rowNumber
		}
	}
	return batch
}

// {{ end }}
