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
// This file is the execgen template for mergejoinerbase.eg.go. It's formatted
// in a special way, so it's both valid Go and a valid text/template input.
// This permits editing this file with editor support.
//
// */}}

package exec

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// {{/*
// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// _TYPES_T is the template type variable for coltypes.T. It will be replaced by
// coltypes.Foo for each type Foo in the coltypes.T type.
const _TYPES_T = coltypes.Unhandled

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in coltypes.T, for example
// int64 for coltypes.Int64.
type _GOTYPE interface{}

// _ASSIGN_EQ is the template equality function for assigning the first input
// to the result of the the second input == the third input.
func _ASSIGN_EQ(_, _, _ interface{}) uint64 {
	execerror.VectorizedInternalPanic("")
}

// */}}

// Use execgen package to remove unused import warning.
var _ interface{} = execgen.UNSAFEGET

// isBufferedGroupFinished checks to see whether or not the buffered group
// corresponding to input continues in batch.
func (o *mergeJoinBase) isBufferedGroupFinished(
	input *mergeJoinInput, batch coldata.Batch, rowIdx int,
) bool {
	if batch.Length() == 0 {
		return true
	}
	bufferedGroup := o.proberState.lBufferedGroup
	if input == &o.right {
		bufferedGroup = o.proberState.rBufferedGroup
	}
	lastBufferedTupleIdx := bufferedGroup.length - 1
	tupleToLookAtIdx := uint64(rowIdx)
	sel := batch.Selection()
	if sel != nil {
		tupleToLookAtIdx = uint64(sel[rowIdx])
	}

	// Check all equality columns in the first row of batch to make sure we're in
	// the same group.
	for _, colIdx := range input.eqCols[:len(input.eqCols)] {
		colTyp := input.sourceTypes[colIdx]

		switch colTyp {
		// {{ range $.MJOverloads }}
		case _TYPES_T:
			// We perform this null check on every equality column of the last
			// buffered tuple regardless of the join type since it is done only once
			// per batch. In some cases (like INNER JOIN, or LEFT OUTER JOIN with the
			// right side being an input) this check will always return false since
			// nulls couldn't be buffered up though.
			if bufferedGroup.ColVec(int(colIdx)).Nulls().NullAt64(uint64(lastBufferedTupleIdx)) {
				return true
			}
			bufferedCol := bufferedGroup.ColVec(int(colIdx))._TemplateType()
			prevVal := execgen.UNSAFEGET(bufferedCol, int(lastBufferedTupleIdx))
			var curVal _GOTYPE
			if batch.ColVec(int(colIdx)).MaybeHasNulls() && batch.ColVec(int(colIdx)).Nulls().NullAt64(tupleToLookAtIdx) {
				return true
			}
			col := batch.ColVec(int(colIdx))._TemplateType()
			curVal = execgen.UNSAFEGET(col, int(tupleToLookAtIdx))
			var match bool
			_ASSIGN_EQ("match", "prevVal", "curVal")
			if !match {
				return true
			}
		// {{end}}
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", colTyp))
		}
	}
	return false
}

// isLeftTupleFilteredOut returns whether a tuple lIdx from lBatch combined
// with any tuple in range [rStartIdx, rEndIdx) from rBatch satisfies the
// filter.
// A special case of rBatch == nil is supported which will run the filter only
// on the partial tuple coming from the left input.
func (o *mergeJoinBase) isLeftTupleFilteredOut(
	ctx context.Context, lBatch, rBatch coldata.Batch, lIdx, rStartIdx, rEndIdx int,
) bool {
	if o.filterOnlyOnLeft || rBatch == nil {
		o.filterInput.reset()
		o.setFilterInputBatch(lBatch, nil /* rBatch */, lIdx, 0 /* rIdx */)
		b := o.filter.Next(ctx)
		return b.Length() == 0
	}
	for rIdx := rStartIdx; rIdx < rEndIdx; rIdx++ {
		o.filterInput.reset()
		o.setFilterInputBatch(lBatch, rBatch, lIdx, rIdx)
		b := o.filter.Next(ctx)
		if b.Length() > 0 {
			return false
		}
	}
	return true
}

// setFilterInputBatch sets the batch of filterFeedOperator, namely, it copies
// a single tuple from each of the batches at the specified indices and puts
// the combined "double" tuple into o.filterInput.
// Either lBatch or rBatch can be nil to indicate that the tuple from the
// respective side should not be set, i.e. the filter input batch will only be
// partially initialized.
func (o *mergeJoinBase) setFilterInputBatch(lBatch, rBatch coldata.Batch, lIdx, rIdx int) {
	if lBatch == nil && rBatch == nil {
		execerror.VectorizedInternalPanic("only one of lBatch and rBatch can be nil")
	}
	setOneSide := func(colOffset int, batch coldata.Batch, sourceTypes []coltypes.T, idx int) {
		for colIdx, col := range batch.ColVecs() {
			colType := sourceTypes[colIdx]
			memCol := col.Slice(colType, uint64(idx), uint64(idx+1))
			switch colType {
			// {{ range $mjOverload := $.MJOverloads }}
			case _TYPES_T:
				o.filterInput.batch.ColVec(colOffset + colIdx).SetCol(memCol._TemplateType())
				// {{ end }}
			default:
				execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", colType))
			}
			o.filterInput.batch.ColVec(colOffset + colIdx).SetNulls(memCol.Nulls())
		}
	}
	if lBatch != nil {
		setOneSide(0 /* colOffset */, lBatch, o.left.sourceTypes, lIdx)
	}
	if rBatch != nil {
		setOneSide(len(o.left.sourceTypes), rBatch, o.right.sourceTypes, rIdx)
	}
	o.filterInput.batch.SetLength(1)
	o.filterInput.batch.SetSelection(false)
}
