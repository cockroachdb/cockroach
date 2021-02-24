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
// This file is the execgen template for mergejoinbase.eg.go. It's formatted
// in a special way, so it's both valid Go and a valid text/template input.
// This permits editing this file with editor support.
//
// */}}

package colexecjoin

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ = typeconv.DatumVecCanonicalTypeFamily
	_ coldataext.Datum
	_ tree.AggType
)

// {{/*
// Declarations to make the template compile properly.

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// _ASSIGN_EQ is the template equality function for assigning the first input
// to the result of the second input == the third input.
func _ASSIGN_EQ(_, _, _, _, _, _ interface{}) int {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

// isBufferedGroupFinished checks to see whether or not the buffered group
// corresponding to input continues in batch.
func (o *mergeJoinBase) isBufferedGroupFinished(
	input *mergeJoinInput, batch coldata.Batch, rowIdx int,
) bool {
	if batch.Length() == 0 {
		return true
	}
	bufferedGroup := o.bufferedGroup.left
	if input == &o.right {
		bufferedGroup = o.bufferedGroup.right
	}
	tupleToLookAtIdx := rowIdx
	sel := batch.Selection()
	if sel != nil {
		tupleToLookAtIdx = sel[rowIdx]
	}

	// Check all equality columns in the first row of batch to make sure we're in
	// the same group.
	for _, colIdx := range input.eqCols[:len(input.eqCols)] {
		switch input.canonicalTypeFamilies[colIdx] {
		// {{range .}}
		case _CANONICAL_TYPE_FAMILY:
			switch input.sourceTypes[colIdx].Width() {
			// {{range .WidthOverloads}}
			case _TYPE_WIDTH:
				// We perform this null check on every equality column of the first
				// buffered tuple regardless of the join type since it is done only once
				// per batch. In some cases (like INNER join, or LEFT OUTER join with the
				// right side being an input) this check will always return false since
				// nulls couldn't be buffered up though.
				// TODO(yuzefovich): consider templating this.
				bufferedNull := bufferedGroup.firstTuple[colIdx].MaybeHasNulls() && bufferedGroup.firstTuple[colIdx].Nulls().NullAt(0)
				incomingNull := batch.ColVec(int(colIdx)).MaybeHasNulls() && batch.ColVec(int(colIdx)).Nulls().NullAt(tupleToLookAtIdx)
				if o.joinType.IsSetOpJoin() {
					if bufferedNull && incomingNull {
						// We have a NULL match, so move onto the next column.
						continue
					}
				}
				if bufferedNull || incomingNull {
					return true
				}
				bufferedCol := bufferedGroup.firstTuple[colIdx].TemplateType()
				prevVal := bufferedCol.Get(0)
				col := batch.ColVec(int(colIdx)).TemplateType()
				curVal := col.Get(tupleToLookAtIdx)
				var match bool
				_ASSIGN_EQ(match, prevVal, curVal, _, bufferedCol, col)
				if !match {
					return true
				}
				// {{end}}
			}
		// {{end}}
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled type %s", input.sourceTypes[colIdx]))
		}
	}
	return false
}
