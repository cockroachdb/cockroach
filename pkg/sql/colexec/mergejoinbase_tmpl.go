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

package colexec

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

// Remove unused warning.
var _ = execgen.UNSAFEGET

// {{/*
// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "duration" package.
var _ duration.Duration

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// _GOTYPE is the template variable.
type _GOTYPE interface{}

// _ASSIGN_EQ is the template equality function for assigning the first input
// to the result of the the second input == the third input.
func _ASSIGN_EQ(_, _, _, _, _, _ interface{}) int {
	colexecerror.InternalError("")
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
	bufferedGroup := o.proberState.lBufferedGroup
	if input == &o.right {
		bufferedGroup = o.proberState.rBufferedGroup
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
				// per batch. In some cases (like INNER JOIN, or LEFT OUTER JOIN with the
				// right side being an input) this check will always return false since
				// nulls couldn't be buffered up though.
				if bufferedGroup.firstTuple[colIdx].Nulls().NullAt(0) {
					return true
				}
				bufferedCol := bufferedGroup.firstTuple[colIdx].TemplateType()
				prevVal := execgen.UNSAFEGET(bufferedCol, 0)
				var curVal _GOTYPE
				if batch.ColVec(int(colIdx)).MaybeHasNulls() && batch.ColVec(int(colIdx)).Nulls().NullAt(tupleToLookAtIdx) {
					return true
				}
				col := batch.ColVec(int(colIdx)).TemplateType()
				curVal = execgen.UNSAFEGET(col, tupleToLookAtIdx)
				var match bool
				_ASSIGN_EQ(match, prevVal, curVal, _, bufferedCol, col)
				if !match {
					return true
				}
				// {{end}}
			}
		// {{end}}
		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled type %s", input.sourceTypes[colIdx]))
		}
	}
	return false
}
