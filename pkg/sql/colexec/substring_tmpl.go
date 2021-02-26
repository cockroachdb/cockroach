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
// This file is the execgen template for substring.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// {{/*

// _START_WIDTH is the template variable.
const _START_WIDTH = 0

// _LENGTH_WIDTH is the template variable.
const _LENGTH_WIDTH = 0

// */}}

func newSubstringOperator(
	allocator *colmem.Allocator,
	typs []*types.T,
	argumentCols []int,
	outputIdx int,
	input colexecop.Operator,
) colexecop.Operator {
	startType := typs[argumentCols[1]]
	lengthType := typs[argumentCols[2]]
	base := substringFunctionBase{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		allocator:      allocator,
		argumentCols:   argumentCols,
		outputIdx:      outputIdx,
	}
	if startType.Family() != types.IntFamily {
		colexecerror.InternalError(errors.AssertionFailedf("non-int start argument type %s", startType))
	}
	if lengthType.Family() != types.IntFamily {
		colexecerror.InternalError(errors.AssertionFailedf("non-int length argument type %s", lengthType))
	}
	switch startType.Width() {
	// {{range $startWidth, $lengthWidths := .}}
	case _START_WIDTH:
		switch lengthType.Width() {
		// {{range $lengthWidth := $lengthWidths}}
		case _LENGTH_WIDTH:
			return &substring_StartType_LengthTypeOperator{base}
			// {{end}}
		}
		// {{end}}
	}
	colexecerror.InternalError(errors.Errorf("unsupported substring argument types: %s %s", startType, lengthType))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

type substringFunctionBase struct {
	colexecop.OneInputHelper
	allocator    *colmem.Allocator
	argumentCols []int
	outputIdx    int
}

// {{range $startWidth, $lengthWidths := .}}
// {{range $lengthWidth := $lengthWidths}}

type substring_StartType_LengthTypeOperator struct {
	substringFunctionBase
}

var _ colexecop.Operator = &substring_StartType_LengthTypeOperator{}

func (s *substring_StartType_LengthTypeOperator) Next() coldata.Batch {
	batch := s.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}

	sel := batch.Selection()
	runeVec := batch.ColVec(s.argumentCols[0]).Bytes()
	startVec := batch.ColVec(s.argumentCols[1])._StartType()
	lengthVec := batch.ColVec(s.argumentCols[2])._LengthType()
	outputVec := batch.ColVec(s.outputIdx)
	if outputVec.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		outputVec.Nulls().UnsetNulls()
	}
	outputCol := outputVec.Bytes()
	s.allocator.PerformOperation(
		[]coldata.Vec{outputVec},
		func() {
			// TODO(yuzefovich): refactor this loop so that BCE occurs when sel
			// is nil.
			for i := 0; i < n; i++ {
				rowIdx := i
				if sel != nil {
					rowIdx = sel[i]
				}

				// The substring operator does not support nulls. If any of the arguments
				// are NULL, we output NULL.
				isNull := false
				for _, col := range s.argumentCols {
					if batch.ColVec(col).Nulls().NullAt(rowIdx) {
						isNull = true
						break
					}
				}
				if isNull {
					batch.ColVec(s.outputIdx).Nulls().SetNull(rowIdx)
					continue
				}

				runes := runeVec.Get(rowIdx)
				// Substring start is 1 indexed.
				start := int(startVec[rowIdx]) - 1
				length := int(lengthVec[rowIdx])
				if length < 0 {
					colexecerror.ExpectedError(errors.Errorf("negative substring length %d not allowed", length))
				}

				end := start + length
				// Check for integer overflow.
				if end < start {
					end = len(runes)
				} else if end < 0 {
					end = 0
				} else if end > len(runes) {
					end = len(runes)
				}

				if start < 0 {
					start = 0
				} else if start > len(runes) {
					start = len(runes)
				}
				outputCol.Set(rowIdx, runes[start:end])
			}
		},
	)
	// Although we didn't change the length of the batch, it is necessary to set
	// the length anyway (this helps maintaining the invariant of flat bytes).
	batch.SetLength(n)
	return batch
}

// {{end}}
// {{end}}
