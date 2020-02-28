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
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func newSubstringOperator(
	allocator *Allocator, columnTypes []types.T, argumentCols []int, outputIdx int, input Operator,
) Operator {
	startType := typeconv.FromColumnType(&columnTypes[argumentCols[1]])
	lengthType := typeconv.FromColumnType(&columnTypes[argumentCols[2]])
	base := substringFunctionBase{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		argumentCols: argumentCols,
		outputIdx:    outputIdx,
	}
	switch startType {
	// {{range $startType, $lengthTypes := .}}
	case _StartType_T:
		switch lengthType {
		// {{range $lengthType := $lengthTypes}}
		case _LengthType_T:
			return &substring_StartType_LengthTypeOperator{base}
		// {{end}}
		default:
			execerror.VectorizedInternalPanic(errors.Errorf("unsupported length argument type %s", lengthType))
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
	// {{end}}
	default:
		execerror.VectorizedInternalPanic(errors.Errorf("unsupported start argument type %s", startType))
		// This code is unreachable, but the compiler cannot infer that.
		return nil
	}
}

type substringFunctionBase struct {
	OneInputNode
	allocator    *Allocator
	argumentCols []int
	outputIdx    int
}

func (s *substringFunctionBase) Init() {
	s.input.Init()
}

// {{range $startType, $lengthTypes := .}}
// {{range $lengthType := $lengthTypes}}

type substring_StartType_LengthTypeOperator struct {
	substringFunctionBase
}

var _ Operator = &substring_StartType_LengthTypeOperator{}

func (s *substring_StartType_LengthTypeOperator) Next(ctx context.Context) coldata.Batch {
	batch := s.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	s.allocator.MaybeAddColumn(batch, coltypes.Bytes, s.outputIdx)

	sel := batch.Selection()
	runeVec := batch.ColVec(s.argumentCols[0]).Bytes()
	startVec := batch.ColVec(s.argumentCols[1])._StartType()
	lengthVec := batch.ColVec(s.argumentCols[2])._LengthType()
	outputVec := batch.ColVec(s.outputIdx)
	outputCol := outputVec.Bytes()
	s.allocator.PerformOperation(
		[]coldata.Vec{outputVec},
		func() {
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
					execerror.NonVectorizedPanic(errors.Errorf("negative substring length %d not allowed", length))
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
