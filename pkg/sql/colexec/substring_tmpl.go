// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

//
// This file is the execgen template for substring.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"unicode/utf8"

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
	colexecerror.InternalError(errors.AssertionFailedf("unsupported substring argument types: %s %s", startType, lengthType))
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
	bytesVec := batch.ColVec(s.argumentCols[0])
	bytesCol := bytesVec.Bytes()
	bytesNulls := bytesVec.Nulls()
	startVec := batch.ColVec(s.argumentCols[1])
	startCol := startVec._StartType()
	startNulls := startVec.Nulls()
	lengthVec := batch.ColVec(s.argumentCols[2])
	lengthCol := lengthVec._LengthType()
	lengthNulls := lengthVec.Nulls()
	outputVec := batch.ColVec(s.outputIdx)
	outputCol := outputVec.Bytes()
	outputNulls := outputVec.Nulls()
	s.allocator.PerformOperation(
		[]*coldata.Vec{outputVec},
		func() {
			argsMaybeHaveNulls := bytesNulls.MaybeHasNulls() || startNulls.MaybeHasNulls() || lengthNulls.MaybeHasNulls()
			for i := 0; i < n; i++ {
				rowIdx := i
				if sel != nil {
					rowIdx = sel[i]
				}

				if argsMaybeHaveNulls {
					// The substring operator does not support nulls. If any of
					// the arguments are NULL, we output NULL.
					if bytesNulls.NullAt(rowIdx) || startNulls.NullAt(rowIdx) || lengthNulls.NullAt(rowIdx) {
						outputNulls.SetNull(rowIdx)
						continue
					}
				}

				bytes := bytesCol.Get(rowIdx)
				// Substring startCharIdx is 1 indexed.
				startCharIdx := int(startCol[rowIdx]) - 1
				length := int(lengthCol[rowIdx])
				if length < 0 {
					colexecerror.ExpectedError(errors.Errorf("negative substring length %d not allowed", length))
				}

				endCharIdx := startCharIdx + length
				// Check for integer overflow.
				if endCharIdx < startCharIdx {
					endCharIdx = len(bytes)
				} else if endCharIdx < 0 {
					endCharIdx = 0
				} else if endCharIdx > len(bytes) {
					endCharIdx = len(bytes)
				}

				if startCharIdx < 0 {
					startCharIdx = 0
				} else if startCharIdx > len(bytes) {
					startCharIdx = len(bytes)
				}
				startBytesIdx := startCharIdx
				endBytesIdx := endCharIdx

				// If there is a rune that uses more than 1 byte in the substring or in
				// the bytes leading up to the substring, then we must adjust the start
				// and end indices to the rune boundaries. However, we have to test the
				// entire bytes slice instead of a subset, since RuneCount will treat an
				// incomplete encoding as a single byte rune.
				if utf8.RuneCount(bytes) != len(bytes) {
					count := 0
					totalSize := 0
					// Find the substring startCharIdx in bytes offset.
					for count < startCharIdx && totalSize < len(bytes) {
						_, size := utf8.DecodeRune(bytes[totalSize:])
						totalSize += size
						count++
					}
					startBytesIdx = totalSize
					// Find the substring endCharIdx in bytes offset.
					for count < endCharIdx && totalSize < len(bytes) {
						_, size := utf8.DecodeRune(bytes[totalSize:])
						totalSize += size
						count++
					}
					endBytesIdx = totalSize
				}
				outputCol.Set(rowIdx, bytes[startBytesIdx:endBytesIdx])
			}
		},
	)
	return batch
}

// {{end}}
// {{end}}
