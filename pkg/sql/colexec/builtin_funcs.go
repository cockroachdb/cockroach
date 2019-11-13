// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type defaultBuiltinFuncOperator struct {
	OneInputNode
	allocator      *Allocator
	evalCtx        *tree.EvalContext
	funcExpr       *tree.FuncExpr
	columnTypes    []types.T
	argumentCols   []int
	outputIdx      int
	outputType     *types.T
	outputPhysType coltypes.T
	converter      func(tree.Datum) (interface{}, error)

	row tree.Datums
	da  sqlbase.DatumAlloc
}

var _ Operator = &defaultBuiltinFuncOperator{}

func (b *defaultBuiltinFuncOperator) Init() {
	b.input.Init()
}

func (b *defaultBuiltinFuncOperator) Next(ctx context.Context) coldata.Batch {
	batch := b.input.Next(ctx)
	n := batch.Length()
	if b.outputIdx == batch.Width() {
		b.allocator.AppendColumn(batch, b.outputPhysType)
	}
	if n == 0 {
		return batch
	}

	sel := batch.Selection()
	for i := uint16(0); i < n; i++ {
		rowIdx := i
		if sel != nil {
			rowIdx = sel[i]
		}

		hasNulls := false

		for j := range b.argumentCols {
			col := batch.ColVec(b.argumentCols[j])
			b.row[j] = PhysicalTypeColElemToDatum(col, rowIdx, b.da, &b.columnTypes[b.argumentCols[j]])
			hasNulls = hasNulls || b.row[j] == tree.DNull
		}

		var (
			res tree.Datum
			err error
		)
		// Some functions cannot handle null arguments.
		if hasNulls && !b.funcExpr.CanHandleNulls() {
			res = tree.DNull
		} else {
			res, err = b.funcExpr.ResolvedOverload().Fn(b.evalCtx, b.row)
			if err != nil {
				execerror.NonVectorizedPanic(err)
			}
		}

		// Convert the datum into a physical type and write it out.
		if res == tree.DNull {
			batch.ColVec(b.outputIdx).Nulls().SetNull(rowIdx)
		} else {
			converted, err := b.converter(res)
			if err != nil {
				execerror.VectorizedInternalPanic(err)
			}
			coldata.SetValueAt(batch.ColVec(b.outputIdx), converted, rowIdx, b.outputPhysType)
		}
	}
	return batch
}

type substringFunctionOperator struct {
	OneInputNode
	allocator    *Allocator
	argumentCols []int
	outputIdx    int
}

var _ Operator = &substringFunctionOperator{}

func (s *substringFunctionOperator) Init() {
	s.input.Init()
}

func (s *substringFunctionOperator) Next(ctx context.Context) coldata.Batch {
	batch := s.input.Next(ctx)
	if s.outputIdx == batch.Width() {
		s.allocator.AppendColumn(batch, coltypes.Bytes)
	}

	n := batch.Length()
	if n == 0 {
		return batch
	}

	sel := batch.Selection()
	runeVec := batch.ColVec(s.argumentCols[0]).Bytes()
	startVec := batch.ColVec(s.argumentCols[1]).Int64()
	lengthVec := batch.ColVec(s.argumentCols[2]).Int64()
	outputVec := batch.ColVec(s.outputIdx).Bytes()
	for i := uint16(0); i < n; i++ {
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

		runes := runeVec.Get(int(rowIdx))
		// Substring start is 1 indexed.
		start := int(startVec[rowIdx]) - 1
		length := int(lengthVec[rowIdx])
		if length < 0 {
			execerror.VectorizedInternalPanic(fmt.Sprintf("negative substring length %d not allowed", length))
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
		outputVec.Set(int(rowIdx), runes[start:end])
	}

	return batch
}

// NewBuiltinFunctionOperator returns an operator that applies builtin functions.
func NewBuiltinFunctionOperator(
	allocator *Allocator,
	evalCtx *tree.EvalContext,
	funcExpr *tree.FuncExpr,
	columnTypes []types.T,
	argumentCols []int,
	outputIdx int,
	input Operator,
) Operator {

	switch funcExpr.ResolvedOverload().SpecializedVecBuiltin {
	case tree.SubstringStringIntInt:
		return &substringFunctionOperator{
			OneInputNode: NewOneInputNode(input),
			allocator:    allocator,
			argumentCols: argumentCols,
			outputIdx:    outputIdx,
		}
	default:
		outputType := funcExpr.ResolvedType()
		return &defaultBuiltinFuncOperator{
			OneInputNode:   NewOneInputNode(input),
			allocator:      allocator,
			evalCtx:        evalCtx,
			funcExpr:       funcExpr,
			outputIdx:      outputIdx,
			columnTypes:    columnTypes,
			outputType:     outputType,
			outputPhysType: typeconv.FromColumnType(outputType),
			converter:      typeconv.GetDatumToPhysicalFn(outputType),
			row:            make(tree.Datums, len(argumentCols)),
			argumentCols:   argumentCols,
		}
	}
}
