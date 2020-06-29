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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type defaultBuiltinFuncOperator struct {
	OneInputNode
	allocator           *colmem.Allocator
	evalCtx             *tree.EvalContext
	funcExpr            *tree.FuncExpr
	columnTypes         []*types.T
	argumentCols        []int
	outputIdx           int
	outputType          *types.T
	toDatumConverter    *vecToDatumConverter
	datumToVecConverter func(tree.Datum) (interface{}, error)

	row tree.Datums
}

var _ colexecbase.Operator = &defaultBuiltinFuncOperator{}

func (b *defaultBuiltinFuncOperator) Init() {
	b.input.Init()
}

func (b *defaultBuiltinFuncOperator) Next(ctx context.Context) coldata.Batch {
	batch := b.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}

	sel := batch.Selection()
	output := batch.ColVec(b.outputIdx)
	if output.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		output.Nulls().UnsetNulls()
	}
	b.allocator.PerformOperation(
		[]coldata.Vec{output},
		func() {
			b.toDatumConverter.convertBatch(batch)
			for i := 0; i < n; i++ {
				hasNulls := false

				for j, argumentCol := range b.argumentCols {
					// Note that we don't need to apply sel to index i because
					// vecToDatumConverter returns a "dense" datum column.
					b.row[j] = b.toDatumConverter.getDatumColumn(argumentCol)[i]
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
						colexecerror.ExpectedError(err)
					}
				}

				rowIdx := i
				if sel != nil {
					rowIdx = sel[i]
				}

				// Convert the datum into a physical type and write it out.
				if res == tree.DNull {
					output.Nulls().SetNull(rowIdx)
				} else {
					converted, err := b.datumToVecConverter(res)
					if err != nil {
						colexecerror.InternalError(err)
					}
					coldata.SetValueAt(output, converted, rowIdx)
				}
			}
		},
	)
	// Although we didn't change the length of the batch, it is necessary to set
	// the length anyway (this helps maintaining the invariant of flat bytes).
	batch.SetLength(n)
	return batch
}

// NewBuiltinFunctionOperator returns an operator that applies builtin functions.
func NewBuiltinFunctionOperator(
	allocator *colmem.Allocator,
	evalCtx *tree.EvalContext,
	funcExpr *tree.FuncExpr,
	columnTypes []*types.T,
	argumentCols []int,
	outputIdx int,
	input colexecbase.Operator,
) (colexecbase.Operator, error) {
	switch funcExpr.ResolvedOverload().SpecializedVecBuiltin {
	case tree.SubstringStringIntInt:
		input = newVectorTypeEnforcer(allocator, input, types.String, outputIdx)
		return newSubstringOperator(
			allocator, columnTypes, argumentCols, outputIdx, input,
		), nil
	default:
		outputType := funcExpr.ResolvedType()
		input = newVectorTypeEnforcer(allocator, input, outputType, outputIdx)
		return &defaultBuiltinFuncOperator{
			OneInputNode:        NewOneInputNode(input),
			allocator:           allocator,
			evalCtx:             evalCtx,
			funcExpr:            funcExpr,
			outputIdx:           outputIdx,
			columnTypes:         columnTypes,
			outputType:          outputType,
			toDatumConverter:    newVecToDatumConverter(len(columnTypes), argumentCols),
			datumToVecConverter: GetDatumToPhysicalFn(outputType),
			row:                 make(tree.Datums, len(argumentCols)),
			argumentCols:        argumentCols,
		}, nil
	}
}
