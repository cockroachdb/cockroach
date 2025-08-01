// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execreleasable"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type defaultBuiltinFuncOperator struct {
	colexecop.OneInputHelper
	allocator           *colmem.Allocator
	evalCtx             *eval.Context
	funcExpr            *tree.FuncExpr
	columnTypes         []*types.T
	argumentCols        []int
	outputIdx           int
	outputType          *types.T
	toDatumConverter    *colconv.VecToDatumConverter
	datumToVecConverter func(tree.Datum) interface{}

	row tree.Datums
}

var _ colexecop.Operator = &defaultBuiltinFuncOperator{}
var _ execreleasable.Releasable = &defaultBuiltinFuncOperator{}

func (b *defaultBuiltinFuncOperator) Next() coldata.Batch {
	batch := b.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}

	sel := batch.Selection()
	output := batch.ColVec(b.outputIdx)
	b.allocator.PerformOperation(
		[]*coldata.Vec{output},
		func() {
			b.toDatumConverter.ConvertBatchAndDeselect(batch)
			for i := 0; i < n; i++ {
				hasNulls := false

				for j, argumentCol := range b.argumentCols {
					// Note that we don't need to apply sel to index i because
					// vecToDatumConverter returns a "dense" datum column.
					b.row[j] = b.toDatumConverter.GetDatumColumn(argumentCol)[i]
					hasNulls = hasNulls || b.row[j] == tree.DNull
				}

				var (
					res tree.Datum
					err error
				)
				// Some functions cannot handle null arguments.
				if hasNulls && !b.funcExpr.ResolvedOverload().CalledOnNullInput {
					res = tree.DNull
				} else {
					res, err = b.funcExpr.ResolvedOverload().
						Fn.(eval.FnOverload)(b.Ctx, b.evalCtx, b.row)
					if err != nil {
						colexecerror.ExpectedError(b.funcExpr.MaybeWrapError(err))
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
					converted := b.datumToVecConverter(res)
					coldata.SetValueAt(output, converted, rowIdx)
				}
			}
		},
	)
	return batch
}

// Release is part of the execinfra.Releasable interface.
func (b *defaultBuiltinFuncOperator) Release() {
	b.toDatumConverter.Release()
}

// errFnWithExprsNotSupported is returned from NewBuiltinFunctionOperator
// when the function in question uses FnWithExprs, which is not supported.
var errFnWithExprsNotSupported = errors.New(
	"builtins with FnWithExprs are not supported in the vectorized engine",
)

// NewBuiltinFunctionOperator returns an operator that applies builtin functions.
func NewBuiltinFunctionOperator(
	allocator *colmem.Allocator,
	evalCtx *eval.Context,
	funcExpr *tree.FuncExpr,
	columnTypes []*types.T,
	argumentCols []int,
	outputIdx int,
	input colexecop.Operator,
) (colexecop.Operator, error) {
	overload := funcExpr.ResolvedOverload()
	if overload.FnWithExprs != nil {
		return nil, errFnWithExprsNotSupported
	}
	outputType := funcExpr.ResolvedType()
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, outputType, outputIdx)
	switch overload.SpecializedVecBuiltin {
	case tree.SubstringStringIntInt:
		return newSubstringOperator(
			allocator, columnTypes, argumentCols, outputIdx, input,
		), nil
	case tree.CrdbInternalRangeStats:
		if len(argumentCols) != 1 {
			return nil, errors.AssertionFailedf(
				"expected 1 input column to crdb_internal.range_stats, got %d",
				len(argumentCols),
			)
		}
		return newRangeStatsOperator(
			evalCtx.RangeStatsFetcher, allocator, argumentCols[0], outputIdx, input, false, /* withErrors */
		)
	case tree.CrdbInternalRangeStatsWithErrors:
		if len(argumentCols) != 1 {
			return nil, errors.AssertionFailedf(
				"expected 1 input column to crdb_internal.range_stats, got %d",
				len(argumentCols),
			)
		}
		return newRangeStatsOperator(
			evalCtx.RangeStatsFetcher, allocator, argumentCols[0], outputIdx, input, true, /* withErrors */
		)
	default:
		return &defaultBuiltinFuncOperator{
			OneInputHelper:      colexecop.MakeOneInputHelper(input),
			allocator:           allocator,
			evalCtx:             evalCtx,
			funcExpr:            funcExpr,
			outputIdx:           outputIdx,
			columnTypes:         columnTypes,
			outputType:          outputType,
			toDatumConverter:    colconv.NewVecToDatumConverter(len(columnTypes), argumentCols, true /* willRelease */),
			datumToVecConverter: colconv.GetDatumToPhysicalFn(outputType),
			row:                 make(tree.Datums, len(argumentCols)),
			argumentCols:        argumentCols,
		}, nil
	}
}
