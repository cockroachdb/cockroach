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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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
	if n == 0 {
		return coldata.ZeroBatch
	}
	b.allocator.MaybeAddColumn(batch, b.outputPhysType, b.outputIdx)

	sel := batch.Selection()
	output := batch.ColVec(b.outputIdx)
	b.allocator.PerformOperation(
		[]coldata.Vec{output},
		func() {
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
					coldata.SetValueAt(output, converted, rowIdx, b.outputPhysType)
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
	allocator *Allocator,
	evalCtx *tree.EvalContext,
	funcExpr *tree.FuncExpr,
	columnTypes []types.T,
	argumentCols []int,
	outputIdx int,
	input Operator,
) (Operator, error) {

	switch funcExpr.ResolvedOverload().SpecializedVecBuiltin {
	case tree.SubstringStringIntInt:
		return newSubstringOperator(
			allocator, columnTypes, argumentCols, outputIdx, input,
		), nil
	default:
		outputType := funcExpr.ResolvedType()
		outputPhysType := typeconv.FromColumnType(outputType)
		if outputPhysType == coltypes.Unhandled {
			return nil, errors.Errorf(
				"unsupported output type %q of %s",
				outputType.String(), funcExpr.String(),
			)
		}
		return &defaultBuiltinFuncOperator{
			OneInputNode:   NewOneInputNode(input),
			allocator:      allocator,
			evalCtx:        evalCtx,
			funcExpr:       funcExpr,
			outputIdx:      outputIdx,
			columnTypes:    columnTypes,
			outputType:     outputType,
			outputPhysType: outputPhysType,
			converter:      typeconv.GetDatumToPhysicalFn(outputType),
			row:            make(tree.Datums, len(argumentCols)),
			argumentCols:   argumentCols,
		}, nil
	}
}
