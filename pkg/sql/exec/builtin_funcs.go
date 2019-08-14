// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type defaultBuiltinFuncOperator struct {
	OneInputNode
	evalCtx        *tree.EvalContext
	funcExpr       *tree.FuncExpr
	columnTypes    []types.T
	inputCols      []int
	outputIdx      int
	outputType     *types.T
	outputPhysType coltypes.T
	converter      func(tree.Datum) (interface{}, error)

	row tree.Datums
	da  sqlbase.DatumAlloc
}

func (b *defaultBuiltinFuncOperator) Init() {
	b.input.Init()
}

func (b *defaultBuiltinFuncOperator) Next(ctx context.Context) coldata.Batch {
	batch := b.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return batch
	}

	if b.outputIdx == batch.Width() {
		batch.AppendCol(b.outputPhysType)
	}

	sel := batch.Selection()
	for i := uint16(0); i < n; i++ {
		rowIdx := i
		if sel != nil {
			rowIdx = sel[i]
		}

		hasNulls := false

		for j := range b.inputCols {
			col := batch.ColVec(b.inputCols[j])
			if col.MaybeHasNulls() && col.Nulls().NullAt(rowIdx) {
				hasNulls = true
				b.row[j] = tree.DNull
			} else {
				b.row[j] = PhysicalTypeColElemToDatum(col, rowIdx, b.da, b.columnTypes[b.inputCols[j]])
			}
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

// NewBuiltinFunctionOperator returns an operator that applies builtin functions.
func NewBuiltinFunctionOperator(
	evalCtx *tree.EvalContext,
	funcExpr *tree.FuncExpr,
	columnTypes []types.T,
	inputCols []int,
	outputIdx int,
	input Operator,
) Operator {

	outputType := funcExpr.ResolvedType()

	// For now, return the default builtin operator. Future work can specialize
	// out the operators to efficient implementations of specific builtins.
	return &defaultBuiltinFuncOperator{
		OneInputNode:   NewOneInputNode(input),
		evalCtx:        evalCtx,
		funcExpr:       funcExpr,
		outputIdx:      outputIdx,
		columnTypes:    columnTypes,
		outputType:     outputType,
		outputPhysType: typeconv.FromColumnType(outputType),
		converter:      typeconv.GetDatumToPhysicalFn(outputType),
		row:            make(tree.Datums, len(inputCols)),
		inputCols:      inputCols,
	}
}
