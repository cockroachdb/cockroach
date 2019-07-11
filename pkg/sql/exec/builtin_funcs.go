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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types/conv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
)

type indexResolver struct {
	row      tree.Datums
	colTypes []semtypes.T
}

var _ tree.IndexedVarContainer = &indexResolver{}

func (i *indexResolver) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return i.row[idx].Eval(ctx)
}

func (i *indexResolver) IndexedVarResolvedType(idx int) *semtypes.T {
	return &i.colTypes[idx]
}

func (i *indexResolver) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("$%d", idx))
	return &n
}

// defaultBuiltinOperator wraps around a builtin function and
// uses it to compute the function one row at a time. This allows
// the vectorized engine to execute all builtin functions that
// distsql can do. However, it repeatedly encodes and decodes
// rows to and from datums, so it should be swapped out with an
// efficient (vectorized) implementation for builtins that are
// important for performance.
type defaultBuiltinOperator struct {
	input Operator

	evalCtx   *tree.EvalContext
	funcExpr  *tree.FuncExpr
	outputIdx int
	colTypes  []semtypes.T

	da  sqlbase.DatumAlloc
	row tree.Datums

	resolved *indexResolver
}

func (d *defaultBuiltinOperator) Init() {
	d.input.Init()
}

func (d *defaultBuiltinOperator) Next(ctx context.Context) coldata.Batch {
	batch := d.input.Next(ctx)
	n := batch.Length()

	if n == 0 {
		return batch
	}

	outputType := d.funcExpr.ResolvedType()
	outputPhysType := conv.FromColumnType(outputType)

	if d.outputIdx == batch.Width() {
		batch.AppendCol(outputPhysType)
	}

	converter := conv.GetDatumToPhysicalFn(outputType)

	sel := batch.Selection()
	for i := uint16(0); i < n; i++ {
		rowIdx := i
		if sel != nil {
			rowIdx = sel[i]
		}

		// Construct the full datum row from the elements in the column batch.
		for j := 0; j < batch.Width(); j++ {
			col := batch.ColVec(j)
			if col.MaybeHasNulls() && col.Nulls().NullAt(rowIdx) {
				d.row[j] = tree.DNull
			} else {
				d.row[j] = PhysicalTypeColElemToDatum(col, rowIdx, d.da, d.colTypes[j])
			}
		}

		// Push an indexVariableResolver onto the context stack, and evaluate
		// the expression on the row.
		d.resolved.row = d.row
		d.evalCtx.PushIVarContainer(d.resolved)
		res, err := d.funcExpr.Eval(d.evalCtx)
		if err != nil {
			panic(err)
		}
		d.evalCtx.PopIVarContainer()

		// Convert the datum into a physical type and write it out.
		converted, err := converter(res)
		if err != nil {
			panic(err)
		}
		if res == tree.DNull {
			batch.ColVec(d.outputIdx).Nulls().SetNull(uint16(rowIdx))
		} else {
			batch.ColVec(d.outputIdx).SetValueAt(converted, rowIdx, outputPhysType)
		}
	}

	return batch
}

// NewBuiltinFunctionOperator returns an operator that applies builtin functions.
func NewBuiltinFunctionOperator(
	tctx *tree.EvalContext,
	columnTypes []semtypes.T,
	input Operator,
	funcExpr *tree.FuncExpr,
	outputIdx int,
) Operator {

	// For now, return the default builtin operator. Future work can specialize
	// out the operators to efficient implementations of specific builtins.
	return &defaultBuiltinOperator{
		input:     input,
		evalCtx:   tctx,
		funcExpr:  funcExpr,
		outputIdx: outputIdx,
		colTypes:  columnTypes,
		row:       make(tree.Datums, len(columnTypes)),
		resolved:  &indexResolver{colTypes: columnTypes},
	}
}
