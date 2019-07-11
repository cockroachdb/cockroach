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

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
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

	writer := func(batch coldata.Batch, datum tree.Datum, i int) {
		if datum == tree.DNull {
			batch.ColVec(d.outputIdx).Nulls().SetNull(uint16(i))
			return
		}
		converted, _ := converter(datum)
		switch outputPhysType {
		case types.Bool:
			batch.ColVec(d.outputIdx).Bool()[i] = converted.(bool)
		case types.Bytes:
			batch.ColVec(d.outputIdx).Bytes()[i] = converted.([]byte)
		case types.Int8:
			batch.ColVec(d.outputIdx).Int8()[i] = converted.(int8)
		case types.Int16:
			batch.ColVec(d.outputIdx).Int16()[i] = converted.(int16)
		case types.Int32:
			batch.ColVec(d.outputIdx).Int32()[i] = converted.(int32)
		case types.Int64:
			batch.ColVec(d.outputIdx).Int64()[i] = converted.(int64)
		case types.Float32:
			batch.ColVec(d.outputIdx).Float32()[i] = converted.(float32)
		case types.Float64:
			batch.ColVec(d.outputIdx).Float64()[i] = converted.(float64)
		case types.Decimal:
			batch.ColVec(d.outputIdx).Decimal()[i] = converted.(apd.Decimal)
		default:
			panic(fmt.Sprintf("unhandled type %s", outputPhysType))
		}
	}

	if sel := batch.Selection(); sel != nil {
		sel = sel[:n]
		for _, i := range sel {
			for j := 0; j < batch.Width(); j++ {
				col := batch.ColVec(j)
				if col.MaybeHasNulls() && col.Nulls().NullAt(i) {
					d.row[j] = tree.DNull
				} else {
					d.row[j] = PhysicalTypeColElemToDatum(col, i, d.da, d.colTypes[j])
				}
			}

			d.resolved.row = d.row
			d.evalCtx.PushIVarContainer(d.resolved)
			// Because the typechecker has passed, we won't get an error here.
			res, _ := d.funcExpr.Eval(d.evalCtx)
			d.evalCtx.PopIVarContainer()
			writer(batch, res, int(i))
		}
	} else {
		for i := uint16(0); i < n; i++ {
			for j := 0; j < batch.Width(); j++ {
				col := batch.ColVec(j)
				if col.MaybeHasNulls() && col.Nulls().NullAt(i) {
					d.row[j] = tree.DNull
				} else {
					d.row[j] = PhysicalTypeColElemToDatum(col, i, d.da, d.colTypes[j])
				}
			}

			d.evalCtx.PushIVarContainer(&indexResolver{row: d.row, colTypes: d.colTypes})
			// Because the typechecker has passed, we won't get an error here.
			res, _ := d.funcExpr.Eval(d.evalCtx)
			d.evalCtx.PopIVarContainer()
			writer(batch, res, int(i))
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
) (Operator, error) {

	// For now, return the default builtin operator. Future work can specialize
	// out the operators to efficient implementations of specific builtins.
	op := &defaultBuiltinOperator{
		input:     input,
		evalCtx:   tctx,
		funcExpr:  funcExpr,
		outputIdx: outputIdx,
		colTypes:  columnTypes,
		row:       make(tree.Datums, len(columnTypes)),
		resolved:  &indexResolver{colTypes: columnTypes},
	}

	return op, nil
}
