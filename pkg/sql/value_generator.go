// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// valueGenerator represents a node that produces rows
// computationally, by means of a "generator function" (called
// "set-generating function" in PostgreSQL).
type valueGenerator struct {
	// expr holds the function call that needs to be performed,
	// including its arguments that need evaluation, to obtain the
	// generator object.
	expr tree.TypedExpr

	// columns is the signature of this generator.
	columns sqlbase.ResultColumns

	run valueGeneratorRun
}

// makeGenerator creates a valueGenerator instance that wraps a call to a
// generator function.
func (p *planner) makeGenerator(ctx context.Context, t *tree.FuncExpr) (planNode, error) {
	if err := p.txCtx.AssertNoAggregationOrWindowing(
		t, "FROM", p.SessionData().SearchPath,
	); err != nil {
		return nil, err
	}

	lastKnownSubqueryIndex := len(p.curPlan.subqueryPlans)
	normalized, err := p.analyzeExpr(
		ctx, t, sqlbase.MultiSourceInfo{}, tree.IndexedVarHelper{}, types.Any, false, "FROM",
	)
	if err != nil {
		return nil, err
	}
	isConst := (len(p.curPlan.subqueryPlans) == lastKnownSubqueryIndex)

	if tType, ok := normalized.ResolvedType().(types.TTable); ok {
		// Set-generating functions: generate_series() etc.
		columns := make(sqlbase.ResultColumns, len(tType.Cols))
		for i := range columns {
			columns[i].Name = tType.Labels[i]
			columns[i].Typ = tType.Cols[i]
		}

		return &valueGenerator{
			expr:    normalized,
			columns: columns,
		}, nil
	}

	// Scalar functions: cos, etc.
	return &valuesNode{
		columns:          sqlbase.ResultColumns{{Name: t.Func.String(), Typ: normalized.ResolvedType()}},
		tuples:           [][]tree.TypedExpr{{normalized}},
		isConst:          isConst,
		specifiedInQuery: true,
	}, nil
}

// valueGeneratorRun contains the run-time state of valueGenerator
// during local execution.
type valueGeneratorRun struct {
	// gen is a reference to the generator object that produces the row
	// for this planNode.
	gen tree.ValueGenerator
}

func (n *valueGenerator) startExec(params runParams) error {
	expr, err := n.expr.Eval(params.EvalContext())
	if err != nil {
		return err
	}
	var tb *tree.DTable
	if expr == tree.DNull {
		tb = builtins.EmptyDTable()
	} else {
		tb = expr.(*tree.DTable)
	}

	gen := tb.ValueGenerator
	if err := gen.Start(); err != nil {
		return err
	}

	n.run.gen = gen
	return nil
}

func (n *valueGenerator) Next(params runParams) (bool, error) {
	if err := params.p.cancelChecker.Check(); err != nil {
		return false, err
	}
	return n.run.gen.Next()
}
func (n *valueGenerator) Values() tree.Datums { return n.run.gen.Values() }

func (n *valueGenerator) Close(context.Context) {
	if n.run.gen != nil {
		n.run.gen.Close()
	}
}
