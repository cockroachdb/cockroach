// Copyright 2019 The Cockroach Authors.
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

package optbuilder

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// windowInfo stores information about a window function call.
type windowInfo struct {
	*tree.FuncExpr

	def memo.FunctionPrivate

	// partition is the set of expressions used in the PARTITION BY clause.
	partition []tree.TypedExpr

	// col is the output column of the aggregation.
	col *scopeColumn
}

// Walk is part of the tree.Expr interface.
func (w *windowInfo) Walk(v tree.Visitor) tree.Expr {
	return w
}

// TypeCheck is part of the tree.Expr interface.
func (w *windowInfo) TypeCheck(ctx *tree.SemaContext, desired *types.T) (tree.TypedExpr, error) {
	if _, err := w.FuncExpr.TypeCheck(ctx, desired); err != nil {
		return nil, err
	}
	return w, nil
}

// Eval is part of the tree.TypedExpr interface.
func (w *windowInfo) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic(pgerror.AssertionFailedf("windowInfo must be replaced before evaluation"))
}

var _ tree.Expr = &windowInfo{}
var _ tree.TypedExpr = &windowInfo{}

// buildWindow adds any window functions on top of the expression.
func (b *Builder) buildWindow(outScope *scope, inScope *scope) {
	if len(inScope.windows) == 0 {
		return
	}
	argLists := make([][]opt.ScalarExpr, len(inScope.windows))
	partitions := make([]opt.ColSet, len(inScope.windows))
	argScope := outScope.push()
	argScope.appendColumnsFromScope(outScope)
	// The arguments to a given window function need to be columns in the input
	// relation. Build a projection that produces those values to go underneath
	// the window functions.
	// TODO(justin): this is unfortunate in common cases where the arguments are
	// constant, since we'll be projecting an extra column in every row.  It
	// would be good if the windower supported being specified with constant
	// values.
	// TODO(justin): it's possible we could reuse some projections here, if a
	// window function takes a column directly.
	// TODO(justin): would it be better to just introduce a projection beneath
	// every window operator and then let norm rules push them all down and merge
	// them at the bottom, rather than building up one projection here?
	for i := range inScope.windows {
		w := &inScope.windows[i]
		argExprs := b.getTypedWindowArgs(w)

		argLists[i] = make(memo.ScalarListExpr, len(argExprs))
		for j, a := range argExprs {
			col := argScope.findExistingCol(a)
			if col == nil {
				col = b.synthesizeColumn(
					argScope,
					fmt.Sprintf("%s_%d_arg%d", w.def.Name, i+1, j+1),
					a.ResolvedType(),
					a,
					b.buildScalar(a, inScope, nil, nil, nil),
				)
			}
			argLists[i][j] = b.factory.ConstructVariable(col.id)
		}

		for j, t := range w.partition {
			col := argScope.findExistingCol(t)
			if col == nil {
				col = b.synthesizeColumn(
					argScope,
					fmt.Sprintf("%s_%d_partition_%d", w.def.Name, i+1, j+1),
					t.ResolvedType(),
					t,
					b.buildScalar(t, inScope, nil, nil, nil),
				)
			}
			partitions[i].Add(int(col.id))
		}
	}

	b.constructProjectForScope(outScope, argScope)
	outScope.expr = argScope.expr

	// Maintain the set of columns that the window function must pass through,
	// since the arguments to the upper window functions must be passed through
	// the lower window functions.
	for i := range inScope.windows {
		w := &inScope.windows[i]
		outScope.expr = b.factory.ConstructWindow(
			outScope.expr,
			b.constructWindowFn(w.def.Name, argLists[i]),
			&memo.WindowPrivate{
				ColID:     w.col.id,
				Partition: partitions[i],
			},
		)
	}
}

// getTypedWindowArgs returns the arguments to the window function as
// a []tree.TypedExpr. In the case of arguments with default values, it
// fills in the values if they are missing.
// TODO(justin): this is a bit of a hack to get around the fact that we don't
// have a good way to represent optional values in the opt tree, figure out
// a better way to do this. In particular this is bad because it results in us
// projecting the default argument to some window functions when we could just
// not do that projection.
func (b *Builder) getTypedWindowArgs(w *windowInfo) []tree.TypedExpr {
	argExprs := make([]tree.TypedExpr, len(w.Exprs))
	for i, pexpr := range w.Exprs {
		argExprs[i] = pexpr.(tree.TypedExpr)
	}

	switch w.def.Name {
	// The second argument of {lead,lag} is 1 by default, and the third argument
	// is NULL by default.
	case "lead", "lag":
		if len(argExprs) < 2 {
			argExprs = append(argExprs, tree.NewDInt(1))
		}
		if len(argExprs) < 3 {
			null, err := tree.ReType(tree.DNull, argExprs[0].ResolvedType())
			if err != nil {
				panic(pgerror.NewAssertionErrorWithWrappedErrf(err, "error calling tree.ReType"))
			}
			argExprs = append(argExprs, null)
		}
	}

	return argExprs
}
