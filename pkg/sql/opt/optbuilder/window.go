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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
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

	// orderBy is the set of expressions used in the ORDER BY clause.
	orderBy tree.OrderBy

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
	orderings := make([]physical.OrderingChoice, len(inScope.windows))
	argScope := outScope.push()
	argScope.appendColumnsFromScope(outScope)
	// The arguments to a given window function need to be columns in the input
	// relation. Build a projection that produces those values to go underneath
	// the window functions.
	// TODO(justin): this is unfortunate in common cases where the arguments are
	// constant, since we'll be projecting an extra column in every row.  It
	// would be good if the windower supported being specified with constant
	// values.
	for i := range inScope.windows {
		w := inScope.windows[i].expr.(*windowInfo)
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

		// PARTITION BY (a, b) => PARTITION BY a, b
		cols := flattenTuples(w.partition)
		for j, e := range cols {
			col := argScope.findExistingCol(e)
			if col == nil {
				col = b.synthesizeColumn(
					argScope,
					fmt.Sprintf("%s_%d_partition_%d", w.def.Name, i+1, j+1),
					e.ResolvedType(),
					e,
					b.buildScalar(e, inScope, nil, nil, nil),
				)
			}
			partitions[i].Add(int(col.id))
		}

		ord := make(opt.Ordering, 0, len(w.orderBy))
		for j, t := range w.orderBy {
			// ORDER BY (a, b) => ORDER BY a, b
			cols := flattenTuples([]tree.TypedExpr{t.Expr.(tree.TypedExpr)})

			for _, e := range cols {
				col := argScope.findExistingCol(e)
				if col == nil {
					col = b.synthesizeColumn(
						argScope,
						fmt.Sprintf("%s_%d_orderby_%d", w.def.Name, i+1, j+1),
						e.ResolvedType(),
						e,
						b.buildScalar(e, inScope, nil, nil, nil),
					)
				}
				ord = append(ord, opt.MakeOrderingColumn(col.id, t.Direction == tree.Descending))
			}
		}
		orderings[i].FromOrdering(ord)
	}

	b.constructProjectForScope(outScope, argScope)
	outScope.expr = argScope.expr

	// frames accumulates the set of distinct window frames we're computing over
	// so that we can group functions over the same partition and ordering.
	frames := make([]memo.WindowExpr, 0, len(inScope.windows))
	for i := range inScope.windows {
		w := inScope.windows[i].expr.(*windowInfo)

		frameIdx := -1

		// The number of window functions is probably fairly small, so do an O(n^2)
		// loop.
		// TODO(justin): make this faster.
		// TODO(justin): consider coalescing frames with compatible orderings.
		for j := range frames {
			if partitions[i].Equals(frames[j].Partition) &&
				orderings[i].Equals(&frames[j].Ordering) {
				frameIdx = j
				break
			}
		}

		// If we can't reuse an existing frame, make a new one.
		if frameIdx == -1 {
			frames = append(frames, memo.WindowExpr{
				WindowPrivate: memo.WindowPrivate{
					Partition: partitions[i],
					Ordering:  orderings[i],
				},
				Windows: memo.WindowsExpr{},
			})
			frameIdx = len(frames) - 1
		}

		frames[frameIdx].Windows = append(frames[frameIdx].Windows,
			memo.WindowsItem{
				Function:   b.constructWindowFn(w.def.Name, argLists[i]),
				ColPrivate: memo.ColPrivate{Col: w.col.id},
			},
		)
	}

	for _, f := range frames {
		outScope.expr = b.factory.ConstructWindow(outScope.expr, f.Windows, &f.WindowPrivate)
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
