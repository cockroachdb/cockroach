// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// constructProject constructs a projection if it will result in a different
// set of columns than its input. Either way, it updates projectionsScope.group
// with the output memo group ID.
func (b *Builder) constructProject(inScope, projectionsScope *scope) {
	// Don't add an unnecessary "pass through" project expression.
	if projectionsScope.hasSameColumns(inScope) {
		projectionsScope.group = inScope.group
	} else {
		p := b.constructList(opt.ProjectionsOp, projectionsScope.cols)
		projectionsScope.group = b.factory.ConstructProject(inScope.group, p)
	}
}

// buildProjectionList builds a set of memo groups that represent the given
// list of select expressions.
//
// See Builder.buildStmt for a description of the remaining input values.
//
// As a side-effect, the appropriate scopes are updated with aggregations
// (scope.groupby.aggs)
func (b *Builder) buildProjectionList(selects tree.SelectExprs, inScope *scope, outScope *scope) {
	for _, e := range selects {
		b.buildProjection(e.Expr, string(e.As), inScope, outScope)
	}
}

// buildProjection builds a set of memo groups that represent a projection
// expression.
//
// projection  The given projection expression.
// label       If a new column is synthesized (e.g., for a scalar expression),
//             it will be labeled with this string.
//
// See Builder.buildStmt for a description of the remaining input values.
func (b *Builder) buildProjection(projection tree.Expr, label string, inScope, outScope *scope) {
	exprs := b.expandStarAndResolveType(projection, inScope)
	if len(exprs) > 1 && label != "" {
		panic(errorf("\"%s\" cannot be aliased", projection))
	}

	for _, e := range exprs {
		b.buildScalarProjection(e, label, inScope, outScope)
	}
}

// buildScalarProjection builds a set of memo groups that represent a scalar
// expression.
//
// texpr   The given scalar expression.
// label   If a new column is synthesized, it will be labeled with this string.
//         For example, the query `SELECT (x + 1) AS "x_incr" FROM t` has a
//         projection with a synthesized column "x_incr".
//
// The return value corresponds to the new column which has been created for
// this scalar expression.
//
// See Builder.buildStmt for a description of the remaining input values.
func (b *Builder) buildScalarProjection(
	texpr tree.TypedExpr, label string, inScope, outScope *scope,
) *scopeColumn {
	// NB: The case statements are sorted lexicographically.
	switch t := texpr.(type) {
	case *scopeColumn:
		b.buildVariableProjection(t, label, inScope, outScope)

	case *tree.FuncExpr:
		out, col := b.buildFunction(t, label, inScope)
		if col != nil {
			// Function was mapped to a column reference, such as in the case
			// of an aggregate.
			outScope.cols = append(outScope.cols, *col)
		} else {
			b.buildDefaultScalarProjection(texpr, out, label, inScope, outScope)
		}

	case *tree.ParenExpr:
		b.buildScalarProjection(t.TypedInnerExpr(), label, inScope, outScope)

	default:
		out := b.buildScalar(texpr, inScope)
		b.buildDefaultScalarProjection(texpr, out, label, inScope, outScope)
	}

	return &outScope.cols[len(outScope.cols)-1]
}

// buildVariableProjection builds a memo group that represents the given
// column. label contains an optional alias for the column (e.g., if specified
// with the AS keyword).
//
// See Builder.buildStmt for a description of the remaining input values.
func (b *Builder) buildVariableProjection(
	col *scopeColumn, label string, inScope, outScope *scope,
) {
	if inScope.inGroupingContext() && !inScope.groupby.inAgg && !inScope.groupby.aggInScope.hasColumn(col.id) {
		panic(groupingError(col.String()))
	}
	col = outScope.appendColumn(col, label)

	// Mark the column as a visible member of an anonymous table.
	col.table.TableName = ""
	col.hidden = false

	col.group = b.factory.ConstructVariable(b.factory.InternColumnID(col.id))
}

// buildDefaultScalarProjection builds a set of memo groups that represent
// a scalar expression.
//
// texpr     The given scalar expression. The expression is any scalar
//           expression except for a bare variable or aggregate (those are
//           handled separately in buildVariableProjection and
//           buildFunction).
// group     The memo group that has already been built for the given
//           expression. It may be replaced by a variable reference if the
//           expression already exists (e.g., as a GROUP BY column).
// label     If a new column is synthesized, it will be labeled with this
//           string.
//
// See Builder.buildStmt for a description of the remaining input values.
func (b *Builder) buildDefaultScalarProjection(
	texpr tree.TypedExpr, group memo.GroupID, label string, inScope, outScope *scope,
) {
	if inScope.inGroupingContext() && !inScope.groupby.inAgg {
		if len(inScope.groupby.varsUsed) > 0 {
			if _, ok := inScope.groupby.groupStrs[symbolicExprStr(texpr)]; !ok {
				// This expression was not found among the GROUP BY expressions.
				i := inScope.groupby.varsUsed[0]
				col := b.colMap[i]
				panic(groupingError(col.String()))
			}

			// Reset varsUsed for the next projection.
			inScope.groupby.varsUsed = inScope.groupby.varsUsed[:0]
		}

		if col := inScope.findGrouping(group); col != nil {
			// The column already exists, so use that instead.
			col = outScope.appendColumn(col, label)

			// Replace the expression with a reference to the column.
			col.group = b.factory.ConstructVariable(b.factory.InternColumnID(col.id))
			return
		}
	}

	// Avoid synthesizing a new column if possible.
	if col := outScope.findExistingCol(texpr); col != nil {
		col = outScope.appendColumn(col, label)
		col.group = group
		return
	}

	b.synthesizeColumn(outScope, label, texpr.ResolvedType(), texpr, group)
}
