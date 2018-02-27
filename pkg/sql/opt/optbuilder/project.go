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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// buildProjectionList builds a set of memo groups that represent the given
// list of select expressions.
//
// The first return value `projections` is an ordered list of top-level memo
// groups corresponding to each select expression. See Builder.buildStmt above
// for a description of the remaining input and return values.
//
// As a side-effect, the appropriate scopes are updated with aggregations
// (scope.groupby.aggs)
func (b *Builder) buildProjectionList(
	selects tree.SelectExprs, inScope *scope, outScope *scope,
) (projections []opt.GroupID) {
	projections = make([]opt.GroupID, 0, len(selects))
	for _, e := range selects {
		subset := b.buildProjection(e.Expr, string(e.As), inScope, outScope)
		projections = append(projections, subset...)
	}

	return projections
}

// buildProjection builds a set of memo groups that represent a projection
// expression.
//
// projection  The given projection expression.
// label       If a new column is synthesized (e.g., for a scalar expression),
//             it will be labeled with this string.
//
// The return value `projections` is an ordered list of top-level memo
// groups corresponding to the expression. The list generally consists of a
// single memo group except in the case of "*", where the expression is
// expanded to multiple columns.
//
// See Builder.buildStmt for a description of the remaining input values
// (outScope is passed as a parameter here rather than a return value because
// the newly bound variables are appended to a growing list to be returned by
// buildProjectionList).
func (b *Builder) buildProjection(
	projection tree.Expr, label string, inScope, outScope *scope,
) (projections []opt.GroupID) {
	exprs := b.expandStarAndResolveType(projection, inScope)
	if len(exprs) > 1 && label != "" {
		panic(errorf("\"%s\" cannot be aliased", projection))
	}

	projections = make([]opt.GroupID, 0, len(exprs))
	for _, e := range exprs {
		projections = append(projections, b.buildScalarProjection(e, label, inScope, outScope))
	}

	return projections
}

// expandStarAndResolveType expands expr into a list of columns if expr
// corresponds to a "*" or "<table>.*". Otherwise, expandStarAndResolveType
// resolves the type of expr and returns it as a []TypedExpr.
func (b *Builder) expandStarAndResolveType(
	expr tree.Expr, inScope *scope,
) (exprs []tree.TypedExpr) {
	// NB: The case statements are sorted lexicographically.
	switch t := expr.(type) {
	case *tree.AllColumnsSelector:
		tn, err := tree.NormalizeTableName(&t.TableName)
		if err != nil {
			panic(builderError{err})
		}

		numRes, src, _, err := inScope.FindSourceMatchingName(b.ctx, tn)
		if err != nil {
			panic(builderError{err})
		}
		if numRes == tree.NoResults {
			panic(builderError{pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
				"no data source named %q", tree.ErrString(&tn))})
		}

		for i := range inScope.cols {
			col := inScope.cols[i]
			if col.table == *src && !col.hidden {
				exprs = append(exprs, &col)
			}
		}

	case tree.UnqualifiedStar:
		for i := range inScope.cols {
			col := inScope.cols[i]
			if !col.hidden {
				exprs = append(exprs, &col)
			}
		}
		if len(exprs) == 0 {
			panic(errorf("failed to expand *"))
		}

	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(builderError{err})
		}
		return b.expandStarAndResolveType(vn, inScope)

	default:
		texpr := inScope.resolveType(t, types.Any)
		exprs = []tree.TypedExpr{texpr}
	}

	return exprs
}

// buildScalarProjection builds a set of memo groups that represent a scalar
// expression.
//
// texpr   The given scalar expression.
// label   If a new column is synthesized, it will be labeled with this string.
//         For example, the query `SELECT (x + 1) AS "x_incr" FROM t` has a
//         projection with a synthesized column "x_incr".
//
// The return value corresponds to the top-level memo group ID for this scalar
// expression.
//
// See Builder.buildStmt for a description of the remaining input values
// (outScope is passed as a parameter here rather than a return value because
// the newly bound variables are appended to a growing list to be returned by
// buildProjectionList).
func (b *Builder) buildScalarProjection(
	texpr tree.TypedExpr, label string, inScope, outScope *scope,
) opt.GroupID {
	// NB: The case statements are sorted lexicographically.
	switch t := texpr.(type) {
	case *columnProps:
		return b.buildVariableProjection(t, label, inScope, outScope)

	case *tree.FuncExpr:
		out, col := b.buildFunction(t, label, inScope)
		if col != nil {
			// Function was mapped to a column reference, such as in the case
			// of an aggregate.
			outScope.cols = append(outScope.cols, *col)
		} else {
			out = b.buildDefaultScalarProjection(texpr, out, label, inScope, outScope)
		}
		return out

	case *tree.ParenExpr:
		return b.buildScalarProjection(t.TypedInnerExpr(), label, inScope, outScope)

	default:
		out := b.buildScalar(texpr, inScope)
		out = b.buildDefaultScalarProjection(texpr, out, label, inScope, outScope)
		return out
	}
}

// buildVariableProjection builds a memo group that represents the given
// column. label contains an optional alias for the column (e.g., if specified
// with the AS keyword).
//
// The return value corresponds to the top-level memo group ID for this scalar
// expression.
//
// See Builder.buildStmt for a description of the remaining input values
// (outScope is passed as a parameter here rather than a return value because
// the newly bound variables are appended to a growing list to be returned by
// buildProjectionList).
func (b *Builder) buildVariableProjection(
	col *columnProps, label string, inScope, outScope *scope,
) opt.GroupID {
	if inScope.inGroupingContext() && !inScope.groupby.inAgg && !inScope.groupby.aggInScope.hasColumn(col.index) {
		panic(groupingError(col.String()))
	}
	out := b.factory.ConstructVariable(b.factory.InternPrivate(col.index))
	outScope.cols = append(outScope.cols, *col)

	// Update the column name with the alias if it exists, and mark the column
	// as a visible member of an anonymous table.
	col = &outScope.cols[len(outScope.cols)-1]
	if label != "" {
		col.name = tree.Name(label)
	}
	col.table.TableName = ""
	col.hidden = false
	return out
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
// The return value corresponds to the top-level memo group ID for this scalar
// expression.
//
// See Builder.buildStmt for a description of the remaining input values
// (outScope is passed as a parameter here rather than a return value because
// the newly bound variables are appended to a growing list to be returned by
// buildProjectionList).
func (b *Builder) buildDefaultScalarProjection(
	texpr tree.TypedExpr, group opt.GroupID, label string, inScope, outScope *scope,
) opt.GroupID {
	if inScope.inGroupingContext() {
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
			col = &b.colMap[col.index]
			if label != "" {
				col.name = tree.Name(label)
			}
			outScope.cols = append(outScope.cols, *col)

			// Replace the expression with a reference to the column.
			return b.factory.ConstructVariable(b.factory.InternPrivate(col.index))
		}
	}

	b.synthesizeColumn(outScope, label, texpr.ResolvedType())
	return group
}

// synthesizeColumn is used to synthesize new columns. This is needed for
// operations such as projection of scalar expressions and aggregations. For
// example, the query `SELECT (x + 1) AS "x_incr" FROM t` has a projection with
// a synthesized column "x_incr".
//
// scope  The scope is passed in so it can can be updated with the newly bound
//        variable.
// label  This is an optional label for the new column (e.g., if specified with
//        the AS keyword).
// typ    The type of the column.
//
// The new column is returned as a columnProps object.
func (b *Builder) synthesizeColumn(scope *scope, label string, typ types.T) *columnProps {
	if label == "" {
		label = fmt.Sprintf("column%d", len(b.colMap))
	}

	colIndex := b.factory.Metadata().AddColumn(label, typ)
	col := columnProps{name: tree.Name(label), typ: typ, index: colIndex}
	b.colMap = append(b.colMap, col)
	scope.cols = append(scope.cols, col)
	return &scope.cols[len(scope.cols)-1]
}

// constructList invokes the factory to create one of the operators that contain
// a list of groups: ProjectionsOp and AggregationsOp.
func (b *Builder) constructList(
	op opt.Operator, items []opt.GroupID, cols []columnProps,
) opt.GroupID {
	colList := make(opt.ColList, len(cols))
	for i := range cols {
		colList[i] = cols[i].index
	}

	list := b.factory.InternList(items)
	private := b.factory.InternPrivate(&colList)

	switch op {
	case opt.ProjectionsOp:
		return b.factory.ConstructProjections(list, private)
	case opt.AggregationsOp:
		return b.factory.ConstructAggregations(list, private)
	}

	panic(fmt.Sprintf("unexpected operator: %s", op))
}
