// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// constructProjectForScope constructs a projection if it will result in a
// different set of columns than its input. Either way, it updates
// projectionsScope.group with the output memo group ID.
func (b *Builder) constructProjectForScope(inScope, projectionsScope *scope) {
	// Don't add an unnecessary "pass through" project.
	if projectionsScope.hasSameColumns(inScope) {
		projectionsScope.expr = inScope.expr
	} else {
		projectionsScope.expr = b.constructProject(
			inScope.expr.(memo.RelExpr),
			append(projectionsScope.cols, projectionsScope.extraCols...),
		)
	}
}

func (b *Builder) constructProject(input memo.RelExpr, cols []scopeColumn) memo.RelExpr {
	var passthrough opt.ColSet
	projections := make(memo.ProjectionsExpr, 0, len(cols))

	// Deduplicate the columns; we only need to project each column once.
	colSet := opt.ColSet{}
	for i := range cols {
		id, scalar := cols[i].id, cols[i].scalar
		if !colSet.Contains(id) {
			if scalar == nil {
				passthrough.Add(id)
			} else {
				projections = append(projections, memo.ProjectionsItem{
					Element:    scalar,
					ColPrivate: memo.ColPrivate{Col: id},
				})
			}
			colSet.Add(id)
		}
	}

	return b.factory.ConstructProject(input, projections, passthrough)
}

// dropOrderingAndExtraCols removes the ordering in the scope and projects away
// any extra columns.
func (b *Builder) dropOrderingAndExtraCols(s *scope) {
	s.ordering = nil
	if len(s.extraCols) > 0 {
		s.extraCols = nil
		s.expr = b.constructProject(s.expr, s.cols)
	}
}

// analyzeProjectionList analyzes the given list of SELECT clause expressions,
// and adds the resulting aliases and typed expressions to outScope. See the
// header comment for analyzeSelectList.
func (b *Builder) analyzeProjectionList(
	selects tree.SelectExprs, desiredTypes []*types.T, inScope, outScope *scope,
) {
	// We need to save and restore the previous values of the replaceSRFs field
	// and the field in semaCtx in case we are recursively called within a
	// subquery context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	defer func(replaceSRFs bool) { inScope.replaceSRFs = replaceSRFs }(inScope.replaceSRFs)

	b.semaCtx.Properties.Require("SELECT", tree.RejectNestedGenerators)
	inScope.context = "SELECT"
	inScope.replaceSRFs = true

	b.analyzeSelectList(selects, desiredTypes, inScope, outScope)
}

// analyzeReturningList analyzes the given list of RETURNING clause expressions,
// and adds the resulting aliases and typed expressions to outScope. See the
// header comment for analyzeSelectList.
func (b *Builder) analyzeReturningList(
	returning tree.ReturningExprs, desiredTypes []*types.T, inScope, outScope *scope,
) {
	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)

	// Ensure there are no special functions in the RETURNING clause.
	b.semaCtx.Properties.Require("RETURNING", tree.RejectSpecial)
	inScope.context = "RETURNING"

	b.analyzeSelectList(tree.SelectExprs(returning), desiredTypes, inScope, outScope)
}

// analyzeSelectList is a helper function used by analyzeProjectionList and
// analyzeReturningList. It normalizes names, expands wildcards, resolves types,
// and adds resulting columns to outScope. The desiredTypes slice contains
// target type hints for the resulting expressions.
//
// As a side-effect, the appropriate scopes are updated with aggregations
// (scope.groupby.aggs)
func (b *Builder) analyzeSelectList(
	selects tree.SelectExprs, desiredTypes []*types.T, inScope, outScope *scope,
) {
	for i, e := range selects {
		// Start with fast path, looking for simple column reference.
		texpr := b.resolveColRef(e.Expr, inScope)
		if texpr == nil {
			// Fall back to slow path. Pre-normalize any VarName so the work is
			// not done twice below.
			if err := e.NormalizeTopLevelVarName(); err != nil {
				panic(err)
			}

			// Special handling for "*", "<table>.*" and "(Expr).*".
			if v, ok := e.Expr.(tree.VarName); ok {
				switch v.(type) {
				case tree.UnqualifiedStar, *tree.AllColumnsSelector, *tree.TupleStar:
					if e.As != "" {
						panic(pgerror.Newf(pgcode.Syntax,
							"%q cannot be aliased", tree.ErrString(v)))
					}

					aliases, exprs := b.expandStar(e.Expr, inScope)
					if outScope.cols == nil {
						outScope.cols = make([]scopeColumn, 0, len(selects)+len(exprs)-1)
					}
					for j, e := range exprs {
						b.addColumn(outScope, aliases[j], e)
					}
					continue
				}
			}

			desired := types.Any
			if i < len(desiredTypes) {
				desired = desiredTypes[i]
			}

			texpr = inScope.resolveType(e.Expr, desired)
		}

		// Output column names should exactly match the original expression, so we
		// have to determine the output column name before we perform type
		// checking.
		if outScope.cols == nil {
			outScope.cols = make([]scopeColumn, 0, len(selects))
		}
		alias := b.getColName(e)
		b.addColumn(outScope, alias, texpr)
	}
}

// buildProjectionList builds a set of memo groups that represent the given
// expressions in projectionsScope.
//
// See Builder.buildStmt for a description of the remaining input values.
func (b *Builder) buildProjectionList(inScope *scope, projectionsScope *scope) {
	for i := range projectionsScope.cols {
		col := &projectionsScope.cols[i]
		b.buildScalar(col.getExpr(), inScope, projectionsScope, col, nil)
	}
}

// resolveColRef looks for the common case of a standalone column reference
// expression, like this:
//
//   SELECT ..., c, ... FROM ...
//
// It resolves the column name to a scopeColumn and returns it as a TypedExpr.
func (b *Builder) resolveColRef(e tree.Expr, inScope *scope) tree.TypedExpr {
	unresolved, ok := e.(*tree.UnresolvedName)
	if ok && !unresolved.Star && unresolved.NumParts == 1 {
		colName := unresolved.Parts[0]
		_, srcMeta, _, err := inScope.FindSourceProvidingColumn(b.ctx, tree.Name(colName))
		if err != nil {
			panic(err)
		}
		return srcMeta.(tree.TypedExpr)
	}
	return nil
}

// getColName returns the output column name for a projection expression.
func (b *Builder) getColName(expr tree.SelectExpr) string {
	s, err := tree.GetRenderColName(b.semaCtx.SearchPath, expr)
	if err != nil {
		panic(err)
	}
	return s
}

// finishBuildScalar completes construction of a new scalar expression. If
// outScope is nil, then finishBuildScalar returns the result memo group, which
// can be nested within the larger expression being built. If outScope is not
// nil, then finishBuildScalar synthesizes a new output column in outScope with
// the expression as its value.
//
// texpr     The given scalar expression. The expression is any scalar
//           expression except for a bare variable or aggregate (those are
//           handled separately in buildVariableProjection and
//           buildFunction).
// scalar    The memo expression that has already been built for the given
//           typed expression.
// outCol    The output column of the scalar which is being built. It can be
//           nil if outScope is nil.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) finishBuildScalar(
	texpr tree.TypedExpr, scalar opt.ScalarExpr, inScope, outScope *scope, outCol *scopeColumn,
) (out opt.ScalarExpr) {
	if outScope == nil {
		return scalar
	}

	// Avoid synthesizing a new column if possible.
	if col := outScope.findExistingCol(texpr); col != nil && col != outCol {
		outCol.id = col.id
		outCol.scalar = scalar
		return scalar
	}

	b.populateSynthesizedColumn(outCol, scalar)
	return scalar
}

// finishBuildScalarRef constructs a reference to the given column. If outScope
// is nil, then finishBuildScalarRef returns a Variable expression that refers
// to the column. This expression can be nested within the larger expression
// being constructed. If outScope is not nil, then finishBuildScalarRef adds the
// column to outScope, either as a passthrough column (if it already exists in
// the input scope), or a variable expression.
//
// col      Column containing the scalar expression that's been referenced.
// outCol   The output column which is being built. It can be nil if outScope is
//          nil.
// colRefs  The set of columns referenced so far by the scalar expression being
//          built. If not nil, it is updated with the ID of this column.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) finishBuildScalarRef(
	col *scopeColumn, inScope, outScope *scope, outCol *scopeColumn, colRefs *opt.ColSet,
) (out opt.ScalarExpr) {
	// Update the sets of column references and outer columns if needed.
	if colRefs != nil {
		colRefs.Add(col.id)
	}

	// Collect the outer columns of the current subquery, if any.
	isOuterColumn := inScope == nil || inScope.isOuterColumn(col.id)
	if isOuterColumn && b.subquery != nil {
		b.subquery.outerCols.Add(col.id)
	}

	// If this is not a projection context, then wrap the column reference with
	// a Variable expression that can be embedded in outer expression(s).
	if outScope == nil {
		return b.factory.ConstructVariable(col.id)
	}

	// Outer columns must be wrapped in a variable expression and assigned a new
	// column id before projection.
	if isOuterColumn {
		// Avoid synthesizing a new column if possible.
		existing := outScope.findExistingCol(col)
		if existing == nil || existing == outCol {
			if outCol.name == "" {
				outCol.name = col.name
			}
			group := b.factory.ConstructVariable(col.id)
			b.populateSynthesizedColumn(outCol, group)
			return group
		}

		col = existing
	}

	// Project the column.
	b.projectColumn(outCol, col)
	return outCol.scalar
}
