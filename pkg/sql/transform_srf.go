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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// srfExtractionVisitor replaces the inner set-returning function(s) in an
// expression with an expression that accesses the relevant
// columns in the FROM clause.
//
// This visitor is intentionally limited to extracting only one SRF, because we
// don't support lateral correlated subqueries.
type srfExtractionVisitor struct {
	err        error
	ctx        context.Context
	p          *planner
	r          *renderNode
	searchPath sessiondata.SearchPath
	seenSRF    bool
}

var _ tree.Visitor = &srfExtractionVisitor{}

func (v *srfExtractionVisitor) VisitPre(expr tree.Expr) (recurse bool, newNode tree.Expr) {
	if v.err != nil {
		return false, expr
	}
	// If a scalar sub-expression is a subquery, this will be handled
	// separately in the latest step by computeRenderAllowingStar(). We
	// do not want to recurse and replace SRFs here, because that work
	// will be done for the subquery later.
	_, isSubquery := expr.(*tree.Subquery)
	if isSubquery {
		return false, expr
	}

	switch t := expr.(type) {
	case *tree.ColumnAccessExpr:
		fe, ok := t.Expr.(*tree.FuncExpr)
		if !ok {
			// If there is no function inside the column access expression, then it
			// will be dealt with elsewhere.
			return false, expr
		}
		newFe := *fe
		newFe.EscapeSRF = true
		newCAE := *t
		newCAE.Expr = &newFe
		expr = &newCAE
	}
	return true, expr
}

func (v *srfExtractionVisitor) VisitPost(expr tree.Expr) tree.Expr {
	if v.err != nil {
		return expr
	}
	switch t := expr.(type) {
	case *tree.ColumnAccessExpr:
		// TODO(knz): support arbitrary composite expressions.
		fe, ok := t.Expr.(*tree.FuncExpr)
		if !ok || !fe.EscapeSRF {
			v.err = pgerror.NewErrorf(pgerror.CodeInternalError,
				"programmer error: invalid Walk recursion on ColumnAccessExpr")
			return expr
		}
		foundSRF, srfName, err := v.lookupSRF(fe)
		if err != nil {
			v.err = err
			return expr
		}
		if !foundSRF {
			v.err = pgerror.UnimplementedWithIssueErrorf(24866,
				"composite-returning scalar functions: %q", tree.ErrString(t.Expr))
			return expr
		}
		newExpr, err := v.transformSRF(srfName, fe, t)
		if err != nil {
			v.err = err
			return expr
		}
		return newExpr
	case *tree.FuncExpr:
		if t.EscapeSRF {
			return expr
		}
		foundSRF, srfName, err := v.lookupSRF(t)
		if err != nil {
			v.err = err
			return expr
		}
		if foundSRF {
			newExpr, err := v.transformSRF(srfName, t, nil /* columnAccess */)
			if err != nil {
				v.err = err
				return expr
			}
			return newExpr
		}
	}
	return expr
}

func (v *srfExtractionVisitor) lookupSRF(t *tree.FuncExpr) (bool, string, error) {
	fd, err := t.Func.Resolve(v.searchPath)
	if err != nil {
		return false, "", err
	}
	if fd.Class == tree.GeneratorClass {
		return true, fd.Name, nil
	}
	return false, "", nil
}

// rewriteSRFs creates data sources for any set-returning functions in the
// provided render expression, cross-joins these data sources with the
// renderNode's existing data sources, and returns a new render expression with
// the set-returning function replaced by an IndexedVar that points at the new
// data source.
//
// Expressions with more than one SRF require lateral correlated subqueries,
// which are not yet supported. For now, this function returns an error if more
// than one SRF is present in the render expression.
func (p *planner) rewriteSRFs(
	ctx context.Context, r *renderNode, targets tree.SelectExprs,
) (tree.SelectExprs, error) {
	// Walk the render expression looking for SRFs.
	v := &p.srfExtractionVisitor

	// Ensure that the previous visitor state is preserved during the
	// analysis below. This is especially important when recursing into
	// scalar subqueries, which may run this analysis too.
	defer func(p *planner, prevV srfExtractionVisitor) { p.srfExtractionVisitor = prevV }(p, *v)
	*v = srfExtractionVisitor{
		err:        nil,
		ctx:        ctx,
		p:          p,
		r:          r,
		seenSRF:    false,
		searchPath: p.SessionData().SearchPath,
	}

	// Rewrite the SRFs, if any. We want to avoid re-allocating a targets
	// array unless necessary.
	var newTargets tree.SelectExprs
	for i, target := range targets {
		newExpr, changed := tree.WalkExpr(v, target.Expr)
		if v.err != nil {
			return targets, v.err
		}
		if !changed {
			if newTargets != nil {
				newTargets[i] = target
			}
			continue
		}

		if newTargets == nil {
			newTargets = make(tree.SelectExprs, len(targets))
			copy(newTargets, targets[:i])
		}
		newTargets[i].Expr = newExpr
		newTargets[i].As = target.As
	}
	if newTargets != nil {
		return newTargets, nil
	}
	return targets, nil
}

func (v *srfExtractionVisitor) transformSRF(
	srfName string, srf *tree.FuncExpr, columnAccessExpr *tree.ColumnAccessExpr,
) (tree.Expr, error) {
	if v.seenSRF {
		// TODO(knz/jordan): multiple SRFs in the same scalar expression
		// should be migrated to a _single_ join operand in the FROM
		// clause, that zip the values togethers into a single table
		// (filling with NULLs where some SRFs produce fewer rows than
		// others).
		return nil, pgerror.UnimplementedWithIssueErrorf(20511,
			"cannot use multiple set-returning functions in a single SELECT clause: %q",
			tree.ErrString(srf))
	}
	v.seenSRF = true

	// Transform the SRF. This must be done in any case, whether the SRF
	// was used as scalar or whether a specific column was requested.
	tblName := tree.MakeUnqualifiedTableName(tree.Name(srfName))
	src, err := v.p.getPlanForRowsFrom(v.ctx, tblName, srf)
	if err != nil {
		return nil, err
	}

	// Save the SRF data source for later.
	srfSource := src

	// Do we already have something in the FROM clause?
	if !isUnarySource(v.r.source) {
		// The FROM clause specifies something. Replace with a cross-join (which is
		// an inner join without a condition).
		src, err = v.p.makeJoin(v.ctx, sqlbase.InnerJoin, v.r.source, src, nil)
		if err != nil {
			return nil, err
		}
	}
	// Keep the remaining relational expression.
	v.r.source = src
	v.r.sourceInfo = sqlbase.MultiSourceInfo{src.info}

	// Now decide what to render.
	if columnAccessExpr != nil {
		// Syntax: (tbl).colname or (tbl).*
		if columnAccessExpr.Star {
			// (tbl).*
			// We want all the columns as separate renders. Let the star
			// expansion that occurs later do the job.
			return &tree.AllColumnsSelector{TableName: tree.MakeUnresolvedName(srfName)}, nil
		}
		// (tbl).colname
		// We just want one column. Render that.
		return tree.NewColumnItem(&tblName, tree.Name(columnAccessExpr.ColName)), nil
	}

	// Just the SRF name. Check whether we have just one column or more.
	if len(srfSource.info.SourceColumns) == 1 {
		// Just one column. Render that column exactly.
		return tree.NewColumnItem(&tblName, tree.Name(srfSource.info.SourceColumns[0].Name)), nil
	}
	// More than one column.
	// We want all the columns as a tuple. Render that.
	// TODO(knz): This should be returned as a composite expression instead
	// (tuple with labels). See #24866.
	tExpr := &tree.Tuple{
		Exprs: make(tree.Exprs, len(srfSource.info.SourceColumns)),
	}
	for i, c := range srfSource.info.SourceColumns {
		tExpr.Exprs[i] = tree.NewColumnItem(&tblName, tree.Name(c.Name))
	}
	return tExpr, nil
}

// A unary source is the special source used with empty FROM clauses:
// a pseudo-table with zero columns and exactly one row.
func isUnarySource(src planDataSource) bool {
	_, ok := src.plan.(*unaryNode)
	return ok && len(src.info.SourceColumns) == 0
}
