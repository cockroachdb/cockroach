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
	"fmt"

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
	searchPath sessiondata.SearchPath

	// origAliases are the known table/column aliases for the original
	// source.  We only initialize sourceAliases below from this if SRFs
	// are actually found.
	origAliases sqlbase.SourceAliases

	// srfs contains the SRFs collected so far. This list is processed
	// at the end of the visit to generate a projectSetNode.
	srfs tree.Exprs

	// sourceAliases contains the extended source aliases. It contains
	// unique names for each of the collected SRF expressions, so that
	// columns from the same SRF function name occurring in multiple
	// places can be disambiguated.
	sourceAliases sqlbase.SourceAliases

	// numColumns is the number of columns seen so far in the data
	// source extended with the SRFs collected so far.
	numColumns int

	// seenSRF is a counter indicating the level of nesting of SRFs. We
	// reject anything greated than 1 for now as unsupported.
	seenSRF int
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

	case *tree.FuncExpr:
		// For each SRF encountered on the way down, remember the nesting
		// level. It will be decreased on the way up in VisitPost()
		// below.
		fd, err := v.lookupSRF(t)
		if err != nil {
			v.err = err
			return false, expr
		}
		if fd != nil {
			v.seenSRF++
		}
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
		fd, err := v.lookupSRF(fe)
		if err != nil {
			v.err = err
			return expr
		}
		if fd == nil {
			v.err = pgerror.UnimplementedWithIssueErrorf(24866,
				"composite-returning scalar functions: %q", tree.ErrString(t.Expr))
			return expr
		}
		newExpr, err := v.transformSRF(fe, fd, t)
		if err != nil {
			v.err = err
			return expr
		}
		v.seenSRF--
		return newExpr

	case *tree.FuncExpr:
		if t.EscapeSRF {
			return expr
		}
		fd, err := v.lookupSRF(t)
		if err != nil {
			v.err = err
			return expr
		}
		if fd != nil {
			newExpr, err := v.transformSRF(t, fd, nil /* columnAccess */)
			if err != nil {
				v.err = err
				return expr
			}
			v.seenSRF--
			return newExpr
		}
	}
	return expr
}

func (v *srfExtractionVisitor) lookupSRF(t *tree.FuncExpr) (*tree.FunctionDefinition, error) {
	fd, err := t.Func.Resolve(v.searchPath)
	if err != nil {
		return nil, err
	}
	if fd.Class != tree.GeneratorClass {
		fd = nil
	}
	return fd, nil
}

// rewriteSRFs creates data sources for any set-returning functions in the
// provided render expression, cross-joins these data sources with the
// renderNode's existing data sources, and returns a new render expression with
// the set-returning function replaced by an IndexedVar that points at the new
// data source.
//
// TODO(knz): this transform should really be done at the end of
// logical planning, and not at the beginning. Also the implementation
// is simple and does not support nested SRFs (the algorithm for that
// is much more complicated, see issue #26234).
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
		ctx:         ctx,
		p:           p,
		searchPath:  p.SessionData().SearchPath,
		origAliases: r.source.info.SourceAliases,
		numColumns:  len(r.source.info.SourceColumns),
	}

	// Rewrite the SRFs, if any. We want to avoid re-allocating a targets
	// array unless necessary.
	var newTargets tree.SelectExprs
	for i, target := range targets {
		curSrfs := len(v.srfs)
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

		if len(v.srfs) != curSrfs && target.As == "" {
			// If the transform is adding a SRF, the expression will have
			// changed structure. To help the user (and for compatibility
			// with pg) we need to ensure the column label is based off the
			// original expression.
			_, newLabel, err := tree.ComputeColNameInternal(v.searchPath, target.Expr)
			if err != nil {
				return nil, err
			}
			newTargets[i].As = tree.UnrestrictedName(newLabel)
		}
	}

	if len(v.srfs) > 0 {
		// Some SRFs were found. Create a projectSetNode to represent the
		// relational operator.
		projectPlan, err := p.ProjectSet(ctx, r.source.plan, "SELECT", v.srfs...)
		if err != nil {
			return nil, err
		}

		newInfo := *r.source.info
		newInfo.SourceColumns = planColumns(projectPlan)
		newInfo.SourceAliases = v.sourceAliases
		r.source.info = &newInfo
		r.sourceInfo[0] = r.source.info
		r.source.plan = projectPlan
	}

	if newTargets != nil {
		return newTargets, nil
	}
	return targets, nil
}

func (v *srfExtractionVisitor) transformSRF(
	srf *tree.FuncExpr, fd *tree.FunctionDefinition, columnAccessExpr *tree.ColumnAccessExpr,
) (tree.Expr, error) {
	if v.seenSRF > 1 {
		return nil, pgerror.UnimplementedWithIssueErrorf(26234, "nested set-returning functions")
	}

	// Create a unique name for this SRF. We need unique names so that
	// we can properly handle the same function name used in multiple
	// places.
	srfSourceName := fmt.Sprintf("%s_%d", fd.Name, len(v.srfs))

	// Remember the SRF for the end of the analysis.
	v.srfs = append(v.srfs, srf)

	// Create an alias definition so that the unique name created above
	// becomes available for name resolution later in renderNode.
	tblName := tree.MakeUnqualifiedTableName(tree.Name(srfSourceName))
	if v.sourceAliases == nil {
		v.sourceAliases = make(sqlbase.SourceAliases, len(v.origAliases))
		copy(v.sourceAliases, v.origAliases)
	}
	sourceAlias := sqlbase.SourceAlias{
		Name:      tblName,
		ColumnSet: sqlbase.FillColumnRange(v.numColumns, v.numColumns+len(fd.ReturnLabels)-1),
	}
	v.sourceAliases = append(v.sourceAliases, sourceAlias)

	// Bump the column count for the next SRF collected.
	v.numColumns += len(fd.ReturnLabels)

	// Now decide what to render.
	if columnAccessExpr != nil {
		// Syntax: (tbl).colname or (tbl).*
		if columnAccessExpr.Star {
			// (tbl).*
			// We want all the columns as separate renders. Let the star
			// expansion that occurs later do the job.
			return &tree.AllColumnsSelector{TableName: tree.MakeUnresolvedName(srfSourceName)}, nil
		}
		// (tbl).colname
		// We just want one column. Render that. For user friendliness,
		// check that the label exists.
		found := false
		for _, l := range fd.ReturnLabels {
			if l == columnAccessExpr.ColName {
				found = true
				break
			}
		}
		if !found {
			return nil, pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
				"could not identify column %q in record data type",
				tree.ErrNameString(&columnAccessExpr.ColName))
		}
		return tree.NewColumnItem(&tblName, tree.Name(columnAccessExpr.ColName)), nil
	}

	// Just the SRF name. Check whether we have just one column or more.
	if len(fd.ReturnLabels) == 1 {
		// Just one column. Render that column exactly.
		return tree.NewColumnItem(&tblName, tree.Name(fd.ReturnLabels[0])), nil
	}
	// More than one column.
	// We want all the columns as a tuple. Render that.
	tExpr := &tree.Tuple{
		Exprs:  make(tree.Exprs, len(fd.ReturnLabels)),
		Labels: fd.ReturnLabels,
	}
	for i, lbl := range fd.ReturnLabels {
		tExpr.Exprs[i] = tree.NewColumnItem(&tblName, tree.Name(lbl))
	}
	return tExpr, nil
}
