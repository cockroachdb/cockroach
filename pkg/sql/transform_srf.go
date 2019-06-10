// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
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

	// srfs contains the SRFs collected so far. This list is processed
	// at the end of the visit to generate a projectSetNode.
	srfs tree.Exprs

	// sourceNames contains unique names for each of the collected SRF
	// expressions, so that columns from the same SRF function name
	// occurring in multiple places can be disambiguated.
	sourceNames tree.TableNames

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

	if t, ok := expr.(*tree.FuncExpr); ok {
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
	if t, ok := expr.(*tree.FuncExpr); ok {
		fd, err := v.lookupSRF(t)
		if err != nil {
			v.err = err
			return expr
		}
		if fd != nil {
			newExpr, err := v.transformSRF(t, fd)
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
		ctx:        ctx,
		p:          p,
		searchPath: p.SessionData().SearchPath,
		numColumns: len(r.source.info.SourceColumns),
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
		projectDS, err := p.ProjectSet(ctx,
			r.source.plan, r.source.info, "SELECT",
			v.sourceNames, v.srfs...)
		if err != nil {
			return nil, err
		}

		r.source.plan = projectDS.plan
		r.source.info = projectDS.info
		r.sourceInfo[0] = r.source.info
	}

	if newTargets != nil {
		return newTargets, nil
	}
	return targets, nil
}

func (v *srfExtractionVisitor) transformSRF(
	srf *tree.FuncExpr, fd *tree.FunctionDefinition,
) (tree.Expr, error) {
	if v.seenSRF > 1 {
		return nil, unimplemented.NewWithIssuef(26234, "nested set-returning functions")
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
	v.sourceNames = append(v.sourceNames, tblName)

	// Bump the column count for the next SRF collected.
	v.numColumns += len(fd.ReturnLabels)

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
