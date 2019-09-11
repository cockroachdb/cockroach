// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
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
