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

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// buildUnion builds a set of memo groups that represent the given union
// clause.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildUnion(
	clause *tree.UnionClause, desiredTypes []*types.T, inScope *scope,
) (outScope *scope) {
	leftScope := b.buildSelect(clause.Left, desiredTypes, inScope)
	rightScope := b.buildSelect(clause.Right, desiredTypes, inScope)

	// Remove any hidden columns, as they are not included in the Union.
	leftScope.removeHiddenCols()
	rightScope.removeHiddenCols()

	// Check that the number of columns matches.
	if len(leftScope.cols) != len(rightScope.cols) {
		panic(pgerror.Newf(
			pgerror.CodeSyntaxError,
			"each %v query must have the same number of columns: %d vs %d",
			clause.Type, len(leftScope.cols), len(rightScope.cols),
		))
	}

	outScope = inScope.push()

	// newColsNeeded indicates whether or not we need to synthesize output
	// columns. This is always required for a UNION, because the output columns
	// of the union contain values from the left and right relations, and we must
	// synthesize new columns to contain these values. This is not necessary for
	// INTERSECT or EXCEPT, since these operations are basically filters on the
	// left relation.
	newColsNeeded := clause.Type == tree.UnionOp
	if newColsNeeded {
		outScope.cols = make([]scopeColumn, 0, len(leftScope.cols))
	}

	// propagateTypesLeft/propagateTypesRight indicate whether we need to wrap
	// the left/right side in a projection to cast some of the columns to the
	// correct type.
	// For example:
	//   SELECT NULL UNION SELECT 1
	// The type of NULL is unknown, and the type of 1 is int. We need to
	// wrap the left side in a project operation with a Cast expression so the
	// output column will have the correct type.
	var propagateTypesLeft, propagateTypesRight bool

	// Build map from left columns to right columns.
	for i := range leftScope.cols {
		l := &leftScope.cols[i]
		r := &rightScope.cols[i]
		// TODO(dan): This currently checks whether the types are exactly the same,
		// but Postgres is more lenient:
		// http://www.postgresql.org/docs/9.5/static/typeconv-union-case.html.
		if !(l.typ.Equivalent(r.typ) ||
			l.typ.Family() == types.UnknownFamily ||
			r.typ.Family() == types.UnknownFamily) {
			panic(pgerror.Newf(pgerror.CodeDatatypeMismatchError,
				"%v types %s and %s cannot be matched", clause.Type, l.typ, r.typ))
		}
		if l.hidden != r.hidden {
			// This should never happen.
			panic(errors.AssertionFailedf("%v types cannot be matched", clause.Type))
		}

		var typ *types.T
		if l.typ.Family() != types.UnknownFamily {
			typ = l.typ
			if r.typ.Family() == types.UnknownFamily {
				propagateTypesRight = true
			}
		} else {
			typ = r.typ
			if r.typ.Family() != types.UnknownFamily {
				propagateTypesLeft = true
			}
		}

		if newColsNeeded {
			b.synthesizeColumn(outScope, string(l.name), typ, nil, nil /* scalar */)
		}
	}

	if propagateTypesLeft {
		leftScope = b.propagateTypes(leftScope, rightScope)
	}
	if propagateTypesRight {
		rightScope = b.propagateTypes(rightScope, leftScope)
	}

	// Create the mapping between the left-side columns, right-side columns and
	// new columns (if needed).
	leftCols := colsToColList(leftScope.cols)
	rightCols := colsToColList(rightScope.cols)
	var newCols opt.ColList
	if newColsNeeded {
		newCols = colsToColList(outScope.cols)
	} else {
		outScope.appendColumnsFromScope(leftScope)
		newCols = leftCols
	}

	left := leftScope.expr.(memo.RelExpr)
	right := rightScope.expr.(memo.RelExpr)
	private := memo.SetPrivate{LeftCols: leftCols, RightCols: rightCols, OutCols: newCols}

	if clause.All {
		switch clause.Type {
		case tree.UnionOp:
			outScope.expr = b.factory.ConstructUnionAll(left, right, &private)
		case tree.IntersectOp:
			outScope.expr = b.factory.ConstructIntersectAll(left, right, &private)
		case tree.ExceptOp:
			outScope.expr = b.factory.ConstructExceptAll(left, right, &private)
		}
	} else {
		switch clause.Type {
		case tree.UnionOp:
			outScope.expr = b.factory.ConstructUnion(left, right, &private)
		case tree.IntersectOp:
			outScope.expr = b.factory.ConstructIntersect(left, right, &private)
		case tree.ExceptOp:
			outScope.expr = b.factory.ConstructExcept(left, right, &private)
		}
	}

	return outScope
}

// propagateTypes propagates the types of the source columns to the destination
// columns by wrapping the destination in a Project operation. The Project
// operation passes through columns that already have the correct type, and
// creates cast expressions for those that don't.
func (b *Builder) propagateTypes(dst, src *scope) *scope {
	expr := dst.expr.(memo.RelExpr)
	dstCols := dst.cols

	dst = dst.push()
	dst.cols = make([]scopeColumn, 0, len(dstCols))

	for i := 0; i < len(dstCols); i++ {
		dstType := dstCols[i].typ
		srcType := src.cols[i].typ
		if dstType.Family() == types.UnknownFamily && srcType.Family() != types.UnknownFamily {
			// Create a new column which casts the old column to the correct type.
			castExpr := b.factory.ConstructCast(b.factory.ConstructVariable(dstCols[i].id), srcType)
			b.synthesizeColumn(dst, string(dstCols[i].name), srcType, nil /* expr */, castExpr)
		} else {
			// The column is already the correct type, so add it as a passthrough
			// column.
			dst.appendColumn(&dstCols[i])
		}
	}
	dst.expr = b.constructProject(expr, dst.cols)
	return dst
}
