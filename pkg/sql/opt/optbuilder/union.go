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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// buildUnionClause builds a set of memo groups that represent the given union
// clause.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildUnionClause(
	clause *tree.UnionClause, desiredTypes []*types.T, inScope *scope,
) (outScope *scope) {
	leftScope := b.buildStmt(clause.Left, desiredTypes, inScope)
	// Try to propagate types left-to-right, if we didn't already have desired
	// types.
	if len(desiredTypes) == 0 {
		desiredTypes = leftScope.makeColumnTypes()
	}
	rightScope := b.buildStmt(clause.Right, desiredTypes, inScope)
	return b.buildSetOp(clause.Type, clause.All, inScope, leftScope, rightScope)
}

func (b *Builder) buildSetOp(
	unionType tree.UnionType, all bool, inScope, leftScope, rightScope *scope,
) (outScope *scope) {
	// Remove any hidden columns, as they are not included in the Union.
	leftScope.removeHiddenCols()
	rightScope.removeHiddenCols()

	outScope = inScope.push()

	setOpTypes, leftCastsNeeded, rightCastsNeeded := b.typeCheckSetOp(
		leftScope, rightScope, unionType.String(),
	)

	if leftCastsNeeded {
		leftScope = b.addCasts(leftScope /* dst */, setOpTypes)
	}
	if rightCastsNeeded {
		rightScope = b.addCasts(rightScope /* dst */, setOpTypes)
	}

	// For UNION, we have to synthesize new output columns (because they contain
	// values from both the left and right relations). This is not necessary for
	// INTERSECT or EXCEPT, since these operations are basically filters on the
	// left relation.
	if unionType == tree.UnionOp {
		outScope.cols = make([]scopeColumn, 0, len(leftScope.cols))
		for i := range leftScope.cols {
			c := &leftScope.cols[i]
			b.synthesizeColumn(outScope, c.name, c.typ, nil, nil /* scalar */)
		}
	} else {
		outScope.appendColumnsFromScope(leftScope)
	}

	// Create the mapping between the left-side columns, right-side columns and
	// new columns (if needed).
	leftCols := colsToColList(leftScope.cols)
	rightCols := colsToColList(rightScope.cols)
	newCols := colsToColList(outScope.cols)

	left := leftScope.expr.(memo.RelExpr)
	right := rightScope.expr.(memo.RelExpr)
	private := memo.SetPrivate{LeftCols: leftCols, RightCols: rightCols, OutCols: newCols}

	if all {
		switch unionType {
		case tree.UnionOp:
			outScope.expr = b.factory.ConstructUnionAll(left, right, &private)
		case tree.IntersectOp:
			outScope.expr = b.factory.ConstructIntersectAll(left, right, &private)
		case tree.ExceptOp:
			outScope.expr = b.factory.ConstructExceptAll(left, right, &private)
		}
	} else {
		switch unionType {
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

// typeCheckSetOp cross-checks the types between the left and right sides of a
// set operation and determines the output types. Either side (or both) might
// need casts (as indicated in the return values).
//
// Throws an error if the scopes don't have the same number of columns, or when
// column types don't match 1-1 or can't be cast to a single output type. The
// error messages use clauseTag.
//
func (b *Builder) typeCheckSetOp(
	leftScope, rightScope *scope, clauseTag string,
) (setOpTypes []*types.T, leftCastsNeeded, rightCastsNeeded bool) {
	if len(leftScope.cols) != len(rightScope.cols) {
		panic(pgerror.Newf(
			pgcode.Syntax,
			"each %s query must have the same number of columns: %d vs %d",
			clauseTag, len(leftScope.cols), len(rightScope.cols),
		))
	}

	setOpTypes = make([]*types.T, len(leftScope.cols))
	for i := range leftScope.cols {
		l := &leftScope.cols[i]
		r := &rightScope.cols[i]

		typ := determineUnionType(l.typ, r.typ, clauseTag)
		setOpTypes[i] = typ
		leftCastsNeeded = leftCastsNeeded || !l.typ.Identical(typ)
		rightCastsNeeded = rightCastsNeeded || !r.typ.Identical(typ)
	}
	return setOpTypes, leftCastsNeeded, rightCastsNeeded
}

// determineUnionType determines the resulting type of a set operation on a
// column with the given left and right types.
//
// We allow implicit up-casts between types of the same numeric family with
// different widths; between int and float; and between int/float and decimal.
//
// Throws an error if we don't support a set operation between the two types.
func determineUnionType(left, right *types.T, clauseTag string) *types.T {
	if left.Identical(right) {
		return left
	}

	if left.Equivalent(right) {
		// Do a best-effort attempt to determine which type is "larger".
		if left.Width() > right.Width() {
			return left
		}
		if left.Width() < right.Width() {
			return right
		}
		// In other cases, use the left type.
		return left
	}
	leftFam, rightFam := left.Family(), right.Family()

	if rightFam == types.UnknownFamily {
		return left
	}
	if leftFam == types.UnknownFamily {
		return right
	}

	// Allow implicit upcast from int to float. Converting an int to float can be
	// lossy (especially INT8 to FLOAT4), but this is what Postgres does.
	if leftFam == types.FloatFamily && rightFam == types.IntFamily {
		return left
	}
	if leftFam == types.IntFamily && rightFam == types.FloatFamily {
		return right
	}

	// Allow implicit upcasts to decimal.
	if leftFam == types.DecimalFamily && (rightFam == types.IntFamily || rightFam == types.FloatFamily) {
		return left
	}
	if (leftFam == types.IntFamily || leftFam == types.FloatFamily) && rightFam == types.DecimalFamily {
		return right
	}

	// TODO(radu): Postgres has more encompassing rules:
	// http://www.postgresql.org/docs/12/static/typeconv-union-case.html
	panic(pgerror.Newf(
		pgcode.DatatypeMismatch,
		"%v types %s and %s cannot be matched", clauseTag, left, right,
	))
}

// addCasts adds a projection to a scope, adding casts as necessary so that the
// resulting columns have the given types.
func (b *Builder) addCasts(dst *scope, outTypes []*types.T) *scope {
	expr := dst.expr.(memo.RelExpr)
	dstCols := dst.cols

	dst = dst.push()
	dst.cols = make([]scopeColumn, 0, len(dstCols))

	for i := 0; i < len(dstCols); i++ {
		if !dstCols[i].typ.Identical(outTypes[i]) {
			// Create a new column which casts the old column to the correct type.
			castExpr := b.factory.ConstructCast(b.factory.ConstructVariable(dstCols[i].id), outTypes[i])
			b.synthesizeColumn(dst, dstCols[i].name, outTypes[i], nil /* expr */, castExpr)
		} else {
			// The column is already the correct type, so add it as a passthrough
			// column.
			dst.appendColumn(&dstCols[i])
		}
	}
	dst.expr = b.constructProject(expr, dst.cols)
	return dst
}
