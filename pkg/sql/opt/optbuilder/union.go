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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// buildUnion builds a set of memo groups that represent the given union
// clause.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildUnion(
	clause *tree.UnionClause, inScope *scope,
) (out opt.GroupID, outScope *scope) {
	left, leftScope := b.buildSelect(clause.Left, inScope)
	right, rightScope := b.buildSelect(clause.Right, inScope)
	outScope = leftScope

	// Check that the number of columns matches.
	if len(leftScope.cols) != len(rightScope.cols) {
		panic(errorf("each %v query must have the same number of columns: %d vs %d",
			clause.Type, len(leftScope.cols), len(rightScope.cols)))
	}

	// newColsNeeded indicates whether or not we need to synthesize output
	// columns. This is always required for a UNION, because the output columns
	// of the union contain values from the left and right relations, and we must
	// synthesize new columns to contain these values. This is not necessary for
	// INTERSECT or EXCEPT, since these operations are basically filters on the
	// left relation.
	//
	// Another benefit to synthesizing new columns is to handle the case
	// when the type of one of the columns in the left relation is unknown, but
	// the type of the matching column in the right relation is known.
	// For example:
	//   SELECT NULL UNION SELECT 1
	// The type of NULL is unknown, and the type of 1 is int. We need to
	// synthesize a new column so the output column will have the correct type.
	newColsNeeded := clause.Type == tree.UnionOp
	if newColsNeeded {
		// Create a new scope to hold the new synthesized columns.
		outScope = leftScope.push()
		outScope.cols = make([]columnProps, 0, len(leftScope.cols))
	}

	// Build map from left columns to right columns.
	for i := range leftScope.cols {
		l := &leftScope.cols[i]
		r := &rightScope.cols[i]
		// TODO(dan): This currently checks whether the types are exactly the same,
		// but Postgres is more lenient:
		// http://www.postgresql.org/docs/9.5/static/typeconv-union-case.html.
		if !(l.typ.Equivalent(r.typ) || l.typ == types.Unknown || r.typ == types.Unknown) {
			panic(errorf("%v types %s and %s cannot be matched", clause.Type, l.typ, r.typ))
		}
		if l.hidden != r.hidden {
			panic(errorf("%v types cannot be matched", clause.Type))
		}

		if newColsNeeded {
			var typ types.T
			if l.typ != types.Unknown {
				typ = l.typ
			} else {
				typ = r.typ
			}

			b.synthesizeColumn(outScope, string(l.name), typ, nil)
		}
	}

	// Create the mapping between the left-side columns, right-side columns and
	// new columns (if needed).
	leftCols := colsToColList(leftScope.cols)
	rightCols := colsToColList(rightScope.cols)
	var newCols opt.ColList
	if newColsNeeded {
		newCols = colsToColList(outScope.cols)
	} else {
		newCols = leftCols
	}
	setOpColMap := opt.SetOpColMap{Left: leftCols, Right: rightCols, Out: newCols}
	private := b.factory.InternPrivate(&setOpColMap)

	if clause.All {
		switch clause.Type {
		case tree.UnionOp:
			out = b.factory.ConstructUnionAll(left, right, private)
		case tree.IntersectOp:
			out = b.factory.ConstructIntersectAll(left, right, private)
		case tree.ExceptOp:
			out = b.factory.ConstructExceptAll(left, right, private)
		}
	} else {
		switch clause.Type {
		case tree.UnionOp:
			out = b.factory.ConstructUnion(left, right, private)
		case tree.IntersectOp:
			out = b.factory.ConstructIntersect(left, right, private)
		case tree.ExceptOp:
			out = b.factory.ConstructExcept(left, right, private)
		}
	}

	return out, outScope
}
