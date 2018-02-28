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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
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

	// projectionRequired indicates whether or not we need to add a wrapper
	// projection around the left relation. This will be required if the type of
	// one of the columns in the left relation is unknown, but the type of the
	// matching column in the right relation is known. For example:
	//   SELECT NULL UNION SELECT 1
	// The type of NULL is unknown, and the type of 1 is int. After a UNION, the
	// left side columns become the output columns, so it is important that they
	// have the correct type.
	projectionRequired := false

	// Build map from left columns to right columns.
	var colMap opt.ColMap
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

		leftIdx := l.index
		if l.typ == types.Unknown && r.typ != types.Unknown {
			if !projectionRequired {
				// Create the new scope for the projection and add the required columns.
				outScope = leftScope.push()
				outScope.cols = make([]columnProps, i, len(leftScope.cols))
				copy(outScope.cols, leftScope.cols[:i])
				projectionRequired = true
			}

			col := b.synthesizeColumn(outScope, string(l.name), r.typ)
			leftIdx = col.index
		} else if projectionRequired {
			outScope.cols = append(outScope.cols, *l)
		}

		colMap.Set(int(leftIdx), int(r.index))
	}
	private := b.factory.InternPrivate(&colMap)

	// Wrap the left side with a projection if needed.
	if projectionRequired {
		projections := make([]opt.GroupID, 0, len(outScope.cols))
		for _, col := range outScope.cols {
			v := b.factory.ConstructVariable(b.factory.InternPrivate(col.index))
			projections = append(projections, v)
		}
		p := b.constructList(opt.ProjectionsOp, projections, outScope.cols)
		left = b.factory.ConstructProject(left, p)
	}

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
