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
func (b *Builder) buildUnion(clause *tree.UnionClause, inScope *scope) (out opt.GroupID, outScope *scope) {
	left, leftScope := b.buildSelect(clause.Left, inScope)
	right, rightScope := b.buildSelect(clause.Right, inScope)

	// Check that the number of columns matches.
	if len(leftScope.cols) != len(rightScope.cols) {
		panic(errorf("each %v query must have the same number of columns: %d vs %d",
			clause.Type, len(leftScope.cols), len(rightScope.cols)))
	}

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
		if l.typ == types.Unknown && r.typ != types.Unknown {
			b.factory.Metadata().SetColumnType(l.index, r.typ)
		}
		colMap.Set(int(l.index), int(r.index))
	}
	private := b.factory.InternPrivate(&colMap)

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

	return out, leftScope
}
