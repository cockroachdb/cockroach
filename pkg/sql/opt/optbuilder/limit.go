// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// buildLimit adds Limit and Offset operators according to the Limit clause.
//
// parentScope is the scope for the LIMIT/OFFSET expressions; this is not the
// same as inScope, because statements like:
//
//	SELECT k FROM kv LIMIT k
//
// are not valid.
func (b *Builder) buildLimit(limit *tree.Limit, parentScope, inScope *scope) {
	if limit.Offset != nil {
		input := inScope.expr
		offset := b.resolveAndBuildScalar(
			limit.Offset, types.Int, exprKindOffset, tree.RejectSpecial, parentScope,
		)
		inScope.expr = b.factory.ConstructOffset(input, offset, inScope.makeOrderingChoice())
	}
	if limit.Count != nil {
		input := inScope.expr
		limit := b.resolveAndBuildScalar(
			limit.Count, types.Int, exprKindLimit, tree.RejectSpecial, parentScope,
		)
		inScope.expr = b.factory.ConstructLimit(input, limit, inScope.makeOrderingChoice())
	}
}
