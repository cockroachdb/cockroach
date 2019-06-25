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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// buildLimit adds Limit and Offset operators according to the Limit clause.
//
// parentScope is the scope for the LIMIT/OFFSET expressions; this is not the
// same as inScope, because statements like:
//   SELECT k FROM kv LIMIT k
// are not valid.
func (b *Builder) buildLimit(limit *tree.Limit, parentScope, inScope *scope) {
	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)

	if limit.Offset != nil {
		op := "OFFSET"
		b.assertNoAggregationOrWindowing(limit.Offset, op)
		b.semaCtx.Properties.Require(op, tree.RejectSpecial)
		parentScope.context = op
		texpr := parentScope.resolveAndRequireType(limit.Offset, types.Int)
		input := inScope.expr.(memo.RelExpr)
		offset := b.buildScalar(texpr, parentScope, nil, nil, nil)
		inScope.expr = b.factory.ConstructOffset(input, offset, inScope.makeOrderingChoice())
	}
	if limit.Count != nil {
		op := "LIMIT"
		b.assertNoAggregationOrWindowing(limit.Count, op)
		b.semaCtx.Properties.Require(op, tree.RejectSpecial)
		parentScope.context = op
		texpr := parentScope.resolveAndRequireType(limit.Count, types.Int)
		input := inScope.expr.(memo.RelExpr)
		limit := b.buildScalar(texpr, parentScope, nil, nil, nil)
		inScope.expr = b.factory.ConstructLimit(input, limit, inScope.makeOrderingChoice())
	}
}
