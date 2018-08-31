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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// buildLimit adds Limit and Offset operators according to the Limit clause.
//
// parentScope is the scope for the LIMIT/OFFSET expressions; this is not the
// same as inScope, because statements like:
//   SELECT k FROM kv LIMIT k
// are not valid.
func (b *Builder) buildLimit(limit *tree.Limit, parentScope, inScope *scope) {
	ordering := inScope.makeOrderingChoice()
	orderingPrivID := b.factory.InternOrderingChoice(&ordering)

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
		offset := b.buildScalar(texpr, parentScope, nil, nil, nil)
		inScope.group = b.factory.ConstructOffset(inScope.group, offset, orderingPrivID)
	}
	if limit.Count != nil {
		op := "LIMIT"
		b.assertNoAggregationOrWindowing(limit.Count, op)
		b.semaCtx.Properties.Require(op, tree.RejectSpecial)
		parentScope.context = op
		texpr := parentScope.resolveAndRequireType(limit.Count, types.Int)
		limit := b.buildScalar(texpr, parentScope, nil, nil, nil)
		inScope.group = b.factory.ConstructLimit(inScope.group, limit, orderingPrivID)
	}
}
