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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// buildAlterTableRelocate builds an ALTER RANGE RELOCATE (LEASE).
func (b *Builder) buildAlterRangeRelocate(
	relocate *tree.RelocateRange, inScope *scope,
) (outScope *scope) {
	//TODO(lunevalex) we need to go from a rangeId to a tableOrIndex name to get perms check to work

	b.DisableMemoReuse = true

	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, colinfo.AlterTableRelocateColumns)

	outScope.expr = b.factory.ConstructAlterRangeRelocate(
		&memo.AlterRangeRelocatePrivate{
			RelocateLease:     relocate.RelocateLease,
			RelocateNonVoters: relocate.RelocateNonVoters,
			RangeID:           relocate.RangeID,
			ToStoreID:         relocate.ToStoreID,
			FromStoreID:       relocate.FromStoreID,
			Columns:           colsToColList(outScope.cols),
			Props:             physical.MinRequired,
		},
	)
	return outScope
}
