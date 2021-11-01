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
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// buildAlterTableRelocate builds an ALTER RANGE RELOCATE (LEASE).
func (b *Builder) buildAlterRangeRelocate(
	relocate *tree.RelocateRange, inScope *scope,
) (outScope *scope) {

	if err := b.catalog.RequireAdminRole(b.ctx, "ALTER RANGE RELOCATE"); err != nil {
		panic(err)
	}

	// Disable optimizer caching, as we do for other ALTER statements.
	b.DisableMemoReuse = true

	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, colinfo.AlterTableRelocateColumns)

	cmdName := "RELOCATE"
	if relocate.RelocateLease {
		cmdName += " LEASE"
	}
	colNames := []string{"range ids"}
	colTypes := []*types.T{types.Int}

	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, colinfo.AlterRangeRelocateColumns)

	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	inputScope := b.buildStmt(relocate.Rows, colTypes, b.allocScope())
	checkInputColumns(cmdName, inputScope, colNames, colTypes, 1)

	outScope.expr = b.factory.ConstructAlterRangeRelocate(
		inputScope.expr,
		&memo.AlterRangeRelocatePrivate{
			RelocateLease:     relocate.RelocateLease,
			RelocateNonVoters: relocate.RelocateNonVoters,
			ToStoreID:         relocate.ToStoreID,
			FromStoreID:       relocate.FromStoreID,
			Columns:           colsToColList(outScope.cols),
			Props:             physical.MinRequired,
		},
	)
	return outScope
}
