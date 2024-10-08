// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// buildAlterTableRelocate builds an ALTER RANGE RELOCATE (LEASE).
func (b *Builder) buildAlterRangeRelocate(
	relocate *tree.RelocateRange, inScope *scope,
) (outScope *scope) {

	if err := b.catalog.CheckPrivilege(b.ctx, syntheticprivilege.GlobalPrivilegeObject, b.catalog.GetCurrentUser(), privilege.REPAIRCLUSTER); err != nil {
		panic(err)
	}

	// Disable optimizer caching, as we do for other ALTER statements.
	b.DisableMemoReuse = true

	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, colinfo.AlterTableRelocateColumns)

	cmdName := "RELOCATE " + relocate.SubjectReplicas.String()
	colNames := []string{"range ids"}
	colTypes := []*types.T{types.Int}

	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, colinfo.AlterRangeRelocateColumns)

	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	emptyScope := b.allocScope()
	inputScope := b.buildStmt(relocate.Rows, colTypes, emptyScope)
	checkInputColumns(cmdName, inputScope, colNames, colTypes, 1)

	var toStoreID, fromStoreID opt.ScalarExpr
	{
		emptyScope.context = exprKindStoreID
		// We need to save and restore the previous value of the field in
		// semaCtx in case we are recursively called within a subquery
		// context.
		defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
		b.semaCtx.Properties.Require(emptyScope.context.String(), tree.RejectSpecial)

		toStoreIDExpr := emptyScope.resolveType(relocate.ToStoreID, types.Int)
		toStoreID = b.buildScalar(toStoreIDExpr, emptyScope, nil, nil, nil)
		fromStoreIDExpr := emptyScope.resolveType(relocate.FromStoreID, types.Int)
		fromStoreID = b.buildScalar(fromStoreIDExpr, emptyScope, nil, nil, nil)
	}

	outScope.expr = b.factory.ConstructAlterRangeRelocate(
		inputScope.expr,
		toStoreID,
		fromStoreID,
		&memo.AlterRangeRelocatePrivate{
			SubjectReplicas: relocate.SubjectReplicas,
			Columns:         colsToColList(outScope.cols),
			Props:           physical.MinRequired,
		},
	)
	return outScope
}
