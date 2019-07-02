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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// buildAlterTableSplit builds an ALTER TABLE/INDEX .. SPLIT AT .. statement.
func (b *Builder) buildAlterTableSplit(split *tree.Split, inScope *scope) (outScope *scope) {
	flags := cat.Flags{
		AvoidDescriptorCaches: true,
		NoTableStats:          true,
	}
	index, err := cat.ResolveTableIndex(b.ctx, b.catalog, flags, &split.TableOrIndex)
	if err != nil {
		panic(builderError{err})
	}
	table := index.Table()
	if err := b.catalog.CheckPrivilege(b.ctx, table, privilege.INSERT); err != nil {
		panic(builderError{err})
	}

	b.DisableMemoReuse = true

	// Calculate the desired types for the select statement. It is OK if the
	// select statement returns fewer columns (the relevant prefix is used).
	desiredTypes := make([]*types.T, index.LaxKeyColumnCount())
	for i := range desiredTypes {
		desiredTypes[i] = index.Column(i).DatumType()
	}

	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	emptyScope := &scope{builder: b}
	stmtScope := b.buildStmt(split.Rows, desiredTypes, emptyScope)
	if len(stmtScope.cols) == 0 {
		panic(pgerror.Newf(pgcode.Syntax, "no columns in SPLIT AT data"))
	}
	if len(stmtScope.cols) > len(desiredTypes) {
		panic(pgerror.Newf(pgcode.Syntax, "too many columns in SPLIT AT data"))
	}
	for i := range stmtScope.cols {
		if !stmtScope.cols[i].typ.Equivalent(desiredTypes[i]) {
			panic(pgerror.Newf(
				pgcode.Syntax, "SPLIT AT data column %d (%s) must be of type %s, not type %s",
				i+1, index.Column(i).ColName(), desiredTypes[i], stmtScope.cols[i].typ,
			))
		}
	}

	// Build the expiration scalar.
	var expiration opt.ScalarExpr
	if split.ExpireExpr != nil {
		emptyScope.context = "ALTER TABLE SPLIT AT"
		// We need to save and restore the previous value of the field in
		// semaCtx in case we are recursively called within a subquery
		// context.
		defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
		b.semaCtx.Properties.Require(emptyScope.context, tree.RejectSpecial)

		texpr := emptyScope.resolveType(split.ExpireExpr, types.String)
		expiration = b.buildScalar(texpr, emptyScope, nil /* outScope */, nil /* outCol */, nil /* colRefs */)
	} else {
		expiration = b.factory.ConstructNull(types.String)
	}

	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, sqlbase.AlterTableSplitColumns)
	outScope.expr = b.factory.ConstructAlterTableSplit(
		stmtScope.expr.(memo.RelExpr),
		expiration,
		&memo.AlterTableSplitPrivate{
			Table:   b.factory.Metadata().AddTable(table),
			Index:   index.Ordinal(),
			Columns: colsToColList(outScope.cols),
			Props:   stmtScope.makePhysicalProps(),
		},
	)
	return outScope
}
