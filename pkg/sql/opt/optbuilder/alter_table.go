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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// buildAlterTableSplit builds an ALTER TABLE/INDEX .. SPLIT AT .. statement.
func (b *Builder) buildAlterTableSplit(split *tree.Split, inScope *scope) (outScope *scope) {
	flags := cat.Flags{
		AvoidDescriptorCaches: true,
		NoTableStats:          true,
	}
	table, idxOrdinal, err := cat.ResolveTableIndex(b.ctx, b.catalog, flags, &split.TableOrIndex)
	if err != nil {
		panic(builderError{err})
	}
	if err := b.catalog.CheckPrivilege(b.ctx, table, privilege.INSERT); err != nil {
		panic(builderError{err})
	}

	b.DisableMemoReuse = true
	index := table.Index(idxOrdinal)

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
		panic(builderError{errors.Errorf("no columns in SPLIT AT data")})
	}
	if len(stmtScope.cols) > len(desiredTypes) {
		panic(builderError{errors.Errorf("too many columns in SPLIT AT data")})
	}
	for i := range stmtScope.cols {
		if !stmtScope.cols[i].typ.Equivalent(desiredTypes[i]) {
			panic(builderError{errors.Errorf(
				"SPLIT AT data column %d (%s) must be of type %s, not type %s",
				i+1, index.Column(i).ColName(), desiredTypes[i], stmtScope.cols[i].typ,
			)})
		}
	}

	// Build the expiration scalar.
	var expiration opt.ScalarExpr
	if split.ExpireExpr != nil {
		b.assertNoAggregationOrWindowing(split.ExpireExpr, "ALTER TABLE SPLIT AT")
		expiration = b.resolveAndBuildScalar(
			split.ExpireExpr, types.String, "ALTER TABLE SPLIT AT", tree.RejectSpecial, emptyScope,
		)
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
			Index:   idxOrdinal,
			Columns: colsToColList(outScope.cols),
			Props:   stmtScope.makePhysicalProps(),
		},
	)
	return outScope
}
