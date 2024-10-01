// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catalog

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func UseRowID(d tree.ColumnTableDef) *tree.ColumnTableDef {
	d.DefaultExpr.Expr = &tree.FuncExpr{Func: tree.WrapFunction("unique_rowid")}
	d.Type = types.Int
	// Column is non-nullable in all cases. PostgreSQL requires this.
	d.Nullable.Nullability = tree.NotNull

	return &d
}

func AssertValidSerialColumnDef(d *tree.ColumnTableDef, tableName *tree.TableName) error {
	if d.HasDefaultExpr() {
		// SERIAL implies a new default expression, we can't have one to
		// start with. This is the error produced by pg in such case.
		return pgerror.Newf(pgcode.Syntax,
			"multiple default values specified for column %q of table %q",
			tree.ErrString(&d.Name), tree.ErrString(tableName))
	}

	if d.Nullable.Nullability == tree.Null {
		// SERIAL implies a non-NULL column, we can't accept a nullability
		// spec. This is the error produced by pg in such case.
		return pgerror.Newf(pgcode.Syntax,
			"conflicting NULL/NOT NULL declarations for column %q of table %q",
			tree.ErrString(&d.Name), tree.ErrString(tableName))
	}

	if d.Computed.Expr != nil {
		// SERIAL cannot be a computed column.
		return pgerror.Newf(pgcode.Syntax,
			"SERIAL column %q of table %q cannot be computed",
			tree.ErrString(&d.Name), tree.ErrString(tableName))
	}

	return nil
}
