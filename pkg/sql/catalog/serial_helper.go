// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catalog

import (
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func UseRowID(d tree.ColumnTableDef) *tree.ColumnTableDef {
	d.DefaultExpr.Expr = &tree.FuncExpr{Func: tree.WrapFunction("unique_rowid")}
	d.Type = types.Int
	// Column is non-nullable in all cases. PostgreSQL requires this.
	d.Nullable.Nullability = tree.NotNull

	return &d
}

func UseUnorderedRowID(d tree.ColumnTableDef) *tree.ColumnTableDef {
	d.DefaultExpr.Expr = &tree.FuncExpr{Func: tree.WrapFunction("unordered_unique_rowid")}
	d.Type = types.Int
	// Column is non-nullable in all cases. PostgreSQL requires this.
	d.Nullable.Nullability = tree.NotNull
	return &d
}

func UseSequence(d tree.ColumnTableDef, seqName *tree.TableName) *tree.ColumnTableDef {
	d.DefaultExpr.Expr = &tree.FuncExpr{
		Func:  tree.WrapFunction("nextval"),
		Exprs: tree.Exprs{tree.NewStrVal(seqName.String())}}
	// Column is non-nullable in all cases. PostgreSQL requires this.
	d.Nullable.Nullability = tree.NotNull
	return &d
}

// SequenceOptionsFromNormalizationMode modifies the column defintion and returns
// the sequence options to support the current serialization mode.
func SequenceOptionsFromNormalizationMode(
	mode sessiondatapb.SerialNormalizationMode,
	st *cluster.Settings,
	d *tree.ColumnTableDef,
	asIntType *types.T,
) (tree.SequenceOptions, error) {
	var seqOpts tree.SequenceOptions
	switch mode {
	case sessiondatapb.SerialUsesSQLSequences:
	case sessiondatapb.SerialUsesVirtualSequences:
		d.Type = types.Int
		seqOpts = append(seqOpts, tree.SequenceOption{
			Name: tree.SeqOptVirtual,
		})
	case sessiondatapb.SerialUsesCachedSQLSequences:
		cacheValue := sqlclustersettings.CachedSequencesCacheSizeSetting.Get(&st.SV)
		seqOpts = append(seqOpts, tree.SequenceOption{
			Name:   tree.SeqOptCache,
			IntVal: &cacheValue,
		})
	case sessiondatapb.SerialUsesCachedNodeSQLSequences:
		cacheValue := sqlclustersettings.CachedSequencesCacheSizeSetting.Get(&st.SV)
		seqOpts = append(seqOpts, tree.SequenceOption{
			Name:   tree.SeqOptCacheNode,
			IntVal: &cacheValue,
		})
	default:
		return nil, errors.AssertionFailedf("unsupported serialization mode: %s", mode)
	}

	// Setup the type of the sequence based on the type observed within
	// the column.
	switch asIntType {
	case types.Int2, types.Int4:
		// Valid types, nothing to do.
	case types.Int:
		// Int is the default, so no cast necessary.
		fallthrough
	default:
		// Types is not an integer so nothing to set.
		asIntType = nil
	}
	if asIntType != nil {
		seqOpts = append(seqOpts, tree.SequenceOption{Name: tree.SeqOptAs, AsIntegerType: asIntType})
	}

	return seqOpts, nil
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
