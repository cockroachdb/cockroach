// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// uniqueRowIDExpr is used as default expression when
// SessionNormalizationMode is SerialUsesRowID.
var uniqueRowIDExpr = &tree.FuncExpr{Func: tree.WrapFunction("unique_rowid")}

// realSequenceOpts (nil) is used when SessionNormalizationMode is
// SerialUsesSQLSequences.
var realSequenceOpts tree.SequenceOptions

// virtualSequenceOpts is used when SessionNormalizationMode is
// SerialUsesVirtualSequences.
var virtualSequenceOpts = tree.SequenceOptions{
	tree.SequenceOption{Name: tree.SeqOptVirtual},
}

// cachedSequencesCacheSize is the default cache size used when
// SessionNormalizationMode is SerialUsesCachedSQLSequences.
var cachedSequencesCacheSizeSetting = settings.RegisterIntSetting(
	"sql.defaults.serial_sequences_cache_size",
	"the default cache size when the session's serial normalization mode is set to cached sequences"+
		"A cache size of 1 means no caching. Any cache size less than 1 is invalid.",
	256,
	settings.PositiveInt,
)

// processSerialInColumnDef analyzes a column definition and determines
// whether to use a sequence if the requested type is SERIAL-like.
// If a sequence must be created, it returns an TableName to use
// to create the new sequence and the DatabaseDescriptor of the
// parent database where it should be created.
// The ColumnTableDef is not mutated in-place; instead a new one is returned.
func (p *planner) processSerialInColumnDef(
	ctx context.Context, d *tree.ColumnTableDef, tableName *tree.TableName,
) (
	*tree.ColumnTableDef,
	*catalog.ResolvedObjectPrefix,
	*tree.TableName,
	tree.SequenceOptions,
	error,
) {
	if !d.IsSerial {
		// Column is not SERIAL: nothing to do.
		return d, nil, nil, nil, nil
	}

	if err := assertValidSerialColumnDef(d, tableName); err != nil {
		return nil, nil, nil, nil, err
	}

	newSpec := *d

	// Make the column non-nullable in all cases. PostgreSQL requires
	// this.
	newSpec.Nullable.Nullability = tree.NotNull

	serialNormalizationMode := p.SessionData().SerialNormalizationMode

	// Find the integer type that corresponds to the specification.
	switch serialNormalizationMode {
	case sessiondata.SerialUsesRowID, sessiondata.SerialUsesVirtualSequences:
		// If unique_rowid() or virtual sequences are requested, we have
		// no choice but to use the full-width integer type, no matter
		// which serial size was requested, otherwise the values will not fit.
		//
		// TODO(bob): Follow up with https://github.com/cockroachdb/cockroach/issues/32534
		// when the default is inverted to determine if we should also
		// switch this behavior around.
		newSpec.Type = types.Int

	case sessiondata.SerialUsesSQLSequences, sessiondata.SerialUsesCachedSQLSequences:
		// With real sequences we can use the requested type as-is.

	default:
		return nil, nil, nil, nil,
			errors.AssertionFailedf("unknown serial normalization mode: %s", serialNormalizationMode)
	}

	// Clear the IsSerial bit now that it's been remapped.
	newSpec.IsSerial = false

	defType, err := tree.ResolveType(ctx, d.Type, p.semaCtx.GetTypeResolver())
	if err != nil {
		return nil, nil, nil, nil, err
	}
	telemetry.Inc(sqltelemetry.SerialColumnNormalizationCounter(
		defType.Name(), serialNormalizationMode.String()))

	if serialNormalizationMode == sessiondata.SerialUsesRowID {
		// We're not constructing a sequence for this SERIAL column.
		// Use the "old school" CockroachDB default.
		newSpec.DefaultExpr.Expr = uniqueRowIDExpr
		return &newSpec, nil, nil, nil, nil
	}

	log.VEventf(ctx, 2, "creating sequence for new column %q of %q", d, tableName)

	// We want a sequence; for this we need to generate a new sequence name.
	// The constraint on the name is that an object of this name must not exist already.
	seqName := tree.NewTableNameWithSchema(
		tableName.CatalogName,
		tableName.SchemaName,
		tree.Name(tableName.Table()+"_"+string(d.Name)+"_seq"))

	// The first step in the search is to prepare the seqName to fill in
	// the catalog/schema parent. This is what ResolveTargetObject does.
	//
	// Here and below we skip the cache because name resolution using
	// the cache does not work (well) if the txn retries and the
	// descriptor was written already in an early txn attempt.
	un := seqName.ToUnresolvedObjectName()
	dbDesc, schemaDesc, prefix, err := p.ResolveTargetObject(ctx, un)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	seqName.ObjectNamePrefix = prefix

	// Now skip over all names that are already taken.
	nameBase := seqName.ObjectName
	for i := 0; ; i++ {
		if i > 0 {
			seqName.ObjectName = tree.Name(fmt.Sprintf("%s%d", nameBase, i))
		}
		res, err := p.resolveUncachedTableDescriptor(ctx, seqName, false /*required*/, tree.ResolveAnyTableKind)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if res == nil {
			break
		}
	}

	defaultExpr := &tree.FuncExpr{
		Func:  tree.WrapFunction("nextval"),
		Exprs: tree.Exprs{tree.NewStrVal(seqName.String())},
	}

	seqType := ""
	seqOpts := realSequenceOpts
	if serialNormalizationMode == sessiondata.SerialUsesVirtualSequences {
		seqType = "virtual "
		seqOpts = virtualSequenceOpts
	} else if serialNormalizationMode == sessiondata.SerialUsesCachedSQLSequences {
		seqType = "cached "

		value := cachedSequencesCacheSizeSetting.Get(&p.ExecCfg().Settings.SV)
		seqOpts = tree.SequenceOptions{
			tree.SequenceOption{Name: tree.SeqOptCache, IntVal: &value},
		}
	}
	log.VEventf(ctx, 2, "new column %q of %q will have %s sequence name %q and default %q",
		d, tableName, seqType, seqName, defaultExpr)

	newSpec.DefaultExpr.Expr = defaultExpr

	return &newSpec, &catalog.ResolvedObjectPrefix{
		Database: dbDesc, Schema: schemaDesc,
	}, seqName, seqOpts, nil
}

// SimplifySerialInColumnDefWithRowID analyzes a column definition and
// simplifies any use of SERIAL as if SerialNormalizationMode was set
// to SerialUsesRowID. No sequence needs to be created.
//
// This is currently used by bulk I/O import statements which do not
// (yet?) support customization of the SERIAL behavior.
func SimplifySerialInColumnDefWithRowID(
	ctx context.Context, d *tree.ColumnTableDef, tableName *tree.TableName,
) error {
	if !d.IsSerial {
		// Column is not SERIAL: nothing to do.
		return nil
	}

	if err := assertValidSerialColumnDef(d, tableName); err != nil {
		return err
	}

	// Make the column non-nullable in all cases. PostgreSQL requires
	// this.
	d.Nullable.Nullability = tree.NotNull

	// We're not constructing a sequence for this SERIAL column.
	// Use the "old school" CockroachDB default.
	d.Type = types.Int
	d.DefaultExpr.Expr = uniqueRowIDExpr

	// Clear the IsSerial bit now that it's been remapped.
	d.IsSerial = false

	return nil
}

func assertValidSerialColumnDef(d *tree.ColumnTableDef, tableName *tree.TableName) error {
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
