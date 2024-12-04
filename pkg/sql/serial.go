// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// uniqueRowIDExpr is used as default expression when
// SessionNormalizationMode is SerialUsesRowID.
var uniqueRowIDExpr = &tree.FuncExpr{Func: tree.WrapFunction("unique_rowid")}

// unorderedUniqueRowIDExpr is used when SessionNormalizationMode is
// SerialUsesUnorderedRowID.
var unorderedUniqueRowIDExpr = &tree.FuncExpr{Func: tree.WrapFunction("unordered_unique_rowid")}

// generateSequenceForSerial generates a new sequence
// which will be used when creating a SERIAL column.
// This is a helper method for generateSerialInColumnDef.
func (p *planner) generateSequenceForSerial(
	ctx context.Context, d *tree.ColumnTableDef, tableName *tree.TableName,
) (*tree.TableName, *tree.FuncExpr, catalog.DatabaseDescriptor, catalog.SchemaDescriptor, error) {
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
	flags := tree.ObjectLookupFlags{
		Required:          false,
		RequireMutable:    false,
		IncludeOffline:    true,
		DesiredObjectKind: tree.AnyObject,
	}
	for i := 0; ; i++ {
		if i > 0 {
			seqName.ObjectName = tree.Name(fmt.Sprintf("%s%d", nameBase, i))
		}
		res, _, err := resolver.ResolveExistingObject(ctx, p, seqName.ToUnresolvedObjectName(), flags)
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

	return seqName, defaultExpr, dbDesc, schemaDesc, nil
}

// generateSerialInColumnDef create a sequence for a new column.
// This is a helper method for processGeneratedAsIdentityColumnDef and processSerialInColumnDef.
func (p *planner) generateSerialInColumnDef(
	ctx context.Context,
	d *tree.ColumnTableDef,
	tableName *tree.TableName,
	serialNormalizationMode sessiondatapb.SerialNormalizationMode,
) (
	*tree.ColumnTableDef,
	*catalog.ResolvedObjectPrefix,
	*tree.TableName,
	tree.SequenceOptions,
	error,
) {

	if err := catalog.AssertValidSerialColumnDef(d, tableName); err != nil {
		return nil, nil, nil, nil, err
	}

	newSpec := *d

	// Column is non-nullable in all cases. PostgreSQL requires this.
	newSpec.Nullable.Nullability = tree.NotNull

	// Clear the IsSerial bit now that it's been remapped.
	newSpec.IsSerial = false

	defType, err := tree.ResolveType(ctx, d.Type, p.semaCtx.GetTypeResolver())
	if err != nil {
		return nil, nil, nil, nil, err
	}
	asIntType := defType

	// Find the integer type that corresponds to the specification.
	switch serialNormalizationMode {
	case sessiondatapb.SerialUsesRowID, sessiondatapb.SerialUsesUnorderedRowID, sessiondatapb.SerialUsesVirtualSequences:
		// If unique_rowid() or unordered_unique_rowid() or virtual sequences are
		// requested, we have no choice but to use the full-width integer type, no
		// matter which serial size was requested, otherwise the values will not
		// fit.
		//
		// TODO(bob): Follow up with https://github.com/cockroachdb/cockroach/issues/32534
		// when the default is inverted to determine if we should also
		// switch this behavior around.
		upgradeType := types.Int
		if defType.Width() < upgradeType.Width() {
			p.BufferClientNotice(
				ctx,
				errors.WithHintf(
					pgnotice.Newf(
						"upgrading the column %s to %s to utilize the session serial_normalization setting",
						d.Name.String(),
						upgradeType.SQLString(),
					),
					"change the serial_normalization to sql_sequence or sql_sequence_cached if you wish "+
						"to use a smaller sized serial column at the cost of performance. See %s",
					docs.URL("serial.html"),
				),
			)
		}
		newSpec.Type = upgradeType
		asIntType = upgradeType

	case sessiondatapb.SerialUsesSQLSequences, sessiondatapb.SerialUsesCachedSQLSequences, sessiondatapb.SerialUsesCachedNodeSQLSequences:
		// With real sequences we can use the requested type as-is.

	default:
		return nil, nil, nil, nil,
			errors.AssertionFailedf("unknown serial normalization mode: %s", serialNormalizationMode)
	}
	telemetry.Inc(sqltelemetry.SerialColumnNormalizationCounter(
		defType.Name(), serialNormalizationMode.String()))

	if serialNormalizationMode == sessiondatapb.SerialUsesRowID {
		// We're not constructing a sequence for this SERIAL column.
		// Use the "old school" CockroachDB default.
		newSpec.DefaultExpr.Expr = uniqueRowIDExpr
		return &newSpec, nil, nil, nil, nil
	} else if serialNormalizationMode == sessiondatapb.SerialUsesUnorderedRowID {
		newSpec.DefaultExpr.Expr = unorderedUniqueRowIDExpr
		return &newSpec, nil, nil, nil, nil
	}

	log.VEventf(ctx, 2, "creating sequence for new column %q of %q", d, tableName)

	seqName, defaultExpr, dbDesc, schemaDesc, err := p.generateSequenceForSerial(ctx, d, tableName)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	seqType := ""
	seqOpts, err := catalog.SequenceOptionsFromNormalizationMode(serialNormalizationMode, p.ExecCfg().Settings, d, asIntType)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	log.VEventf(ctx, 2, "new column %q of %q will have %s sequence name %q and default %q",
		d, tableName, seqType, seqName, defaultExpr)

	newSpec.DefaultExpr.Expr = defaultExpr

	return &newSpec, &catalog.ResolvedObjectPrefix{
		Database: dbDesc, Schema: schemaDesc,
	}, seqName, seqOpts, nil
}

// processGeneratedAsIdentityColumnDef provide info of a general sequence for a new column.
// It is invoked when GENERATED BY DEFAULT / ALWAYS AS IDENTITY is specified
// for a column under CREATE TABLE.
// This is a helper method for processSerialLikeInColumnDef.
func (p *planner) processGeneratedAsIdentityColumnDef(
	ctx context.Context, d *tree.ColumnTableDef, tableName *tree.TableName,
) (
	*tree.ColumnTableDef,
	*catalog.ResolvedObjectPrefix,
	*tree.TableName,
	tree.SequenceOptions,
	error,
) {
	// To generate a general sequence is the same as generate a serial
	// with serial_normalization being 'sql_sequence'.
	curSerialNormalizationMode := sessiondatapb.SerialUsesSQLSequences
	newSpecPtr, catalogPrefixPtr, seqName, seqOpts, err := p.generateSerialInColumnDef(ctx, d, tableName, curSerialNormalizationMode)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return newSpecPtr, catalogPrefixPtr, seqName, seqOpts, nil
}

// processSerialInColumnDef provide info of a sequence for a new column.
// It is invoked when SERIAL is specified
// for a column under CREATE TABLE.
// This is a helper method for processSerialLikeInColumnDef.
//
// The type of the generated sequence relies on the serial_normalization variable in session Data
// You can set this session data by "SET serial_normalization = your_mode".
// Reference: https://www.cockroachlabs.com/docs/stable/set-vars.html.
func (p *planner) processSerialInColumnDef(
	ctx context.Context, d *tree.ColumnTableDef, tableName *tree.TableName,
) (
	*tree.ColumnTableDef,
	*catalog.ResolvedObjectPrefix,
	*tree.TableName,
	tree.SequenceOptions,
	error,
) {
	serialNormalizationMode := p.SessionData().SerialNormalizationMode
	newSpecPtr, catalogPrefixPtr, seqName, seqOpts, err := p.generateSerialInColumnDef(ctx, d, tableName, serialNormalizationMode)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return newSpecPtr, catalogPrefixPtr, seqName, seqOpts, nil
}

// processSerialLikeInColumnDef analyzes a column definition and determines
// whether to use a sequence if the requested type is SERIAL-like.
// If a sequence must be created, it returns an TableName to use
// to create the new sequence and the DatabaseDescriptor of the
// parent database where it should be created.
// The ColumnTableDef is not mutated in-place; instead a new one is returned.
func (p *planner) processSerialLikeInColumnDef(
	ctx context.Context, d *tree.ColumnTableDef, tableName *tree.TableName,
) (
	*tree.ColumnTableDef,
	*catalog.ResolvedObjectPrefix,
	*tree.TableName,
	tree.SequenceOptions,
	error,
) {
	var newSpecPtr *tree.ColumnTableDef
	var catalogPrefixPtr *catalog.ResolvedObjectPrefix
	var seqName *tree.TableName
	var seqOpts tree.SequenceOptions
	var err error

	if d.IsSerial {
		newSpecPtr, catalogPrefixPtr, seqName, seqOpts, err = p.processSerialInColumnDef(ctx, d, tableName)
		if err != nil {
			return nil, nil, nil, nil, err
		}

	} else if d.GeneratedIdentity.IsGeneratedAsIdentity {
		newSpecPtr, catalogPrefixPtr, seqName, seqOpts, err = p.processGeneratedAsIdentityColumnDef(ctx, d, tableName)
		seqOpts = append(seqOpts, d.GeneratedIdentity.SeqOptions...)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	} else {
		return d, nil, nil, nil, nil
	}
	return newSpecPtr, catalogPrefixPtr, seqName, seqOpts, nil
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

	if err := catalog.AssertValidSerialColumnDef(d, tableName); err != nil {
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
