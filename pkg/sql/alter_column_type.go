// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachange"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// AlterColumnType takes an AlterTableAlterColumnType, determines
// which conversion to use and applies the type conversion.
func AlterColumnType(
	ctx context.Context,
	tableDesc *tabledesc.Mutable,
	col catalog.Column,
	t *tree.AlterTableAlterColumnType,
	params runParams,
	cmds tree.AlterTableCmds,
	tn *tree.TableName,
) error {
	objType := "column"
	op := "alter type of"
	for _, tableRef := range tableDesc.DependedOnBy {
		found := false
		for _, colID := range tableRef.ColumnIDs {
			if colID == col.GetID() {
				found = true
			}
		}
		if found {
			return params.p.dependentError(
				ctx, objType, col.GetName(), tableDesc.ParentID, tableRef.ID, tableDesc.ID, op,
			)
		}
	}

	if err := schemaexpr.ValidateTTLExpression(tableDesc, tableDesc.GetRowLevelTTL(), col, tn, op); err != nil {
		return err
	}

	if err := schemaexpr.ValidateComputedColumnExpressionDoesNotDependOnColumn(tableDesc, col, objType, op); err != nil {
		return err
	}

	if err := schemaexpr.ValidatePartialIndex(tableDesc, col, objType, op); err != nil {
		return err
	}

	typ, err := tree.ResolveType(ctx, t.ToType, params.p.semaCtx.GetTypeResolver())
	if err != nil {
		return err
	}

	typ, err = schemachange.ValidateAlterColumnTypeChecks(ctx, t,
		params.EvalContext().Settings, typ, col.IsGeneratedAsIdentity(), col.IsVirtual())
	if err != nil {
		return err
	}

	kind, err := schemachange.ClassifyConversionFromTree(ctx, t, col.GetType(), typ, col.IsVirtual())
	if err != nil {
		return err
	}

	switch kind {
	case schemachange.ColumnConversionDangerous, schemachange.ColumnConversionImpossible:
		// We're not going to make it impossible for the user to perform
		// this conversion, but we do want them to explicit about
		// what they're going for.
		return pgerror.Newf(pgcode.CannotCoerce,
			"the requested type conversion (%s -> %s) requires an explicit USING expression",
			col.GetType().SQLString(), typ.SQLString())
	case schemachange.ColumnConversionTrivial:
		if col.HasDefault() {
			if validCast := cast.ValidCast(col.GetType(), typ, cast.ContextAssignment); !validCast {
				return pgerror.Wrapf(
					err,
					pgcode.DatatypeMismatch,
					"default for column %q cannot be cast automatically to type %s",
					col.GetName(),
					typ.SQLString(),
				)
			}
		}
		if col.HasOnUpdate() {
			if validCast := cast.ValidCast(col.GetType(), typ, cast.ContextAssignment); !validCast {
				return pgerror.Wrapf(
					err,
					pgcode.DatatypeMismatch,
					"on update for column %q cannot be cast automatically to type %s",
					col.GetName(),
					typ.SQLString(),
				)
			}
		}

		col.ColumnDesc().Type = typ
	case schemachange.ColumnConversionGeneral, schemachange.ColumnConversionValidate:
		if err := alterColumnTypeGeneral(ctx, tableDesc, col, typ, t.Using, params, cmds, tn); err != nil {
			return err
		}
		if err := params.p.createOrUpdateSchemaChangeJob(params.ctx, tableDesc, tree.AsStringWithFQNames(t, params.Ann()), tableDesc.ClusterVersion().NextMutationID); err != nil {
			return err
		}
		params.p.BufferClientNotice(params.ctx, pgnotice.Newf("ALTER COLUMN TYPE changes are finalized asynchronously; "+
			"further schema changes on this table may be restricted until the job completes; "+
			"some writes to the altered column may be rejected until the schema change is finalized"))
	default:
		return errors.AssertionFailedf("unknown conversion for %s -> %s",
			col.GetType().SQLString(), typ.SQLString())
	}

	return nil
}

func alterColumnTypeGeneral(
	ctx context.Context,
	tableDesc *tabledesc.Mutable,
	col catalog.Column,
	toType *types.T,
	using tree.Expr,
	params runParams,
	cmds tree.AlterTableCmds,
	tn *tree.TableName,
) error {
	// Disallow ALTER COLUMN TYPE general for columns that own sequences.
	if col.NumOwnsSequences() != 0 {
		return sqlerrors.NewAlterColumnTypeColOwnsSequenceNotSupportedErr()
	}

	// Disallow ALTER COLUMN TYPE general for columns that have a check
	// constraint.
	for _, ck := range tableDesc.EnforcedCheckConstraints() {
		if ck.CollectReferencedColumnIDs().Contains(col.GetID()) {
			return sqlerrors.NewAlterColumnTypeColWithConstraintNotSupportedErr()
		}
	}

	// Disallow ALTER COLUMN TYPE general for columns that have a
	// UNIQUE WITHOUT INDEX constraint.
	for _, uc := range tableDesc.UniqueConstraintsWithoutIndex() {
		if uc.CollectKeyColumnIDs().Contains(col.GetID()) {
			return sqlerrors.NewAlterColumnTypeColWithConstraintNotSupportedErr()
		}
	}

	// Disallow ALTER COLUMN TYPE general for columns that have a foreign key
	// constraint.
	for _, fk := range tableDesc.OutboundForeignKeys() {
		if fk.CollectOriginColumnIDs().Contains(col.GetID()) {
			return sqlerrors.NewAlterColumnTypeColWithConstraintNotSupportedErr()
		}
	}
	for _, fk := range tableDesc.InboundForeignKeys() {
		if fk.GetReferencedTableID() == tableDesc.GetID() &&
			fk.CollectReferencedColumnIDs().Contains(col.GetID()) {
			return sqlerrors.NewAlterColumnTypeColWithConstraintNotSupportedErr()
		}
	}

	// Disallow ALTER COLUMN TYPE general for columns that are
	// part of indexes.
	for _, idx := range tableDesc.NonDropIndexes() {
		if idx.CollectKeyColumnIDs().Contains(col.GetID()) ||
			idx.CollectKeySuffixColumnIDs().Contains(col.GetID()) ||
			idx.CollectSecondaryStoredColumnIDs().Contains(col.GetID()) {
			return sqlerrors.NewAlterColumnTypeColInIndexNotSupportedErr()
		}
	}

	// Disallow ALTER COLUMN TYPE general inside a multi-statement transaction.
	if !params.extendedEvalCtx.TxnIsSingleStmt {
		return sqlerrors.NewAlterColTypeInTxnNotSupportedErr()
	}

	if len(cmds) > 1 {
		return sqlerrors.NewAlterColTypeInCombinationNotSupportedError()
	}

	// Disallow ALTER COLUMN TYPE general if the table is already undergoing
	// a schema change.
	currentMutationID := tableDesc.ClusterVersion().NextMutationID
	for i := range tableDesc.Mutations {
		mut := &tableDesc.Mutations[i]
		if mut.MutationID < currentMutationID {
			return unimplemented.NewWithIssuef(
				47137, "table %s is currently undergoing a schema change", tableDesc.Name)
		}
	}

	// The algorithm relies heavily on the computed column expression, so we don’t
	// support altering a column if it’s also computed. There’s currently no way to
	// track the original expression in this case. However, this is supported in the
	// declarative schema changer.
	if col.IsComputed() {
		return unimplemented.Newf("ALTER COLUMN ... TYPE",
			"ALTER COLUMN TYPE requiring an on-disk data rewrite with the legacy schema changer "+
				"is not supported for computed columns")
	}

	// Starting in 25.1, ALTER COLUMN TYPE is fully supported in the declarative
	// schema changer (DSC) and no longer requires the experimental setting. This
	// version gate ensures backward compatibility for mixed-version clusters. If
	// 25.1 is active, the DSC becomes the only way to alter the column type.
	// TODO(#164735): clean this up further.
	return pgerror.New(pgcode.FeatureNotSupported,
		"ALTER COLUMN TYPE is only implemented in the declarative schema changer")
}
