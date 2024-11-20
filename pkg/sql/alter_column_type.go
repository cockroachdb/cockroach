// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachange"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// AlterColTypeInTxnNotSupportedErr is returned when an ALTER COLUMN TYPE
// is tried in an explicit transaction.
var AlterColTypeInTxnNotSupportedErr = unimplemented.NewWithIssuef(
	49351, "ALTER COLUMN TYPE is not supported inside a transaction")

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
				ctx, objType, col.GetName(), tableDesc.ParentID, tableRef.ID, op,
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
	if !params.SessionData().AlterColumnTypeGeneralEnabled {
		return pgerror.WithCandidateCode(
			errors.WithHint(
				errors.WithIssueLink(
					errors.Newf("ALTER COLUMN TYPE from %v to %v is only "+
						"supported experimentally",
						col.GetType(), toType),
					errors.IssueLink{IssueURL: build.MakeIssueURL(49329)}),
				"you can enable alter column type general support by running "+
					"`SET enable_experimental_alter_column_type_general = true`"),
			pgcode.ExperimentalFeature)
	}

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
		return AlterColTypeInTxnNotSupportedErr
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

	nameExists := func(name string) bool {
		return catalog.FindColumnByName(tableDesc, name) != nil
	}

	shadowColName := tabledesc.GenerateUniqueName(col.GetName(), nameExists)

	var newColComputeExpr *string
	// oldCol still needs to have values written to it in case nodes read it from
	// it with a TableDescriptor version from before the swap.
	// To achieve this, we make oldCol a computed column of newCol using
	// inverseExpr.
	// If a USING EXPRESSION was provided, we cannot generally invert
	// the expression to insert into the old column and thus will force an
	// error using the computed expression. Any inserts into the new column
	// will fail until the old column is dropped.
	var inverseExpr string
	if using != nil {
		// Validate the provided using expr and ensure it has the correct type.
		expr, _, _, err := schemaexpr.DequalifyAndValidateExpr(
			ctx,
			tableDesc,
			using,
			toType,
			tree.AlterColumnTypeUsingExpr,
			&params.p.semaCtx,
			volatility.Volatile,
			tn,
			params.ExecCfg().Settings.Version.ActiveVersion(ctx),
		)

		if err != nil {
			return err
		}
		newColComputeExpr = &expr

		insertedValToString := tree.CastExpr{
			Expr:       &tree.ColumnItem{ColumnName: tree.Name(col.GetName())},
			Type:       types.String,
			SyntaxMode: tree.CastShort,
		}
		insertedVal := tree.Serialize(&insertedValToString)
		// Set the computed expression to use crdb_internal.force_error() to
		// prevent writes into the column. Whenever the new column is written to
		// crdb_internal.force_error() will cause the write to error out.
		// This prevents writes to the column undergoing ALTER COLUMN TYPE
		// until the original column is dropped.
		// This is safe to do so because after the column swap, the old column
		// should become "read-only".
		// The computed expression uses the old column to trigger the error
		// by using the string 'tried to insert <value> into <column name>'.
		errMsg := fmt.Sprintf(
			"'column %s is undergoing the ALTER COLUMN TYPE USING EXPRESSION "+
				"schema change, inserts are not supported until the schema change is "+
				"finalized, '",
			col.GetName())
		failedInsertMsg := fmt.Sprintf(
			"'tried to insert ', %s, ' into %s'", insertedVal, col.GetName(),
		)
		inverseExpr = fmt.Sprintf(
			"crdb_internal.force_error('%s', concat(%s, %s))",
			pgcode.SQLStatementNotYetComplete, errMsg, failedInsertMsg)
	} else {
		// The default computed expression is casting the column to the new type.
		newComputedExpr := tree.CastExpr{
			Expr:       &tree.ColumnItem{ColumnName: tree.Name(col.GetName())},
			Type:       toType,
			SyntaxMode: tree.CastShort,
		}
		s := tree.Serialize(&newComputedExpr)
		newColComputeExpr = &s

		oldColComputeExpr := tree.CastExpr{
			Expr:       &tree.ColumnItem{ColumnName: tree.Name(col.GetName())},
			Type:       col.GetType(),
			SyntaxMode: tree.CastShort,
		}
		inverseExpr = tree.Serialize(&oldColComputeExpr)

		// Validate that the column can be automatically cast to toType without explicit casting.
		if validCast := cast.ValidCast(col.GetType(), toType, cast.ContextAssignment); !validCast {
			return errors.WithHintf(
				pgerror.Newf(
					pgcode.DatatypeMismatch,
					"column %q cannot be cast automatically to type %s",
					col.GetName(),
					toType.SQLString(),
				), "You might need to specify \"USING %s\".", s,
			)
		}
	}
	// Create the default expression for the new column.
	hasDefault := col.HasDefault()
	hasUpdate := col.HasOnUpdate()
	if hasDefault {
		if validCast := cast.ValidCast(col.GetType(), toType, cast.ContextAssignment); !validCast {
			return pgerror.Newf(
				pgcode.DatatypeMismatch,
				"default for column %q cannot be cast automatically to type %s",
				col.GetName(),
				toType.SQLString(),
			)
		}
	}
	if hasUpdate {
		if validCast := cast.ValidCast(col.GetType(), toType, cast.ContextAssignment); !validCast {
			return pgerror.Newf(
				pgcode.DatatypeMismatch,
				"on update for column %q cannot be cast automatically to type %s",
				col.GetName(),
				toType.SQLString(),
			)
		}
	}

	// Set up the new column's descriptor. Since it is a computed column,
	// we need to omit the default and onUpdate expressions, as computed columns
	// cannot have them. These expressions are set during the computed column swap
	// later.
	newCol := descpb.ColumnDescriptor{
		Name:            shadowColName,
		Type:            toType,
		Nullable:        col.IsNullable(),
		Hidden:          col.IsHidden(),
		UsesSequenceIds: col.ColumnDesc().UsesSequenceIds,
		OwnsSequenceIds: col.ColumnDesc().OwnsSequenceIds,
		ComputeExpr:     newColComputeExpr,
	}
	// Ensure new column is created in the same column family as the original
	// so backfiller writes to the same column family.
	family, err := tableDesc.GetFamilyOfColumn(col.GetID())
	if err != nil {
		return err
	}

	if err := tableDesc.AddColumnToFamilyMaybeCreate(
		newCol.Name, family.Name, false, false); err != nil {
		return err
	}

	tableDesc.AddColumnMutation(&newCol, descpb.DescriptorMutation_ADD)
	if !newCol.Virtual {
		// Add non-virtual column name and ID to primary index.
		primaryIndex := tableDesc.GetPrimaryIndex().IndexDescDeepCopy()
		primaryIndex.StoreColumnNames = append(primaryIndex.StoreColumnNames, newCol.Name)
		primaryIndex.StoreColumnIDs = append(primaryIndex.StoreColumnIDs, newCol.ID)
		tableDesc.SetPrimaryIndex(primaryIndex)
	}

	version := params.ExecCfg().Settings.Version.ActiveVersion(ctx)
	if err := tableDesc.AllocateIDs(ctx, version); err != nil {
		return err
	}

	swapArgs := &descpb.ComputedColumnSwap{
		OldColumnId: col.GetID(),
		NewColumnId: newCol.ID,
		InverseExpr: inverseExpr,
	}

	tableDesc.AddComputedColumnSwapMutation(swapArgs)
	return nil
}
