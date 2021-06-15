// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachange"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

var colInIndexNotSupportedErr = unimplemented.NewWithIssuef(
	47636, "ALTER COLUMN TYPE requiring rewrite of on-disk "+
		"data is currently not supported for columns that are part of an index")

var colOwnsSequenceNotSupportedErr = unimplemented.NewWithIssuef(
	48244, "ALTER COLUMN TYPE for a column that owns a sequence "+
		"is currently not supported")

var colWithConstraintNotSupportedErr = unimplemented.NewWithIssuef(
	48288, "ALTER COLUMN TYPE for a column that has a constraint "+
		"is currently not supported")

// AlterColTypeInTxnNotSupportedErr is returned when an ALTER COLUMN TYPE
// is tried in an explicit transaction.
var AlterColTypeInTxnNotSupportedErr = unimplemented.NewWithIssuef(
	49351, "ALTER COLUMN TYPE is not supported inside a transaction")

var alterColTypeInCombinationNotSupportedErr = unimplemented.NewWithIssuef(
	49351, "ALTER COLUMN TYPE cannot be used in combination "+
		"with other ALTER TABLE commands")

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
	for _, tableRef := range tableDesc.DependedOnBy {
		found := false
		for _, colID := range tableRef.ColumnIDs {
			if colID == col.GetID() {
				found = true
			}
		}
		if found {
			return params.p.dependentViewError(
				ctx, "column", col.GetName(), tableDesc.ParentID, tableRef.ID, "alter type of",
			)
		}
	}

	typ, err := tree.ResolveType(ctx, t.ToType, params.p.semaCtx.GetTypeResolver())
	if err != nil {
		return err
	}

	version := params.ExecCfg().Settings.Version.ActiveVersionOrEmpty(params.ctx)
	if supported, err := isTypeSupportedInVersion(version, typ); err != nil {
		return err
	} else if !supported {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"type %s is not supported until version upgrade is finalized",
			typ.SQLString(),
		)
	}

	// Special handling for STRING COLLATE xy to verify that we recognize the language.
	if t.Collation != "" {
		if types.IsStringType(typ) {
			typ = types.MakeCollatedString(typ, t.Collation)
		} else {
			return pgerror.New(pgcode.Syntax, "COLLATE can only be used with string types")
		}
	}

	err = colinfo.ValidateColumnDefType(typ)
	if err != nil {
		return err
	}

	var kind schemachange.ColumnConversionKind
	if t.Using != nil {
		// If an expression is provided, we always need to try a general conversion.
		// We have to follow the process to create a new column and backfill it
		// using the expression.
		kind = schemachange.ColumnConversionGeneral
	} else {
		kind, err = schemachange.ClassifyConversion(ctx, col.GetType(), typ)
		if err != nil {
			return err
		}
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
		col.ColumnDesc().Type = typ
	case schemachange.ColumnConversionGeneral, schemachange.ColumnConversionValidate:
		if err := alterColumnTypeGeneral(ctx, tableDesc, col, typ, t.Using, params, cmds, tn); err != nil {
			return err
		}
		if err := params.p.createOrUpdateSchemaChangeJob(params.ctx, tableDesc, tree.AsStringWithFQNames(t, params.Ann()), tableDesc.ClusterVersion.NextMutationID); err != nil {
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
	// Make sure that all nodes in the cluster are able to perform
	// general alter column type conversions.
	if !params.p.ExecCfg().Settings.Version.IsActive(
		params.ctx,
		clusterversion.AlterColumnTypeGeneral,
	) {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"version %v must be finalized to run this alter column type",
			clusterversion.AlterColumnTypeGeneral)
	}
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
			pgcode.FeatureNotSupported)
	}

	// Disallow ALTER COLUMN TYPE general for columns that own sequences.
	if col.NumOwnsSequences() != 0 {
		return colOwnsSequenceNotSupportedErr
	}

	// Disallow ALTER COLUMN TYPE general for columns that have a check
	// constraint.
	for i := range tableDesc.Checks {
		uses, err := tableDesc.CheckConstraintUsesColumn(tableDesc.Checks[i], col.GetID())
		if err != nil {
			return err
		}
		if uses {
			return colWithConstraintNotSupportedErr
		}
	}

	// Disallow ALTER COLUMN TYPE general for columns that have a
	// UNIQUE WITHOUT INDEX constraint.
	for _, uc := range tableDesc.AllActiveAndInactiveUniqueWithoutIndexConstraints() {
		for _, id := range uc.ColumnIDs {
			if col.GetID() == id {
				return colWithConstraintNotSupportedErr
			}
		}
	}

	// Disallow ALTER COLUMN TYPE general for columns that have a foreign key
	// constraint.
	for _, fk := range tableDesc.AllActiveAndInactiveForeignKeys() {
		for _, id := range append(fk.OriginColumnIDs, fk.ReferencedColumnIDs...) {
			if col.GetID() == id {
				return colWithConstraintNotSupportedErr
			}
		}
	}

	// Disallow ALTER COLUMN TYPE general for columns that are
	// part of indexes.
	for _, idx := range tableDesc.NonDropIndexes() {
		for i := 0; i < idx.NumKeyColumns(); i++ {
			if idx.GetKeyColumnID(i) == col.GetID() {
				return colInIndexNotSupportedErr
			}
		}
		for i := 0; i < idx.NumKeySuffixColumns(); i++ {
			if idx.GetKeySuffixColumnID(i) == col.GetID() {
				return colInIndexNotSupportedErr
			}
		}
	}

	// Disallow ALTER COLUMN TYPE general inside an explicit transaction.
	if !params.p.EvalContext().TxnImplicit {
		return AlterColTypeInTxnNotSupportedErr
	}

	if len(cmds) > 1 {
		return alterColTypeInCombinationNotSupportedErr
	}

	// Disallow ALTER COLUMN TYPE general if the table is already undergoing
	// a schema change.
	currentMutationID := tableDesc.ClusterVersion.NextMutationID
	for i := range tableDesc.Mutations {
		mut := &tableDesc.Mutations[i]
		if mut.MutationID < currentMutationID {
			return unimplemented.NewWithIssuef(
				47137, "table %s is currently undergoing a schema change", tableDesc.Name)
		}
	}

	nameExists := func(name string) bool {
		_, err := tableDesc.FindColumnWithName(tree.Name(name))
		return err == nil
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
			"ALTER COLUMN TYPE USING EXPRESSION",
			&params.p.semaCtx,
			tree.VolatilityVolatile,
			tn,
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
	}

	// Create the default expression for the new column.
	hasDefault := col.HasDefault()
	var newColDefaultExpr *string
	if hasDefault {
		if col.ColumnDesc().HasNullDefault() {
			s := tree.Serialize(tree.DNull)
			newColDefaultExpr = &s
		} else {
			// The default expression for the new column is applying the
			// computed expression to the previous default expression.
			expr, err := parser.ParseExpr(col.GetDefaultExpr())
			if err != nil {
				return err
			}
			typedExpr, err := expr.TypeCheck(ctx, &params.p.semaCtx, toType)
			if err != nil {
				return err
			}
			castExpr := tree.NewTypedCastExpr(typedExpr, toType)
			newDefaultComputedExpr, err := castExpr.Eval(params.EvalContext())
			if err != nil {
				return err
			}
			s := tree.Serialize(newDefaultComputedExpr)
			newColDefaultExpr = &s
		}
	}

	newCol := descpb.ColumnDescriptor{
		Name:            shadowColName,
		Type:            toType,
		Nullable:        col.IsNullable(),
		DefaultExpr:     newColDefaultExpr,
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

	if err := tableDesc.AllocateIDs(ctx); err != nil {
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
