// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/semenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam/tablestorageparam"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// isRBRUsingConstraintParam returns true if the key matches the RBR using
// constraint storage parameter name.
func isRBRUsingConstraintParam(key string) bool {
	return strings.ToLower(key) == catpb.RBRUsingConstraintTableSettingName
}

// buildTTLColumnExpr builds the expression: current_timestamp() + interval_expr.
// This is the expression used for the default and on-update values of the
// crdb_internal_expiration column when ttl_expire_after is set.
func buildTTLColumnExpr(ttl *catpb.RowLevelTTL) tree.Expr {
	intervalExpr, err := parser.ParseExpr(string(ttl.DurationExpr))
	if err != nil {
		panic(errors.Wrapf(err, "unexpected expression for TTL duration"))
	}
	return &tree.BinaryExpr{
		Operator: treebin.MakeBinaryOperator(treebin.Plus),
		Left:     &tree.FuncExpr{Func: tree.WrapFunction("current_timestamp")},
		Right:    intervalExpr,
	}
}

// addTTLColumn adds the crdb_internal_expiration column for ttl_expire_after.
// It returns the column ID of the newly created column.
func addTTLColumn(
	b BuildCtx, tbl *scpb.Table, t *tree.AlterTableSetStorageParams, ttl *catpb.RowLevelTTL,
) {
	// Check that column doesn't already exist.
	tableElts := b.QueryByID(tbl.TableID)
	var existingColID catid.ColumnID
	tableElts.FilterColumnName().ForEach(func(curr scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName) {
		if e.Name == string(catpb.TTLDefaultExpirationColumnName) && target != scpb.ToAbsent {
			existingColID = e.ColumnID
		}
	})
	if existingColID != 0 {
		panic(pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"cannot add TTL to table with the %s column already defined",
			catpb.TTLDefaultExpirationColumnName,
		))
	}

	// Build the column expression: current_timestamp() + interval_expr.
	ttlExpr := buildTTLColumnExpr(ttl)

	// Create column elements using addColumn pattern.
	colID := b.NextTableColumnID(tbl)
	colType := &scpb.ColumnType{
		TableID:                 tbl.TableID,
		ColumnID:                colID,
		TypeT:                   scpb.TypeT{Type: types.TimestampTZ},
		ElementCreationMetadata: scdecomp.NewElementCreationMetadata(b.EvalCtx().Settings.Version.ActiveVersion(b)),
	}

	// Use sanitizeColumnExpression to get properly type-annotated expression.
	// This ensures the expression has the expected format like
	// "current_timestamp():::TIMESTAMPTZ + '00:10:00':::INTERVAL".
	typedExpr, _, err := sanitizeColumnExpression(
		b,
		b.SemaCtx(),
		ttlExpr,
		colType,
		tree.TTLDefaultExpr,
	)
	if err != nil {
		panic(err)
	}
	wrappedExpr := b.WrapExpression(tbl.TableID, typedExpr)

	spec := addColumnSpec{
		tbl: tbl,
		col: &scpb.Column{
			TableID:  tbl.TableID,
			ColumnID: colID,
		},
		name: &scpb.ColumnName{
			TableID:  tbl.TableID,
			ColumnID: colID,
			Name:     string(catpb.TTLDefaultExpirationColumnName),
		},
		colType: colType,
		def: &scpb.ColumnDefaultExpression{
			TableID:    tbl.TableID,
			ColumnID:   colID,
			Expression: *wrappedExpr,
		},
		onUpdate: &scpb.ColumnOnUpdateExpression{
			TableID:    tbl.TableID,
			ColumnID:   colID,
			Expression: *wrappedExpr,
		},
		hidden:  true,
		notNull: true,
	}

	addColumn(b, spec, t)
}

// dropTTLColumn drops the crdb_internal_expiration column.
func dropTTLColumn(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, stmt tree.Statement, n tree.NodeFormatter,
) {
	tableElts := b.QueryByID(tbl.TableID)
	var col *scpb.Column
	var colName *scpb.ColumnName
	tableElts.FilterColumnName().ForEach(func(curr scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName) {
		if e.Name == string(catpb.TTLDefaultExpirationColumnName) && target != scpb.ToAbsent {
			colName = e
		}
	})
	if colName == nil {
		return // Column doesn't exist, nothing to do.
	}
	tableElts.FilterColumn().ForEach(func(curr scpb.Status, target scpb.TargetStatus, e *scpb.Column) {
		if e.ColumnID == colName.ColumnID && target != scpb.ToAbsent {
			col = e
		}
	})
	if col == nil {
		return // Column doesn't exist, nothing to do.
	}

	// Use the dropColumn logic.
	colElts := tableElts.Filter(hasColumnIDAttrFilter(col.ColumnID))
	dropColumn(b, tn, tbl, stmt, n, col, colElts, tree.DropDefault)
}

// updateTTLColumnExpressions updates the default and on-update expressions
// for the crdb_internal_expiration column when the ttl_expire_after interval
// is modified.
func updateTTLColumnExpressions(
	b BuildCtx, tbl *scpb.Table, tn *tree.TableName, ttl *catpb.RowLevelTTL,
) {
	tableElts := b.QueryByID(tbl.TableID)
	var colID catid.ColumnID
	tableElts.FilterColumnName().ForEach(func(curr scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName) {
		if e.Name == string(catpb.TTLDefaultExpirationColumnName) && target != scpb.ToAbsent {
			colID = e.ColumnID
		}
	})
	if colID == 0 {
		panic(errors.AssertionFailedf("TTL column not found"))
	}

	// Get the existing column.
	col := mustRetrieveColumnElem(b, tbl.TableID, colID)

	// Build new expression.
	ttlExpr := buildTTLColumnExpr(ttl)

	// Use panicIfInvalidNonComputedColumnExpr to get a properly typed expression.
	colName := string(catpb.TTLDefaultExpirationColumnName)
	typedExpr := panicIfInvalidNonComputedColumnExpr(
		b, tbl, tn.ToUnresolvedObjectName(), col, colName, ttlExpr, tree.TTLDefaultExpr,
	)
	wrappedExpr := b.WrapExpression(tbl.TableID, typedExpr)

	// Drop old expressions and add new ones.
	// Handle ColumnDefaultExpression.
	oldDefExpr := tableElts.FilterColumnDefaultExpression().Filter(
		func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnDefaultExpression) bool {
			return e.ColumnID == colID
		}).MustGetZeroOrOneElement()
	if oldDefExpr != nil {
		b.Drop(oldDefExpr)
	}
	b.Add(&scpb.ColumnDefaultExpression{
		TableID:    tbl.TableID,
		ColumnID:   colID,
		Expression: *wrappedExpr,
	})

	// Handle ColumnOnUpdateExpression.
	oldOnUpdate := tableElts.FilterColumnOnUpdateExpression().Filter(
		func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnOnUpdateExpression) bool {
			return e.ColumnID == colID
		}).MustGetZeroOrOneElement()
	if oldOnUpdate != nil {
		b.Drop(oldOnUpdate)
	}
	b.Add(&scpb.ColumnOnUpdateExpression{
		TableID:    tbl.TableID,
		ColumnID:   colID,
		Expression: *wrappedExpr,
	})
}

// isTTLParam returns true if this is a TTL storage parameter.
func isTTLParam(key string) bool {
	loweredKey := strings.ToLower(key)
	return strings.HasPrefix(loweredKey, "ttl")
}

// applyTTLStorageParamsSet processes TTL-related storage parameters for SET.
// It handles dropping the old RowLevelTTL element and adding the new one.
func applyTTLStorageParamsSet(
	b BuildCtx,
	tbl *scpb.Table,
	tn *tree.TableName,
	stmt tree.Statement,
	t *tree.AlterTableSetStorageParams,
	params tree.StorageParams,
) {
	if len(params) == 0 {
		return
	}

	origElem := b.QueryByID(tbl.TableID).FilterRowLevelTTL().MustGetZeroOrOneElement()

	// Build new TTL from original (or empty if none).
	var newTTL catpb.RowLevelTTL
	if origElem != nil {
		newTTL = origElem.RowLevelTTL
	}

	setter := tablestorageparam.NewTTLSetter(&newTTL, false /* isNewObject */)
	if err := storageparam.Set(
		b,
		b.SemaCtx(),
		b.EvalCtx(),
		params,
		setter,
	); err != nil {
		panic(err)
	}

	// Determine if we need column operations based on DurationExpr changes.
	origHasDuration := origElem != nil && origElem.RowLevelTTL.HasDurationExpr()
	newHasDuration := newTTL.HasDurationExpr()

	if !origHasDuration && newHasDuration {
		// Adding ttl_expire_after: create the TTL column.
		addTTLColumn(b, tbl, t, &newTTL)
	} else if origHasDuration && !newHasDuration {
		// Switching from ttl_expire_after to ttl_expiration_expression:
		// drop the TTL column.
		dropTTLColumn(b, tn, tbl, stmt, t)
	} else if origHasDuration && newHasDuration &&
		origElem.RowLevelTTL.DurationExpr != newTTL.DurationExpr {
		// Modifying ttl_expire_after value: update the column expressions.
		updateTTLColumnExpressions(b, tbl, tn, &newTTL)
	}

	// Validate the TTL expiration expression if present.
	var newTTLExpr *scpb.Expression
	if newTTL.HasExpirationExpr() {
		ttlExpr := b.TTLExpirationExpression(
			tbl.TableID,
			&newTTL,
			func() colinfo.ResultColumns {
				return getNonDropResultColumns(b, tbl.TableID)
			},
			func(columnName tree.Name) (exists, accessible, computed bool, id catid.ColumnID, typ *types.T) {
				return columnLookupFn(b, tbl.TableID, columnName)
			},
		)
		newTTLExpr = b.WrapExpression(tbl.TableID, ttlExpr)
	}

	// Construct new scpb.RowLevelTTL element with incremented SeqNum.
	var seqNum uint32
	if origElem != nil {
		seqNum = origElem.SeqNum + 1
	}
	newElem := &scpb.RowLevelTTL{
		TableID:     tbl.TableID,
		RowLevelTTL: newTTL,
		TTLExpr:     newTTLExpr,
		SeqNum:      seqNum,
	}

	// Drop old element and add new one.
	if origElem != nil {
		b.Drop(origElem)
	}
	b.Add(newElem)

	// If TTL is being added to a table with inbound FKs that have cascading
	// delete actions, send a notice about the performance implications.
	if origElem == nil && hasInboundFKWithCascadingDeleteAction(b, tbl) {
		b.EvalCtx().ClientNoticeSender.BufferClientNotice(
			b,
			pgnotice.Newf("Columns within table %s are referenced as foreign keys."+
				" This will make TTL deletion jobs more expensive as dependent rows"+
				" in other tables will need to be updated as well. To improve performance"+
				" of the TTL job, consider reducing the value of ttl_delete_batch_size.", tn.Object()),
		)
	}
}

// applyTTLStorageParamsReset processes TTL-related storage parameters for RESET.
// It handles dropping the old RowLevelTTL element and adding a new one if needed.
func applyTTLStorageParamsReset(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableResetStorageParams,
	params []string,
) {
	if len(params) == 0 {
		return
	}

	origElem := b.QueryByID(tbl.TableID).FilterRowLevelTTL().MustGetZeroOrOneElement()

	// Check if we're resetting 'ttl' or 'ttl_expire_after' which may require
	// dropping the TTL column.
	var resettingTTL, resettingExpireAfter bool
	for _, param := range params {
		switch strings.ToLower(param) {
		case "ttl":
			resettingTTL = true
		case "ttl_expire_after":
			resettingExpireAfter = true
		}
	}

	// Build new TTL from original (or empty if none).
	var newTTL *catpb.RowLevelTTL
	var origTTLExpr *scpb.Expression
	if origElem != nil {
		ttlCopy := origElem.RowLevelTTL
		newTTL = &ttlCopy
		origTTLExpr = origElem.TTLExpr
	}

	// Even if origElem is nil, we apply all the resets, since some of them
	// have side effects like sending notices.
	setter := tablestorageparam.NewTTLSetter(newTTL, false /* isNewObject */)
	if err := storageparam.Reset(
		b,
		b.EvalCtx(),
		params,
		setter,
	); err != nil {
		panic(err)
	}

	if origElem == nil {
		return
	}

	// Check if we need to drop the TTL column and/or the RowLevelTTL element.
	origHasDuration := origElem.RowLevelTTL.HasDurationExpr()
	needDropColumn := origHasDuration && (resettingTTL || resettingExpireAfter)

	// Always drop the old element first (required before column drop).
	b.Drop(origElem)
	if needDropColumn {
		dropTTLColumn(b, tn, tbl, stmt, t)
	}

	// If resetting 'ttl', we're done - TTL is completely removed.
	if resettingTTL {
		telemetry.Inc(sqltelemetry.RowLevelTTLDropped)
		return
	}

	// Construct and add new scpb.RowLevelTTL element with incremented SeqNum.
	newElem := &scpb.RowLevelTTL{
		TableID:     tbl.TableID,
		RowLevelTTL: *newTTL,
		SeqNum:      origElem.SeqNum + 1,
	}
	if newTTL.HasExpirationExpr() {
		// If the new TTL still has an expiration expression, retain the original
		// expression.
		newElem.TTLExpr = origTTLExpr
	}
	b.Add(newElem)
}

// AlterTableSetStorageParams implements ALTER TABLE ... SET {storage_param} in the declarative schema changer.
func AlterTableSetStorageParams(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableSetStorageParams,
) {
	var ttlParams, rbrConstraintParams, otherParams tree.StorageParams
	for _, param := range t.StorageParams {
		if isTTLParam(param.Key) {
			ttlParams = append(ttlParams, param)
		} else if isRBRUsingConstraintParam(param.Key) {
			rbrConstraintParams = append(rbrConstraintParams, param)
		} else {
			otherParams = append(otherParams, param)
		}
	}

	// Handle TTL params first, using the RowLevelTTL element.
	applyTTLStorageParamsSet(b, tbl, tn, stmt, t, ttlParams)

	// Handle RBR using constraint param.
	if len(rbrConstraintParams) > 0 {
		setRBRUsingConstraint(b, tbl, rbrConstraintParams)
	}

	if err := storageparam.StorageParamPreChecks(
		b,
		b.EvalCtx(),
		false, /* isNewObject */
		otherParams,
		nil, /* resetParams */
	); err != nil {
		panic(err)
	}

	for _, param := range otherParams {
		key := param.Key
		val, err := tablestorageparam.ParseAndValidate(b, b.SemaCtx(), b.EvalCtx(), param)
		if err != nil {
			panic(err) // tried to set an invalid value for param
		}

		// schema_locked uses a dedicated TableSchemaLocked element.
		if key == "schema_locked" {
			setSchemaLocked(b, tbl, val)
			continue
		}

		// Do extra validation for exclude_data_from_backup
		validateExcludeDataFromBackup(b, tbl, key)
		currElem := b.QueryByID(tbl.TableID).FilterTableStorageParam().Filter(
			func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.TableStorageParam) bool {
				return e.Name == key
			}).MustGetZeroOrOneElement()
		if currElem != nil {
			b.Drop(currElem)
		}
		if val != "" {
			newElem := scpb.TableStorageParam{
				TableID: tbl.TableID,
				Name:    key,
				Value:   val,
			}
			b.Add(&newElem)
		}
	}
}

func validateExcludeDataFromBackup(b BuildCtx, tbl *scpb.Table, key string) {
	if key != "exclude_data_from_backup" {
		return
	}
	if tbl.IsTemporary {
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot set data in a temporary table to be excluded from backup"))
	}
	// Check that the table does not have any incoming FK references. During a
	// backup, the rows of a table with ephemeral data will not be backed up, and
	// could result in a violation of FK constraints on restore. To prevent this,
	// we only allow a table with no incoming FK references to be marked as
	// ephemeral.
	if isTableReferencedByFK(b, tbl) {
		panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"cannot set data in a table with inbound foreign key constraints to be excluded from backup"))
	}
}

// isTableReferencedByFK returns true if the table has any inbound foreign key
// constraints where tbl is the referenced table.
func isTableReferencedByFK(b BuildCtx, tbl *scpb.Table) bool {
	hasInboundFK := false
	backRefs := b.BackReferences(tbl.TableID)
	// Check validated foreign key constraints
	backRefs.FilterForeignKeyConstraint().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.ForeignKeyConstraint) {
		if e.ReferencedTableID == tbl.TableID {
			hasInboundFK = true
		}
	})
	// Check unvalidated foreign key constraints
	backRefs.FilterForeignKeyConstraintUnvalidated().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.ForeignKeyConstraintUnvalidated) {
		if e.ReferencedTableID == tbl.TableID {
			hasInboundFK = true
		}
	})
	return hasInboundFK
}

// hasInboundFKWithCascadingDeleteAction returns true if the table has any
// inbound foreign key constraints with ON DELETE actions that cascade (i.e.,
// not NO_ACTION or RESTRICT). This is used to warn users that TTL deletions
// will be more expensive when dependent rows need to be updated.
func hasInboundFKWithCascadingDeleteAction(b BuildCtx, tbl *scpb.Table) bool {
	hasCascadingFK := false
	backRefs := b.BackReferences(tbl.TableID)
	// Check validated foreign key constraints
	backRefs.FilterForeignKeyConstraint().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.ForeignKeyConstraint) {
		if e.ReferencedTableID == tbl.TableID &&
			e.OnDeleteAction != semenumpb.ForeignKeyAction_NO_ACTION &&
			e.OnDeleteAction != semenumpb.ForeignKeyAction_RESTRICT {
			hasCascadingFK = true
		}
	})
	// Check unvalidated foreign key constraints
	backRefs.FilterForeignKeyConstraintUnvalidated().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.ForeignKeyConstraintUnvalidated) {
		if e.ReferencedTableID == tbl.TableID &&
			e.OnDeleteAction != semenumpb.ForeignKeyAction_NO_ACTION &&
			e.OnDeleteAction != semenumpb.ForeignKeyAction_RESTRICT {
			hasCascadingFK = true
		}
	})
	return hasCascadingFK
}

// AlterTableResetStorageParams implements ALTER TABLE ... RESET {storage_param} in the declarative schema changer.
func AlterTableResetStorageParams(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableResetStorageParams,
) {
	var ttlParams, otherParams []string
	hasRBRConstraintReset := false
	for _, param := range t.Params {
		if isTTLParam(param) {
			ttlParams = append(ttlParams, param)
		} else if isRBRUsingConstraintParam(param) {
			hasRBRConstraintReset = true
		} else {
			otherParams = append(otherParams, param)
		}
	}

	// Handle TTL params first, using the RowLevelTTL element.
	applyTTLStorageParamsReset(b, tn, tbl, stmt, t, ttlParams)

	// Handle RBR using constraint reset.
	if hasRBRConstraintReset {
		resetRBRUsingConstraint(b, tbl)
	}

	if err := storageparam.StorageParamPreChecks(
		b,
		b.EvalCtx(),
		false, /* isNewObject */
		nil,   /* setParams */
		otherParams,
	); err != nil {
		panic(err)
	}

	for _, key := range otherParams {
		// Validate the key is a known storage parameter.
		if err := tablestorageparam.IsValidParamKey(key); err != nil {
			panic(err)
		}

		// Get the reset value for this param. Most of the time this is the
		// zero value, but for some params it may be different.
		resetVal, err := tablestorageparam.GetResetValue(b, b.EvalCtx(), key)
		if err != nil {
			panic(err)
		}

		// schema_locked uses a dedicated TableSchemaLocked element.
		if key == "schema_locked" {
			setSchemaLocked(b, tbl, resetVal)
			continue
		}

		// Find and drop the existing element.
		currElem := b.QueryByID(tbl.TableID).FilterTableStorageParam().Filter(
			func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.TableStorageParam) bool {
				return e.Name == key
			}).MustGetZeroOrOneElement()
		if currElem != nil {
			b.Drop(currElem)
		}

		// If there's a non-empty reset value, add a new element with that value
		// (semantically resetting to the default value).
		if resetVal != "" {
			newElem := scpb.TableStorageParam{
				TableID: tbl.TableID,
				Name:    key,
				Value:   resetVal,
			}
			b.Add(&newElem)
		}
	}
}

// setRBRUsingConstraint handles SET infer_rbr_region_col_using_constraint. It
// validates the table is RBR, extracts the constraint name, validates the
// constraint is a suitable FK, and adds a TableLocalityRegionalByRowUsingConstraint
// element.
func setRBRUsingConstraint(b BuildCtx, tbl *scpb.Table, params tree.StorageParams) {
	// Check that the feature is enabled.
	if !sqlclustersettings.InferRegionUsingConstraintEnabled.Get(&b.ClusterSettings().SV) {
		panic(pgerror.Newf(
			pgcode.FeatureNotSupported,
			`storage parameter "%s" is not enabled; set the cluster setting`+
				` "feature.infer_rbr_region_col_using_constraint.enabled" to true to enable it`,
			catpb.RBRUsingConstraintTableSettingName,
		))
	}

	// Validate table is REGIONAL BY ROW.
	if !isTableLocalityRegionalByRow(b, tbl.TableID) {
		panic(pgerror.Newf(
			pgcode.InvalidParameterValue,
			`storage parameter "%s" can only be set on REGIONAL BY ROW tables`,
			catpb.RBRUsingConstraintTableSettingName,
		))
	}

	// Extract the constraint name from the param value.
	constraintName := extractRBRConstraintName(b, params)
	if constraintName == "" {
		return
	}

	// Resolve the constraint by name using the standard constraint resolution
	// mechanism, which finds both index-backed and non-index constraints.
	tableElts := b.QueryByID(tbl.TableID)
	constraintElems := b.ResolveConstraint(tbl.TableID, constraintName, ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.CREATE,
	})
	// Find the constraint ID from the resolved elements.
	var constraintID catid.ConstraintID
	constraintWithoutIndexName := constraintElems.FilterConstraintWithoutIndexName().
		MustGetZeroOrOneElement()
	if constraintWithoutIndexName != nil {
		constraintID = constraintWithoutIndexName.ConstraintID
	}

	// If we found a constraint ID, check if it's a FK (validated or unvalidated).
	var fkColumnIDs []catid.ColumnID
	if constraintID != 0 {
		tableElts.FilterForeignKeyConstraint().ForEach(
			func(_ scpb.Status, target scpb.TargetStatus, e *scpb.ForeignKeyConstraint) {
				if target == scpb.ToAbsent {
					return
				}
				if e.ConstraintID == constraintID {
					fkColumnIDs = e.ColumnIDs
				}
			},
		)
		if fkColumnIDs == nil {
			tableElts.FilterForeignKeyConstraintUnvalidated().ForEach(
				func(
					_ scpb.Status, target scpb.TargetStatus, e *scpb.ForeignKeyConstraintUnvalidated,
				) {
					if target == scpb.ToAbsent {
						return
					}
					if e.ConstraintID == constraintID {
						fkColumnIDs = e.ColumnIDs
					}
				},
			)
		}
	}
	if fkColumnIDs == nil {
		panic(pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"constraint %q is not a foreign key constraint",
			constraintName,
		))
	}

	// Get the region column name and ID.
	rbrElem := tableElts.FilterTableLocalityRegionalByRow().MustGetOneElement()
	regionColName := tree.Name(rbrElem.As)
	if regionColName == tree.RegionalByRowRegionNotSpecifiedName {
		regionColName = tree.RegionalByRowRegionDefaultColName
	}
	var regionColID catid.ColumnID
	tableElts.FilterColumnName().ForEach(
		func(_ scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName) {
			if target == scpb.ToAbsent {
				return
			}
			if tree.Name(e.Name) == regionColName {
				regionColID = e.ColumnID
			}
		},
	)
	if regionColID == 0 {
		panic(pgerror.Newf(
			pgcode.UndefinedColumn,
			`column %q does not exist`,
			regionColName,
		))
	}

	// Validate: region column must not be computed.
	tableElts.FilterColumnComputeExpression().ForEach(
		func(_ scpb.Status, target scpb.TargetStatus, e *scpb.ColumnComputeExpression) {
			if target == scpb.ToAbsent {
				return
			}
			if e.ColumnID == regionColID {
				panic(pgerror.Newf(
					pgcode.InvalidTableDefinition,
					"cannot use computed column %q as the region column in a REGIONAL BY ROW table with "+
						"the %q storage parameter specified",
					regionColName, catpb.RBRUsingConstraintTableSettingName,
				))
			}
		},
	)

	// Validate: FK must include the region column.
	fkHasRegionCol := false
	for _, colID := range fkColumnIDs {
		if colID == regionColID {
			fkHasRegionCol = true
			break
		}
	}
	if !fkHasRegionCol {
		panic(pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"cannot use constraint %q to determine the region column for REGIONAL BY ROW "+
				"as it does not include the region column %q",
			constraintName, regionColName,
		))
	}

	// Validate: FK must have more than just the region column.
	if len(fkColumnIDs) == 1 {
		panic(pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"cannot use constraint %q to determine the region column for REGIONAL BY ROW "+
				"as it only includes the region column",
			constraintName,
		))
	}

	// Validate: no computed columns may reference the region column.
	tableElts.FilterColumnComputeExpression().ForEach(
		func(_ scpb.Status, target scpb.TargetStatus, e *scpb.ColumnComputeExpression) {
			if target == scpb.ToAbsent {
				return
			}
			if e.ColumnID == regionColID {
				// Already checked above.
				return
			}
			for _, refColID := range e.Expression.ReferencedColumnIDs {
				if refColID == regionColID {
					colName := mustRetrieveColumnName(b, tbl.TableID, e.ColumnID)
					panic(pgerror.Newf(
						pgcode.InvalidTableDefinition,
						`computed column %q cannot reference the region column %q; the region `+
							`column value must be able to be determined from the non-region FK columns`,
						colName.Name, regionColName,
					))
				}
			}
		},
	)

	// Drop any existing element.
	existingElem := tableElts.FilterTableLocalityRegionalByRowUsingConstraint().MustGetZeroOrOneElement()
	if existingElem != nil {
		b.Drop(existingElem)
	}

	// Add the new element.
	b.Add(&scpb.TableLocalityRegionalByRowUsingConstraint{
		TableID:      tbl.TableID,
		ConstraintID: constraintID,
	})
}

// extractRBRConstraintName extracts the constraint name string from the
// infer_rbr_region_col_using_constraint storage parameter value.
func extractRBRConstraintName(b BuildCtx, params tree.StorageParams) tree.Name {
	paramVal := params.GetVal(catpb.RBRUsingConstraintTableSettingName)
	if paramVal == nil {
		return ""
	}
	if paramVal == tree.DNull {
		panic(pgerror.Newf(
			pgcode.InvalidParameterValue,
			`storage parameter "%s" cannot be NULL`, catpb.RBRUsingConstraintTableSettingName,
		))
	}
	// The expressions may be an unresolved name. Cast it as a string.
	paramVal = paramparse.UnresolvedNameToStrVal(paramVal)
	typedExpr, err := schemaexpr.SanitizeVarFreeExpr(
		b, paramVal, types.String, "RBR_USING_CONSTRAINT_NAME", b.SemaCtx(),
		volatility.Volatile, false, /* allowAssignmentCast */
	)
	if err != nil {
		panic(err)
	}
	d, err := eval.Expr(b, b.EvalCtx(), typedExpr)
	if err != nil {
		panic(err)
	}
	constraintName, isStringVal := tree.AsDString(d)
	if !isStringVal {
		panic(pgerror.Newf(
			pgcode.InvalidParameterValue,
			`storage parameter "%s" must be a string`, catpb.RBRUsingConstraintTableSettingName,
		))
	}
	return tree.Name(constraintName)
}

// resetRBRUsingConstraint handles RESET infer_rbr_region_col_using_constraint.
// It drops the existing TableLocalityRegionalByRowUsingConstraint element if
// present.
func resetRBRUsingConstraint(b BuildCtx, tbl *scpb.Table) {
	existingElem := b.QueryByID(tbl.TableID).
		FilterTableLocalityRegionalByRowUsingConstraint().MustGetZeroOrOneElement()
	if existingElem != nil {
		b.Drop(existingElem)
	}
}

// setSchemaLocked sets the schema_locked storage parameter using the dedicated
// TableSchemaLocked element. The val is parsed as a boolean; empty string is
// treated as false.
func setSchemaLocked(b BuildCtx, tbl *scpb.Table, val string) {
	locked, _ := strconv.ParseBool(val)
	currElem := b.QueryByID(tbl.TableID).FilterTableSchemaLocked().MustGetZeroOrOneElement()
	if locked {
		// Setting schema_locked=true means TableSchemaLocked should be PUBLIC.
		b.Add(&scpb.TableSchemaLocked{TableID: tbl.TableID})
	} else if currElem != nil {
		// Setting schema_locked=false means TableSchemaLocked should be ABSENT.
		b.Drop(currElem)
	}
}
