// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/semenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam/tablestorageparam"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// validateStorageParamKey validates a storage param key and panics with
// NotImplementedError if the param is not yet supported in the declarative
// schema changer.
func validateStorageParamKey(t tree.NodeFormatter, key string) {
	loweredKey := strings.ToLower(key)
	// These TTL params still require legacy schema changer because they
	// affect column management.
	switch loweredKey {
	case "ttl", "ttl_expire_after":
		panic(scerrors.NotImplementedErrorf(t, redact.Sprintf("%s not implemented yet", redact.SafeString(key))))
	}
	if loweredKey == catpb.RBRUsingConstraintTableSettingName {
		panic(scerrors.NotImplementedErrorf(t, "infer_rbr_region_col_using_constraint not implemented yet"))
	}
}

// isTTLParam returns true if this is a TTL storage parameter.
func isTTLParam(key string) bool {
	loweredKey := strings.ToLower(key)
	return strings.HasPrefix(loweredKey, "ttl")
}

// applyTTLStorageParamsSet processes TTL-related storage parameters for SET.
// It returns:
// - origElem: the original RowLevelTTL element (nil if table had no TTL).
// - newElem: the updated RowLevelTTL element (nil if no TTL params were handled).
// - changed: true if any TTL param was modified.
func applyTTLStorageParamsSet(
	b BuildCtx, tbl *scpb.Table, params tree.StorageParams,
) (origElem *scpb.RowLevelTTL, newElem *scpb.RowLevelTTL, changed bool) {
	origElem = b.QueryByID(tbl.TableID).FilterRowLevelTTL().MustGetZeroOrOneElement()

	if len(params) == 0 {
		return origElem, nil, false
	}

	// Build new TTL from original (or empty if none).
	var newTTL catpb.RowLevelTTL
	var origTTLExpr *scpb.Expression
	if origElem != nil {
		newTTL = origElem.RowLevelTTL
		origTTLExpr = origElem.TTLExpr
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

	// Validate the TTL expiration expression if present.
	b.ValidateTTLExpirationExpression(
		tbl.TableID,
		&newTTL,
		func() colinfo.ResultColumns {
			return getNonDropResultColumns(b, tbl.TableID)
		},
		func(columnName tree.Name) (exists, accessible, computed bool, id catid.ColumnID, typ *types.T) {
			return columnLookupFn(b, tbl.TableID, columnName)
		},
	)

	// Construct new scpb.RowLevelTTL element with incremented SeqNum.
	var seqNum uint32
	if origElem != nil {
		seqNum = origElem.SeqNum + 1
	}
	newElem = &scpb.RowLevelTTL{
		TableID:     tbl.TableID,
		RowLevelTTL: newTTL,
		TTLExpr:     origTTLExpr,
		SeqNum:      seqNum,
	}
	return origElem, newElem, true
}

// applyTTLStorageParamsReset processes TTL-related storage parameters for RESET.
// It returns:
// - origElem: the original RowLevelTTL element (nil if table had no TTL).
// - newElem: the updated RowLevelTTL element (nil if no TTL params were handled).
// - changed: true if any TTL param was modified.
func applyTTLStorageParamsReset(
	b BuildCtx, tbl *scpb.Table, params []string,
) (origElem *scpb.RowLevelTTL, newElem *scpb.RowLevelTTL, changed bool) {
	origElem = b.QueryByID(tbl.TableID).FilterRowLevelTTL().MustGetZeroOrOneElement()

	if len(params) == 0 {
		return origElem, nil, false
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
		return nil, nil, false
	}

	// Construct new scpb.RowLevelTTL element with incremented SeqNum.
	newElem = &scpb.RowLevelTTL{
		TableID:     tbl.TableID,
		RowLevelTTL: *newTTL,
		TTLExpr:     origTTLExpr,
		SeqNum:      origElem.SeqNum + 1,
	}
	return origElem, newElem, true
}

// AlterTableSetStorageParams implements ALTER TABLE ... SET {storage_param} in the declarative schema changer.
func AlterTableSetStorageParams(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableSetStorageParams,
) {
	var ttlParams, otherParams tree.StorageParams
	for _, param := range t.StorageParams {
		validateStorageParamKey(t, param.Key)
		if isTTLParam(param.Key) {
			ttlParams = append(ttlParams, param)
		} else {
			otherParams = append(otherParams, param)
		}
	}

	// Handle TTL params first, using the RowLevelTTL element.
	origTTL, newTTL, ttlChanged := applyTTLStorageParamsSet(b, tbl, ttlParams)
	if ttlChanged {
		if origTTL != nil {
			b.Drop(origTTL)
		}
		b.Add(newTTL)

		// If TTL is being added to a table with inbound FKs that have cascading
		// delete actions, send a notice about the performance implications.
		if origTTL == nil && hasInboundFKWithCascadingDeleteAction(b, tbl) {
			b.EvalCtx().ClientNoticeSender.BufferClientNotice(
				b,
				pgnotice.Newf("Columns within table %s are referenced as foreign keys."+
					" This will make TTL deletion jobs more expensive as dependent rows"+
					" in other tables will need to be updated as well. To improve performance"+
					" of the TTL job, consider reducing the value of ttl_delete_batch_size.", tn.Object()),
			)
		}
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
		panic(errors.New("cannot set data in a table with inbound foreign key constraints to be excluded from backup"))
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
	for _, param := range t.Params {
		validateStorageParamKey(t, param)
		if isTTLParam(param) {
			ttlParams = append(ttlParams, param)
		} else {
			otherParams = append(otherParams, param)
		}
	}

	// Handle TTL params first, using the RowLevelTTL element.
	origTTL, newTTL, ttlChanged := applyTTLStorageParamsReset(b, tbl, ttlParams)
	if ttlChanged {
		if origTTL != nil {
			b.Drop(origTTL)
		}
		b.Add(newTTL)
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
