// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam/tablestorageparam"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// AlterTableSetStorageParams implements ALTER TABLE ... SET {storage_param} in the declarative schema changer.
func AlterTableSetStorageParams(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableSetStorageParams,
) {
	if err := storageparam.StorageParamPreChecks(
		b,
		b.EvalCtx(),
		false, /* isNewObject */
		t.StorageParams,
		nil, /* resetParams */
	); err != nil {
		panic(err)
	}
	for _, param := range t.StorageParams {
		val, err := tablestorageparam.ParseAndValidate(b, b.SemaCtx(), b.EvalCtx(), param)
		if err != nil {
			panic(err) // tried to set an invalid value for param
		}
		key := param.Key
		if strings.HasPrefix(strings.ToLower(key), "ttl") {
			panic(scerrors.NotImplementedErrorf(t, redact.Sprintf("%s not implemented yet", redact.SafeString(key))))
		}
		if key == catpb.RBRUsingConstraintTableSettingName {
			panic(scerrors.NotImplementedErrorf(t, "infer_rbr_region_col_using_constraint not implemented yet"))
		}
		if key == "schema_locked" {
			// schema_locked has a dedicated element and will be handled differently
			panic(scerrors.NotImplementedErrorf(t, "schema_locked not implemented yet"))
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
