// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// fixInvalidObjectsThatLookLikeBadUserfileConstraint attempts to remove a
// dangling table descriptor mutation for foreign key constraints on
// usefile-related upload_payload tables.
//
// It only proceeds with the fix if
//
//   - the fk mutation it the _only_ mutation on the table,
//   - the table name, column names, and column type all look like a userfile table, and
//   - the fk is referencing a table whose name, column names, and column types all look like a userfile table.
//
// To fix the table, we remove the mutation as the FK constraint is unnecessary
// in the current implementation.
func fixInvalidObjectsThatLookLikeBadUserfileConstraint(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return d.InternalExecutorFactory.DescsTxnWithExecutor(ctx, d.DB, nil,
		func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection, ie sqlutil.InternalExecutor) error {
			query := `SELECT * FROM crdb_internal.invalid_objects`
			rows, err := ie.QueryIterator(ctx, "find-invalid-descriptors", txn, query)
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()

			var hasNext bool
			for hasNext, err = rows.Next(ctx); hasNext && err == nil; hasNext, err = rows.Next(ctx) {
				// crdb_internal.invalid_objects has five columns: id, database name, schema name, table name, error.
				row := rows.Cur()
				tableID := descpb.ID(tree.MustBeDInt(row[0]))
				errString := string(tree.MustBeDString(row[4]))
				if veryLikelyKnownUserfileBreakage(ctx, txn, descriptors, tableID, errString) {
					log.Infof(ctx, "attempting to fix invalid table descriptor %d assuming it is a userfile-related table", tableID)
					mutTableDesc, err := descriptors.ByID(txn).Mutable().Table(ctx, tableID)
					if err != nil {
						return err
					}
					mutTableDesc.Mutations = nil
					mutTableDesc.MutationJobs = nil
					if err := descriptors.WriteDesc(ctx, false, mutTableDesc, txn); err != nil {
						return err
					}
				}
			}
			if err != nil {
				// TODO(ssd): We always return a nil error here because I'm not sure that this
				// would be worth failing an upgrade for.
				log.Warningf(ctx, "could not fix broken userfile: %v", err)
			}
			return nil
		})
}

// veryLikelyKnownUserfileBreakage returns true if the given descriptor id and
// error message from crdb_internal.invalid_objects is likely related to a known
// userfile-related table corruption.
func veryLikelyKnownUserfileBreakage(
	ctx context.Context, txn *kv.Txn, descriptors *descs.Collection, id descpb.ID, errMsg string,
) bool {
	if !strings.HasSuffix(errMsg, "job not found") {
		return false
	}

	tableDesc, err := descriptors.ByID(txn).Immutable().Table(ctx, id)
	if err != nil {
		return false
	}
	return tableLooksLikeUserfilePayloadTable(tableDesc) && mutationsLookLikeuserfilePayloadCorruption(ctx, txn, tableDesc, descriptors)
}

func tableLooksLikeUserfilePayloadTable(tableDesc catalog.TableDescriptor) bool {
	columns := tableDesc.PublicColumns()
	return strings.HasSuffix(tableDesc.GetName(), "_upload_payload") &&
		len(columns) == 3 &&
		(columns[0].ColName() == "file_id" && columns[0].GetType().Oid() == types.Uuid.Oid()) &&
		(columns[1].ColName() == "byte_offset" && columns[1].GetType().Oid() == types.Int.Oid()) &&
		(columns[2].ColName() == "payload" && columns[2].GetType().Oid() == types.Bytes.Oid())
}

func tableLooksLikeUserfileFileTable(tableDesc catalog.TableDescriptor) bool {
	columns := tableDesc.PublicColumns()
	return strings.HasSuffix(tableDesc.GetName(), "_upload_files") &&
		len(columns) == 5 &&
		(columns[0].ColName() == "filename" && columns[0].GetType().Oid() == types.String.Oid()) &&
		(columns[1].ColName() == "file_id" && columns[1].GetType().Oid() == types.Uuid.Oid()) &&
		(columns[2].ColName() == "file_size" && columns[2].GetType().Oid() == types.Int.Oid()) &&
		(columns[3].ColName() == "username" && columns[3].GetType().Oid() == types.String.Oid()) &&
		(columns[4].ColName() == "upload_time" && columns[4].GetType().Oid() == types.Timestamp.Oid())
}

func mutationsLookLikeuserfilePayloadCorruption(
	ctx context.Context,
	txn *kv.Txn,
	tableDesc catalog.TableDescriptor,
	descriptors *descs.Collection,
) bool {
	if len(tableDesc.GetMutationJobs()) != 1 {
		return false
	}
	if len(tableDesc.AllMutations()) != 1 {
		return false
	}
	mutation := tableDesc.AllMutations()[0]
	if mutation.Adding() && mutation.DeleteOnly() {
		if fkConstraint := mutation.AsForeignKey(); fkConstraint != nil {
			targetTableDesc, err := descriptors.ByID(txn).Immutable().Table(ctx, fkConstraint.GetReferencedTableID())
			if err != nil {
				return false
			}
			if tableLooksLikeUserfileFileTable(targetTableDesc) {
				return true
			}

		}
	}
	return false
}
