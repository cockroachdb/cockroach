// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/kr/pretty"
)

type operation struct {
	// Operation name.
	name redact.RedactableString
	// List of schema names, e.g., column names, which are modified in the query.
	schemaList []string
	// Schema change query.
	query string
	// Function to check existing schema.
	schemaExistsFn func(catalog.TableDescriptor, catalog.TableDescriptor, string) (bool, error)
}

// migrateTable is run during a migration to a new version and changes an existing
// table's schema based on schemaChangeQuery. The schema-change is ignored if the
// table already has the required changes.
//
// This function reads the existing table descriptor from storage and passes it
// to schemaExists function to verify whether the schema-change already exists or
// not. If the change is already done, the function does not perform the change
// again, which makes migrateTable idempotent.
//
// schemaExists function should be customized based on the table being modified,
// ignoring the fields that do not matter while comparing with an existing descriptor.
//
// If multiple changes are done in the same query, e.g., if multiple columns are
// added, the function should check all changes to exist or absent, returning
// an error if changes exist partially.
func migrateTable(
	ctx context.Context,
	_ clusterversion.ClusterVersion,
	d migration.TenantDeps,
	op operation,
	storedTableID descpb.ID,
	expectedTable catalog.TableDescriptor,
) error {
	for {
		// - Fetch the table, reading its descriptor from storage.
		// - Check if any mutation jobs exist for the table. These mutations can
		//   belong to a previous migration attempt that failed.
		// - If any mutation job exists:
		//   - Wait for the ongoing mutations to complete.
		// 	 - Continue to the beginning of the loop to cater for the mutations
		//	   that may have started while waiting for existing mutations to complete.
		// - Check if the intended schema-changes already exist.
		//   - If the changes already exist, skip the schema-change and return as
		//     the changes are already done in a previous migration attempt.
		//   - Otherwise, perform the schema-change and return.

		log.Infof(ctx, "performing table migration operation %v", op.name)

		// Retrieve the table.
		storedTable, err := readTableDescriptor(ctx, d, storedTableID)
		if err != nil {
			return err
		}

		// Wait for any in-flight schema changes to complete.
		if mutations := storedTable.GetMutationJobs(); len(mutations) > 0 {
			for _, mutation := range mutations {
				log.Infof(ctx, "waiting for the mutation job %v to complete", mutation.JobID)
				if _, err := d.InternalExecutor.Exec(ctx, "migration-mutations-wait",
					nil, "SHOW JOB WHEN COMPLETE $1", mutation.JobID); err != nil {
					return err
				}
			}
			continue
		}

		// Ignore the schema change if the table already has the required schema.
		// Expect all or none.
		var exists bool
		for i, schemaName := range op.schemaList {
			hasSchema, err := op.schemaExistsFn(storedTable, expectedTable, schemaName)
			if err != nil {
				return errors.Wrapf(err, "error while validating descriptors during"+
					" operation %s", op.name)
			}
			if i > 0 && exists != hasSchema {
				return errors.Errorf("error while validating descriptors. observed"+
					" partial schema exists while performing %v", op.name)
			}
			exists = hasSchema
		}
		if exists {
			log.Infof(ctx, "skipping %s operation as the schema change already exists.", op.name)
			return nil
		}

		// Modify the table.
		log.Infof(ctx, "performing operation: %s", op.name)
		if _, err := d.InternalExecutor.ExecEx(
			ctx,
			fmt.Sprintf("migration-alter-table-%d", storedTableID),
			nil, /* txn */
			sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
			op.query); err != nil {
			return err
		}
		return nil
	}
}

func readTableDescriptor(
	ctx context.Context, d migration.TenantDeps, tableID descpb.ID,
) (catalog.TableDescriptor, error) {
	var t catalog.TableDescriptor

	if err := d.CollectionFactory.Txn(ctx, d.InternalExecutor, d.DB, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) (err error) {
		t, err = descriptors.GetImmutableTableByID(ctx, txn, tableID, tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				AvoidLeased: true,
				Required:    true,
			},
		})
		return err
	}); err != nil {
		return nil, err
	}
	return t, nil
}

// ensureProtoMessagesAreEqual verifies whether the given protobufs are equal or
// not, returning an error if they are not equal.
func ensureProtoMessagesAreEqual(expected, found protoutil.Message) error {
	expectedBytes, err := protoutil.Marshal(expected)
	if err != nil {
		return err
	}
	foundBytes, err := protoutil.Marshal(found)
	if err != nil {
		return err
	}
	if bytes.Equal(expectedBytes, foundBytes) {
		return nil
	}
	return errors.Errorf("expected descriptor doesn't match "+
		"with found descriptor: %s", strings.Join(pretty.Diff(expected, found), "\n"))
}

// hasColumn returns true if storedTable already has the given column, comparing
// with expectedTable.
// storedTable descriptor must be read from system storage as compared to reading
// from the systemschema package. On the contrary, expectedTable must be accessed
// directly from systemschema package.
// This function returns an error if the column exists but doesn't match with the
// expectedTable descriptor. The comparison is not strict as several descriptor
// fields are ignored.
func hasColumn(storedTable, expectedTable catalog.TableDescriptor, colName string) (bool, error) {
	storedCol, err := storedTable.FindColumnWithName(tree.Name(colName))
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return false, nil
		}
		return false, err
	}

	expectedCol, err := expectedTable.FindColumnWithName(tree.Name(colName))
	if err != nil {
		return false, errors.Wrapf(err, "columns name %s is invalid.", colName)
	}

	expectedCopy := expectedCol.ColumnDescDeepCopy()
	storedCopy := storedCol.ColumnDescDeepCopy()

	storedCopy.ID = 0
	expectedCopy.ID = 0

	if err = ensureProtoMessagesAreEqual(&expectedCopy, &storedCopy); err != nil {
		return false, err
	}
	return true, nil
}

// hasIndex returns true if storedTable already has the given index, comparing
// with expectedTable.
// storedTable descriptor must be read from system storage as compared to reading
// from the systemschema package. On the contrary, expectedTable must be accessed
// directly from systemschema package.
// This function returns an error if the index exists but doesn't match with the
// expectedTable descriptor. The comparison is not strict as several descriptor
// fields are ignored.
func hasIndex(storedTable, expectedTable catalog.TableDescriptor, indexName string) (bool, error) {
	storedIdx, err := storedTable.FindIndexWithName(indexName)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return false, nil
		}
		return false, err
	}
	expectedIdx, err := expectedTable.FindIndexWithName(indexName)
	if err != nil {
		return false, errors.Wrapf(err, "index name %s is invalid", indexName)
	}
	storedCopy := storedIdx.IndexDescDeepCopy()
	expectedCopy := expectedIdx.IndexDescDeepCopy()
	// Ignore the fields that don't matter in the comparison.
	storedCopy.ID = 0
	expectedCopy.ID = 0
	storedCopy.Version = 0
	expectedCopy.Version = 0
	// CreatedExplicitly is an ignored field because there exists an inconsistency
	// between CREATE TABLE (... INDEX) and CREATE INDEX.
	// See https://github.com/cockroachdb/cockroach/issues/65929.
	storedCopy.CreatedExplicitly = false
	expectedCopy.CreatedExplicitly = false
	storedCopy.StoreColumnNames = []string{}
	expectedCopy.StoreColumnNames = []string{}
	storedCopy.StoreColumnIDs = []descpb.ColumnID{0, 0, 0}
	expectedCopy.StoreColumnIDs = []descpb.ColumnID{0, 0, 0}

	if err = ensureProtoMessagesAreEqual(&expectedCopy, &storedCopy); err != nil {
		return false, err
	}
	return true, nil
}
