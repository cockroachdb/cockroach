// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"bytes"
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
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

// waitForJobStatement is the statement used to wait for an ongoing job to
// complete.
const waitForJobStatement = "SHOW JOBS WHEN COMPLETE VALUES ($1)"

// migrateTable is run during an upgrade to a new version and changes an existing
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
	d upgrade.TenantDeps,
	op operation,
	storedTableID descpb.ID,
	expectedTable catalog.TableDescriptor,
) error {
	for {
		// - Fetch the table, reading its descriptor from storage.
		// - Check if any mutation jobs exist for the table. These mutations can
		//   belong to a previous upgrade attempt that failed.
		// - If any mutation job exists:
		//   - Wait for the ongoing mutations to complete.
		// 	 - Continue to the beginning of the loop to cater for the mutations
		//	   that may have started while waiting for existing mutations to complete.
		// - Check if the intended schema-changes already exist.
		//   - If the changes already exist, skip the schema-change and return as
		//     the changes are already done in a previous upgrade attempt.
		//   - Otherwise, perform the schema-change and return.

		log.Infof(ctx, "performing table migration operation %v", op.name)

		// Retrieve the table.
		storedTable, err := readTableDescriptor(ctx, d, storedTableID)
		if err != nil {
			return err
		}

		// Wait for any in-flight schema changes to complete.
		// Check legacy schema changer jobs.
		if mutations := storedTable.GetMutationJobs(); len(mutations) > 0 {
			for _, mutation := range mutations {
				log.Infof(ctx, "waiting for the mutation job %v to complete", mutation.JobID)
				if _, err := d.InternalExecutor.Exec(ctx, "migration-mutations-wait",
					nil, waitForJobStatement, mutation.JobID); err != nil {
					return err
				}
			}
			continue
		}
		// Check declarative schema changer jobs.
		if state := storedTable.GetDeclarativeSchemaChangerState(); state != nil && state.JobID != catpb.InvalidJobID {
			log.Infof(ctx, "waiting for the mutation job %v to complete", state.JobID)
			if _, err := d.InternalExecutor.Exec(ctx, "migration-mutations-wait",
				nil, waitForJobStatement, state.JobID); err != nil {
				return err
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
			redact.Sprintf("migration-alter-table-%d", storedTableID),
			nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			op.query); err != nil {
			return err
		}
		return nil
	}
}

func readTableDescriptor(
	ctx context.Context, d upgrade.TenantDeps, tableID descpb.ID,
) (catalog.TableDescriptor, error) {
	var t catalog.TableDescriptor

	if err := d.DB.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) (err error) {
		t, err = txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, tableID)
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
	storedCol := catalog.FindColumnByName(storedTable, colName)
	if storedCol == nil {
		return false, nil
	}

	expectedCol, err := catalog.MustFindColumnByName(expectedTable, colName)
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

// columnExists returns true if storedTable contains a column with the given
// colName. Unlike hasColumn, it does not check that the column descriptor in
// storedTable and expectedTable match.
//
// This weaker check should be used when a migration/multiple migrations
// alter(s) the same column multiple times in order to ensure the migration(s)
// remain(s) idempotent. Consider the following series of (sub)migrations:
//  1. column C is first added to a table as nullable (NULL)
//  2. column C is backfilled with non-NULL values
//  3. column C is altered to be not nullable (NOT NULL)
//
// When we are deciding whether (sub)migration 1 should run, we can either
// (a) compare it to the expected descriptor after the column has been added
// but before it has been altered to be NOT NULL or (b) compare it to the
// expected descriptor after the column has been altered to be NOT NULL.
//
// If we choose to do (a) and for some reason after (sub)migration 3 is
// completed, we need to restart and run (sub)migration 1 again, hasColumn
// would now return an error because the column exists and is NOT NULL but the
// expected descriptor we have has it as NULL.
//
// If we choose to do (b) and for some reason after (sub)migration 1 is
// completed but before (sub)migration 3 runs, we restart and try
// (sub)migration 1 again, hasColumn would also now return an error because the
// column exists and is NULL when the expected descriptor has it as NOT NULL.
//
// In either case, the cluster would enter an unrecoverable state where it will
// repeatedly attempt to perform (sub)migration 1 and fail, preventing the
// migration and any further migrations from running.
func columnExists(
	storedTable, expectedTable catalog.TableDescriptor, colName string,
) (bool, error) {
	return catalog.FindColumnByName(storedTable, colName) != nil, nil
}

// columnExistsAndIsNotNull returns true if storedTable contains a non-nullable
// (NOT NULL) column with the given colName. Like columnExists, it does not
// check that the column descriptor in storedTable and expectedTable match and
// it should be used when a migration/multiple migrations alter(s) the same
// column multiple times. See the comment for columnExists for the reasoning
// behind this.
func columnExistsAndIsNotNull(
	storedTable, expectedTable catalog.TableDescriptor, colName string,
) (bool, error) {
	storedCol := catalog.FindColumnByName(storedTable, colName)
	return storedCol != nil && !storedCol.IsNullable(), nil
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
	storedIdx := catalog.FindIndexByName(storedTable, indexName)
	if storedIdx == nil {
		return false, nil
	}
	expectedIdx, err := catalog.MustFindIndexByName(expectedTable, indexName)
	if err != nil {
		return false, errors.Wrapf(err, "index name %s is invalid", indexName)
	}
	// Ignore the fields that don't matter in the comparison.
	storedCopy := indexDescForComparison(storedIdx)
	expectedCopy := indexDescForComparison(expectedIdx)
	if err = ensureProtoMessagesAreEqual(expectedCopy, storedCopy); err != nil {
		return false, err
	}
	return true, nil
}

// indexDescForComparison extracts an index descriptor from an index with
// numerical fields zeroed so that the meaning can be compared directly even
// if the numerical values differ.
func indexDescForComparison(idx catalog.Index) *descpb.IndexDescriptor {
	desc := idx.IndexDescDeepCopy()
	desc.ID = 0
	desc.Version = 0
	desc.ConstraintID = 0
	// CreatedExplicitly is an ignored field because there exists an inconsistency
	// between CREATE TABLE (... INDEX) and CREATE INDEX.
	// See https://github.com/cockroachdb/cockroach/issues/65929.
	desc.CreatedExplicitly = false

	// Clear out the column IDs, but retain their length. Column IDs may
	// change. Note that we retain the name slices. Those should match.
	for i := range desc.StoreColumnIDs {
		desc.StoreColumnIDs[i] = 0
	}
	for i := range desc.KeyColumnIDs {
		desc.KeyColumnIDs[i] = 0
	}
	for i := range desc.KeySuffixColumnIDs {
		desc.KeySuffixColumnIDs[i] = 0
	}
	for i := range desc.CompositeColumnIDs {
		desc.CompositeColumnIDs[i] = 0
	}

	desc.CreatedAtNanos = 0
	return &desc
}

// doesNotHaveIndex returns true if storedTable does not have an index named indexName.
func doesNotHaveIndex(
	storedTable, expectedTable catalog.TableDescriptor, indexName string,
) (bool, error) {
	idx := catalog.FindIndexByName(storedTable, indexName)
	return idx == nil, nil
}

// hasColumnFamily returns true if storedTable already has the given column
// family, comparing with expectedTable. storedTable descriptor must be read
// from system storage as compared to reading from the systemschema package. On
// the contrary, expectedTable must be accessed directly from systemschema
// package. This function returns an error if the column doesn't exist in the
// expectedTable descriptor.
func hasColumnFamily(
	storedTable, expectedTable catalog.TableDescriptor, colFamily string,
) (bool, error) {
	var storedFamily, expectedFamily *descpb.ColumnFamilyDescriptor
	for _, fam := range storedTable.GetFamilies() {
		if fam.Name == colFamily {
			storedFamily = &fam
			break
		}
	}
	if storedFamily == nil {
		return false, nil
	}

	for _, fam := range expectedTable.GetFamilies() {
		if fam.Name == colFamily {
			expectedFamily = &fam
			break
		}
	}
	if expectedFamily == nil {
		return false, errors.Errorf("column family %s does not exist", colFamily)
	}

	// Check that columns match.
	storedFamilyCols := storedFamily.ColumnNames
	expectedFamilyCols := expectedFamily.ColumnNames
	if len(storedFamilyCols) != len(expectedFamilyCols) {
		return false, nil
	}
	for i, storedCol := range storedFamilyCols {
		if storedCol != expectedFamilyCols[i] {
			return false, nil
		}
	}
	return true, nil
}

// onlyHasColumnFamily returns true if storedTable has only the given column
// family, comparing with expectedTable. storedTable descriptor must be read
// from system storage as compared to reading from the systemschema package. On
// the contrary, expectedTable must be accessed directly from systemschema
// package. This function returns an error if there is more than one column
// family, or if the only column family does not match the provided family name.
func onlyHasColumnFamily(
	storedTable, expectedTable catalog.TableDescriptor, colFamily string,
) (bool, error) {
	var storedFamily, expectedFamily *descpb.ColumnFamilyDescriptor
	storedFamilies := storedTable.GetFamilies()
	if len(storedFamilies) > 1 {
		return false, nil
	}
	if storedFamilies[0].Name == colFamily {
		storedFamily = &storedFamilies[0]
	}

	if storedFamily == nil {
		return false, nil
	}

	expectedFamilies := expectedTable.GetFamilies()
	if expectedFamilies[0].Name == colFamily {
		expectedFamily = &expectedFamilies[0]
	}

	if expectedFamily == nil {
		return false, errors.Errorf("column family %s does not exist", colFamily)
	}

	// Check that columns match.
	storedFamilyCols := storedFamily.ColumnNames
	expectedFamilyCols := expectedFamily.ColumnNames
	if len(storedFamilyCols) != len(expectedFamilyCols) {
		return false, nil
	}
	for i, storedCol := range storedFamilyCols {
		if storedCol != expectedFamilyCols[i] {
			return false, nil
		}
	}
	return true, nil
}
