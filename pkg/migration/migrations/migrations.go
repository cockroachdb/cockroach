// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package migrations contains the implementation of migrations. It is imported
// by the server library.
//
// This package registers the migrations with the migration package.
package migrations

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
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
	"github.com/kr/pretty"
)

// GetMigration returns the migration corresponding to this version if
// one exists.
func GetMigration(key clusterversion.ClusterVersion) (migration.Migration, bool) {
	m, ok := registry[key]
	return m, ok
}

// NoPrecondition is a PreconditionFunc that doesn't check anything.
func NoPrecondition(context.Context, clusterversion.ClusterVersion, migration.TenantDeps) error {
	return nil
}

// registry defines the global mapping between a cluster version and the
// associated migration. The migration is only executed after a cluster-wide
// bump of the corresponding version gate.
var registry = make(map[clusterversion.ClusterVersion]migration.Migration)

var migrations = []migration.Migration{
	migration.NewSystemMigration(
		"use unreplicated TruncatedState and RangeAppliedState for all ranges",
		toCV(clusterversion.TruncatedAndRangeAppliedStateMigration),
		truncatedStateMigration,
	),
	migration.NewSystemMigration(
		"purge all replicas using the replicated TruncatedState",
		toCV(clusterversion.PostTruncatedAndRangeAppliedStateMigration),
		postTruncatedStateMigration,
	),
	migration.NewSystemMigration(
		"stop using monolithic encryption-at-rest registry for all stores",
		toCV(clusterversion.RecordsBasedRegistry),
		recordsBasedRegistryMigration,
	),
	migration.NewTenantMigration(
		"add the systems.join_tokens table",
		toCV(clusterversion.JoinTokensTable),
		NoPrecondition,
		joinTokensTableMigration,
	),
	migration.NewTenantMigration(
		"delete the deprecated namespace table descriptor at ID=2",
		toCV(clusterversion.DeleteDeprecatedNamespaceTableDescriptorMigration),
		NoPrecondition,
		deleteDeprecatedNamespaceTableDescriptorMigration,
	),
	migration.NewTenantMigration(
		"fix all descriptors",
		toCV(clusterversion.FixDescriptors),
		NoPrecondition,
		fixDescriptorMigration,
	),
	migration.NewTenantMigration(
		"add the system.sql_statement_stats table",
		toCV(clusterversion.SQLStatsTable),
		NoPrecondition,
		sqlStatementStatsTableMigration,
	),
	migration.NewTenantMigration(
		"add the system.sql_transaction_stats table",
		toCV(clusterversion.SQLStatsTable),
		NoPrecondition,
		sqlTransactionStatsTableMigration,
	),
	migration.NewTenantMigration(
		"add the system.database_role_settings table",
		toCV(clusterversion.DatabaseRoleSettings),
		NoPrecondition,
		databaseRoleSettingsTableMigration,
	),
	migration.NewTenantMigration(
		"add the systems.tenant_usage table",
		toCV(clusterversion.TenantUsageTable),
		NoPrecondition,
		tenantUsageTableMigration,
	),
	migration.NewTenantMigration(
		"add the system.sql_instances table",
		toCV(clusterversion.SQLInstancesTable),
		NoPrecondition,
		sqlInstancesTableMigration,
	),
	migration.NewSystemMigration(
		"move over all intents to separate lock table",
		toCV(clusterversion.SeparatedIntentsMigration),
		separatedIntentsMigration),
	migration.NewSystemMigration(
		"run no-op migrate command on all ranges after lock table migration",
		toCV(clusterversion.PostSeparatedIntentsMigration),
		postSeparatedIntentsMigration),
	migration.NewTenantMigration(
		"add last_run and num_runs columns to system.jobs",
		toCV(clusterversion.RetryJobsWithExponentialBackoff),
		NoPrecondition,
		retryJobsWithExponentialBackoff),
	migration.NewTenantMigration(
		"validates no interleaved tables exist",
		toCV(clusterversion.EnsureNoInterleavedTables),
		interleavedTablesRemovedMigration,
		interleavedTablesRemovedMigration,
	),
}

func init() {
	for _, m := range migrations {
		registry[m.ClusterVersion()] = m
	}
}

func toCV(key clusterversion.Key) clusterversion.ClusterVersion {
	return clusterversion.ClusterVersion{
		Version: clusterversion.ByKey(key),
	}
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
// ignoring the fields in the descriptor that do not
//
// If multiple changes are done in the same query, e.g., if multiple columns are
// added, the function should check all changes to exist or absent, returning
// an error if changes exist partially.
func migrateTable(
	ctx context.Context,
	_ clusterversion.ClusterVersion,
	d migration.TenantDeps,
	opTag string,
	tableID descpb.ID,
	schemaChangeQuery string,
	schemaExists func(descriptor catalog.TableDescriptor) (bool, error),
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

		log.Infof(ctx, "performing table migration operation %v", opTag)
		// Retrieve the table.
		jt, err := readTableDescriptor(ctx, d, tableID)
		if err != nil {
			return err
		}
		// Wait for any in-flight schema changes to complete.
		if mutations := jt.GetMutationJobs(); len(mutations) > 0 {
			for _, mutation := range mutations {
				log.Infof(ctx, "waiting for the mutation job %v to complete", mutation.JobID)
				if d.TestingKnobs.BeforeWaitInRetryJobsWithExponentialBackoffMigration != nil {
					d.TestingKnobs.BeforeWaitInRetryJobsWithExponentialBackoffMigration(jobspb.JobID(mutation.JobID))
				}
				if _, err := d.InternalExecutor.Exec(ctx, "migration-mutations-wait",
					nil, "SHOW JOB WHEN COMPLETE $1", mutation.JobID); err != nil {
					return err
				}
			}
			continue
		}

		// Ignore the schema change if the table already has the required schema.
		if ok, err := schemaExists(jt); err != nil {
			return errors.Wrapf(err, "error while validating descriptors during"+
				" operation %s", opTag)
		} else if ok {
			log.Infof(ctx, "skipping %s operation as the schema change already exists.", opTag)
			if d.TestingKnobs != nil && d.TestingKnobs.SkippedMutation != nil {
				d.TestingKnobs.SkippedMutation()
			}
			return nil
		}

		// Modify the table.
		log.Infof(ctx, "performing operation: %s", opTag)
		if _, err := d.InternalExecutor.ExecEx(
			ctx,
			fmt.Sprintf("migration-alter-table-%d", tableID),
			nil, /* txn */
			sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
			schemaChangeQuery); err != nil {
			return err
		}
		return nil
	}
}

func readTableDescriptor(
	ctx context.Context, d migration.TenantDeps, tableID descpb.ID,
) (catalog.TableDescriptor, error) {
	var jt catalog.TableDescriptor

	if err := d.CollectionFactory.Txn(ctx, d.InternalExecutor, d.DB, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) (err error) {
		jt, err = descriptors.GetImmutableTableByID(ctx, txn, tableID, tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				AvoidCached: true,
				Required:    true,
			},
		})
		return err
	}); err != nil {
		return nil, err
	}
	return jt, nil
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
	storedCopy.CreatedExplicitly = false
	expectedCopy.CreatedExplicitly = false
	storedCopy.StoreColumnIDs = []descpb.ColumnID{0, 0, 0}
	expectedCopy.StoreColumnIDs = []descpb.ColumnID{0, 0, 0}

	if err = ensureProtoMessagesAreEqual(&expectedCopy, &storedCopy); err != nil {
		return false, err
	}
	return true, nil
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
