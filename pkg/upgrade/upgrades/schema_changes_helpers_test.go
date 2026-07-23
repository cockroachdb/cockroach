// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

type SchemaChangeTestMigrationFunc func() (m upgrade.TenantUpgradeFunc, expectedTableDescriptor *atomic.Value)

const (
	// TestingAddColsQuery is used by TestMigrationWithFailures.
	TestingAddColsQuery = `
ALTER TABLE test.test_table
  ADD COLUMN num_runs INT8 FAMILY claim, 
  ADD COLUMN last_run TIMESTAMP FAMILY claim`

	// TestingAddIndexQuery is used by TestMigrationWithFailures.
	TestingAddIndexQuery = `
CREATE INDEX jobs_run_stats_idx
		ON test.test_table (claim_session_id, status, created)
		STORING (last_run, num_runs, claim_instance_id)
		WHERE ` + systemschema.JobsRunStatsIdxPredicate
)

// MakeFakeMigrationForTestMigrationWithFailures makes the migration function
// used in the
func MakeFakeMigrationForTestMigrationWithFailures() (
	m upgrade.TenantUpgradeFunc,
	expectedTableDescriptor *atomic.Value,
) {
	expectedTableDescriptor = &atomic.Value{}
	return func(
		ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
	) error {
		row, err := d.InternalExecutor.QueryRow(ctx, "look-up-id", nil, /* txn */
			`select id from system.namespace where name = $1`, "test_table")
		if err != nil {
			return err
		}
		tableID := descpb.ID(tree.MustBeDInt(row[0]))
		for _, op := range []operation{
			{
				name:           "jobs-add-columns",
				schemaList:     []string{"num_runs", "last_run"},
				query:          TestingAddColsQuery,
				schemaExistsFn: hasColumn,
			},
			{
				name:           "jobs-add-index",
				schemaList:     []string{"jobs_run_stats_idx"},
				query:          TestingAddIndexQuery,
				schemaExistsFn: hasIndex,
			},
		} {
			expected := expectedTableDescriptor.Load().(catalog.TableDescriptor)
			if err := migrateTable(ctx, cs, d, op, tableID, expected); err != nil {
				return err
			}
		}
		return nil
	}, expectedTableDescriptor
}

var _ SchemaChangeTestMigrationFunc = MakeFakeMigrationForTestMigrationWithFailures

const (
	// TestingAddNewColStmt is used by TestMigrationWithFailuresMultipleAltersOnSameColumn.
	TestingAddNewColStmt = `
ALTER TABLE test.test_table
ADD COLUMN user_id OID
`

	// TestingAlterNewColStmt is used by TestMigrationWithFailuresMultipleAltersOnSameColumn.
	TestingAlterNewColStmt = `
ALTER TABLE test.test_table
ALTER COLUMN user_id SET NOT NULL
`
)

// MakeFakeMigrationForTestMigrationWithFailuresMultipleAltersOnSameColumn makes the
// migration function used in TestMigrationWithFailuresMultipleAltersOnSameColumn.
func MakeFakeMigrationForTestMigrationWithFailuresMultipleAltersOnSameColumn() (
	m upgrade.TenantUpgradeFunc,
	expectedTableDescriptor *atomic.Value,
) {
	expectedTableDescriptor = &atomic.Value{}
	return func(
		ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
	) error {
		row, err := d.InternalExecutor.QueryRow(ctx, "look-up-id", nil, /* txn */
			`select id from system.namespace where name = $1`, "test_table")
		if err != nil {
			return err
		}
		tableID := descpb.ID(tree.MustBeDInt(row[0]))
		for _, op := range []operation{
			{
				name:           "add-user-id-column",
				schemaList:     []string{"user_id"},
				query:          TestingAddNewColStmt,
				schemaExistsFn: columnExists,
			},
			{
				name:           "alter-user-id-column",
				schemaList:     []string{"user_id"},
				query:          TestingAlterNewColStmt,
				schemaExistsFn: columnExistsAndIsNotNull,
			},
		} {
			expected := expectedTableDescriptor.Load().(catalog.TableDescriptor)
			if err := migrateTable(ctx, cs, d, op, tableID, expected); err != nil {
				return err
			}
		}
		return nil
	}, expectedTableDescriptor
}

var _ SchemaChangeTestMigrationFunc = MakeFakeMigrationForTestMigrationWithFailuresMultipleAltersOnSameColumn
