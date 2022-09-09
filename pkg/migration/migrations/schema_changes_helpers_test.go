// Copyright 2022 The Cockroach Authors.
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
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

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
	m migration.TenantMigrationFunc,
	expectedTableDescriptor *atomic.Value,
) {
	expectedTableDescriptor = &atomic.Value{}
	return func(
		ctx context.Context, cs clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
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
