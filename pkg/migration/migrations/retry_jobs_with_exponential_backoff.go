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
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// retryJobsWithExponentialBackoff changes the schema of system.jobs table in
// two steps. It first adds two new columns and then an index.
func retryJobsWithExponentialBackoff(
	ctx context.Context, cs clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	const addColsQuery = `ALTER TABLE system.jobs
			ADD COLUMN IF NOT EXISTS num_runs INT8 FAMILY claim,
			ADD COLUMN IF NOT EXISTS last_run TIMESTAMP FAMILY claim;`

	const addIndexQuery = `
CREATE INDEX IF NOT EXISTS jobs_status_claim
		ON system.jobs (claim_session_id, created, status)
		STORING (num_runs, last_run, claim_instance_id)
		WHERE
			status
			IN (
					'running',
					'reverting',
					'pending',
					'pause-requested',
					'cancel-requested'
				);`

	// add new columns
	if err := runJobsTableMigration(ctx, cs, d, addColsQuery, hasBackoffCols); err != nil {
		return err
	}

	return runJobsTableMigration(ctx, cs, d, addIndexQuery, hasBackoffIndex)
}

func runJobsTableMigration(
	ctx context.Context,
	_ clusterversion.ClusterVersion,
	d migration.TenantDeps,
	schemaChangeQuery string,
	hasAlreadyChanged func(descriptor catalog.TableDescriptor) bool,
) error {
	for {
		// Fetch the jobs table
		// Check if mutation job exists for system.jobs
		// if exists
		// 		wait for job to finish, continue the loop
		// otherwise
		// check if migration is complete (check new cols and ix), return
		// run schema change
		// on success continue the loop
		var jobsTable catalog.TableDescriptor

		// Retrieve the jobs table.
		if err := descs.Txn(ctx, d.Settings, d.LeaseManager, d.InternalExecutor, d.DB, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) (err error) {
			jobsTable, err = descriptors.GetImmutableTableByID(ctx, txn, keys.JobsTableID, tree.ObjectLookupFlags{
				CommonLookupFlags: tree.CommonLookupFlags{
					AvoidCached: true,
					Required:    true,
				},
			})
			return err
		}); err != nil {
			return err
		}

		// Wait for any in-flight schema change in the jobs table to complete.
		if mutations := jobsTable.GetMutationJobs(); len(mutations) > 0 {
			for _, mutation := range mutations {
				if _, err := d.InternalExecutor.Exec(ctx, "migration-mutations-wait",
					nil, "SHOW JOB WHEN COMPLETE $1", mutation.JobID); err != nil {
					return err
				}
			}
			continue
		}

		// Ignore the schema change if the jobs table already has the required schema.
		if hasAlreadyChanged(jobsTable) {
			return nil
		}

		// Modify the jobs table.
		log.Info(ctx, "Adding last_run and num_runs columns to system.jobs and creating an index")
		if _, err := d.InternalExecutor.Exec(ctx, "migration-alter-jobs-table", nil, schemaChangeQuery); err != nil {
			return err
		}
		return nil
	}
}

func hasBackoffCols(jobsTable catalog.TableDescriptor) bool {
	const (
		numRunsId   = 10
		numRunsName = "num_runs"
		lastRunId   = 11
		lastRunName = "last_run"
	)

	col, err := jobsTable.FindColumnWithID(numRunsId)
	if err != nil {
		return false
	}

	if col.GetName() != numRunsName || !col.IsNullable() || !col.GetType().Equivalent(types.Int) {
		return false
	}

	col, err = jobsTable.FindColumnWithID(lastRunId)
	if err != nil {
		return false
	}

	if col.GetName() != lastRunName || !col.IsNullable() || !col.GetType().Equivalent(types.Timestamp) {
		return false
	}

	return true
}

func hasBackoffIndex(jobsTable catalog.TableDescriptor) bool {
	const (
		indexID       = 4
		indexName     = "jobs_run_stats_idx"
		numKeyCols    = 3
		numStoredCols = 3
	)
	keyCols := [...]int{8, 2, 3}
	storedCols := [...]int{9, 10, 11}

	idx, err := jobsTable.FindIndexWithID(indexID)
	if err != nil {
		return false
	}
	if idx.GetName() != indexName {
		return false
	}

	if idx.NumKeyColumns() != numKeyCols {
		return false
	}
	keyColIds := idx.CollectKeyColumnIDs()
	for id := range keyCols {
		if !keyColIds.Contains(descpb.ColumnID(id)) {
			return false
		}
	}

	if idx.NumPrimaryStoredColumns() != numStoredCols {
		return false
	}
	storeColIds := idx.CollectSecondaryStoredColumnIDs()
	for id := range storedCols {
		if !storeColIds.Contains(descpb.ColumnID(id)) {
			return false
		}
	}

	return true
}
