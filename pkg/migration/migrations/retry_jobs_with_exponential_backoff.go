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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
)

// Target schema changes in the system.jobs table, adding two columns and an
// index for job retries with exponential backoff functionality.
const (
	addColsQuery = `
ALTER TABLE system.jobs
  ADD COLUMN num_runs INT8 FAMILY claim, 
  ADD COLUMN last_run TIMESTAMP FAMILY claim`
	addIndexQuery = `
CREATE INDEX jobs_run_stats_idx
		ON system.jobs (claim_session_id, status, created)
		STORING (last_run, num_runs, claim_instance_id)
		WHERE ` + systemschema.JobsRunStatsIdxPredicate
)

// retryJobsWithExponentialBackoff changes the schema of system.jobs table in
// two steps. It first adds two new columns and then an index.
func retryJobsWithExponentialBackoff(
	ctx context.Context, cs clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	for _, op := range []operation{
		{
			name:           "jobs-add-columns",
			schemaList:     []string{"num_runs", "last_run"},
			query:          addColsQuery,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "jobs-add-index",
			schemaList:     []string{"jobs_run_stats_idx"},
			query:          addIndexQuery,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.JobsTableID, systemschema.JobsTable); err != nil {
			return err
		}
	}
	return nil
}
