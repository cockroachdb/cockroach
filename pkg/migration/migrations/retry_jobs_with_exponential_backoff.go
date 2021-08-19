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
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/errors"
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
	ctx context.Context, cs clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	jobsTable := systemschema.JobsTable
	ops := [...]struct {
		// Operation name.
		name string
		// List of schema names, e.g., column names, which are modified in the query.
		schemaList []string
		// Schema change query.
		query string
		// Function to check existing schema.
		schemaExistsFn func(catalog.TableDescriptor, catalog.TableDescriptor, string) (bool, error)
	}{
		{"jobs-add-columns", []string{"num_runs", "last_run"}, addColsQuery, hasColumn},
		{"jobs-add-index", []string{"jobs_run_stats_idx"}, addIndexQuery, hasIndex},
	}
	for _, op := range ops {
		if err := migrateTable(ctx, cs, d, op.name, keys.JobsTableID, op.query,
			func(storedTable catalog.TableDescriptor) (bool, error) {
				// Expect all or none.
				var exists bool
				for i, schemaName := range op.schemaList {
					hasSchema, err := op.schemaExistsFn(storedTable, jobsTable, schemaName)
					if err != nil {
						return false, err
					}
					if i > 0 && exists != hasSchema {
						return false, errors.Errorf("observed partial schema exists while performing %v", op.name)
					}
					exists = hasSchema
				}
				return exists, nil
			}); err != nil {
			return err
		}
	}
	return nil
}
