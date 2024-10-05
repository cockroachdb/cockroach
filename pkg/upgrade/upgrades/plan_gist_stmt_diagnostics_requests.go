// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// Target schema changes in the system.statement_diagnostics_requests table,
// adding two columns and updating the secondary index to store those columns.
const (
	addPlanGistColsToStmtDiagReqs = `
ALTER TABLE system.statement_diagnostics_requests
  ADD COLUMN plan_gist STRING NULL FAMILY "primary",
  ADD COLUMN anti_plan_gist BOOL NULL FAMILY "primary"`

	createCompletedIdxV2 = `
CREATE INDEX completed_idx_v2 ON system.statement_diagnostics_requests (completed, ID)
  STORING (statement_fingerprint, min_execution_latency, expires_at, sampling_probability, plan_gist, anti_plan_gist)`

	dropCompletedIdx = `DROP INDEX IF EXISTS system.statement_diagnostics_requests@completed_idx`
)

// stmtDiagForPlanGistMigration changes the schema of the
// system.statement_diagnostics_requests table to support plan gist matching.
func stmtDiagForPlanGistMigration(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	for _, op := range []operation{
		{
			name:           "add-stmt-diag-reqs-plan-gist-column",
			schemaList:     []string{"plan_gist", "anti_plan_gist"},
			query:          addPlanGistColsToStmtDiagReqs,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-stmt-diag-reqs-v2-index",
			schemaList:     []string{"completed_idx_v2"},
			query:          createCompletedIdxV2,
			schemaExistsFn: hasIndex,
		},
		{
			name:       "drop-stmt-diag-reqs-old-index",
			schemaList: []string{"completed_idx"},
			query:      dropCompletedIdx,
			schemaExistsFn: func(catalog.TableDescriptor, catalog.TableDescriptor, string) (bool, error) {
				return false, nil
			},
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.StatementDiagnosticsRequestsTableID,
			systemschema.StatementDiagnosticsRequestsTable); err != nil {
			return err
		}
	}
	return bumpSystemDatabaseSchemaVersion(ctx, cs, d)
}
