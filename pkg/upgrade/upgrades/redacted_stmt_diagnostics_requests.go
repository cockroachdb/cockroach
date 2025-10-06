// Copyright 2024 The Cockroach Authors.
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
// adding one column and updating the secondary index to store that column.
const (
	addRedactedColToStmtDiagReqs = `
ALTER TABLE system.statement_diagnostics_requests
  ADD COLUMN redacted BOOL NOT NULL DEFAULT false FAMILY "primary"`

	createCompletedIdx = `
CREATE INDEX completed_idx ON system.statement_diagnostics_requests (completed, ID)
  STORING (statement_fingerprint, min_execution_latency, expires_at, sampling_probability, plan_gist, anti_plan_gist, redacted)`

	// Note that only the DROP INDEX stmt must be idempotent since we don't have
	// the corresponding `schemaExistsFn`.
	dropCompletedIdxV2 = `DROP INDEX IF EXISTS system.statement_diagnostics_requests@completed_idx_v2`
)

// stmtDiagRedactedMigration changes the schema of the
// system.statement_diagnostics_requests table to support requesting redacted
// bundles.
func stmtDiagRedactedMigration(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	for _, op := range []operation{
		{
			name:           "add-stmt-diag-reqs-redacted-column",
			schemaList:     []string{"redacted"},
			query:          addRedactedColToStmtDiagReqs,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-stmt-diag-reqs-index",
			schemaList:     []string{"completed_idx"},
			query:          createCompletedIdx,
			schemaExistsFn: hasIndex,
		},
		{
			name:       "drop-stmt-diag-reqs-old-index",
			schemaList: []string{"completed_idx_v2"},
			query:      dropCompletedIdxV2,
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
	return nil
}
