// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// stmtDiagnosticsAddRequestIDColumnAndIndex adds the request_id column and
// its index to the system.statement_diagnostics table, and adds the
// collections_remaining column to system.statement_diagnostics_requests.
// The request_id column links bundles to their originating diagnostic request,
// enabling multiple bundles per request. The collections_remaining column
// bounds the number of bundles collected per continuous request.
func stmtDiagnosticsAddRequestIDColumnAndIndex(
	ctx context.Context, cv clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	for _, op := range []operation{
		{
			name:           "add-statement-diagnostics-request-id-column",
			schemaList:     []string{"request_id"},
			query:          `ALTER TABLE system.statement_diagnostics ADD COLUMN IF NOT EXISTS request_id INT8 FAMILY "primary"`,
			schemaExistsFn: columnExists,
		},
		{
			name:           "add-statement-diagnostics-request-id-index",
			schemaList:     []string{"request_id_idx"},
			query:          `CREATE INDEX IF NOT EXISTS request_id_idx ON system.statement_diagnostics (request_id)`,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cv, d, op,
			keys.StatementDiagnosticsTableID,
			systemschema.StatementDiagnosticsTable); err != nil {
			return err
		}
	}

	// Add collections_remaining column and update the completed_idx_v2 index
	// on statement_diagnostics_requests.
	for _, op := range []operation{
		{
			name:           "add-stmt-diag-requests-collections-remaining",
			schemaList:     []string{"collections_remaining"},
			query:          `ALTER TABLE system.statement_diagnostics_requests ADD COLUMN IF NOT EXISTS collections_remaining INT8 FAMILY "primary"`,
			schemaExistsFn: columnExists,
		},
		{
			// Drop the old completed_idx_v2 index so it can be recreated with
			// collections_remaining in the STORING clause. Uses hasMatchingIndex
			// to skip the drop if the index already matches the expected definition.
			name:           "drop-old-completed-idx-v2-on-stmt-diag-requests",
			schemaList:     []string{"completed_idx_v2"},
			query:          `DROP INDEX IF EXISTS system.statement_diagnostics_requests@completed_idx_v2`,
			schemaExistsFn: hasMatchingIndex,
		},
		{
			name:           "recreate-completed-idx-v2-with-collections-remaining",
			schemaList:     []string{"completed_idx_v2"},
			query:          `CREATE INDEX IF NOT EXISTS completed_idx_v2 ON system.statement_diagnostics_requests (completed, id) STORING (statement_fingerprint, min_execution_latency, expires_at, sampling_probability, plan_gist, anti_plan_gist, redacted, username, collections_remaining)`,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cv, d, op,
			keys.StatementDiagnosticsRequestsTableID,
			systemschema.StatementDiagnosticsRequestsTable); err != nil {
			return err
		}
	}

	return nil
}
