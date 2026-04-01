// Copyright 2026 The Cockroach Authors.
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

const dropStmtDiagCompletedIdxV2 = `DROP INDEX IF EXISTS system.statement_diagnostics_requests@completed_idx_v2`

const createStmtDiagCompletedIdxV2 = `CREATE INDEX IF NOT EXISTS completed_idx_v2
	ON system.statement_diagnostics_requests (completed, id)
	STORING (statement_fingerprint, min_execution_latency, expires_at,
		sampling_probability, plan_gist, anti_plan_gist, redacted, username,
		max_execution_latency)`

// stmtDiagnosticsAddMaxLatencyColumn adds the max_execution_latency column
// to the system.statement_diagnostics_requests table and recreates the
// completed_idx_v2 index to include max_execution_latency in its STORING
// clause.
func stmtDiagnosticsAddMaxLatencyColumn(
	ctx context.Context, cv clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Step 1: Add the new column.
	op := operation{
		name:           "add-stmt-diag-requests-max-execution-latency-column",
		schemaList:     []string{"max_execution_latency"},
		query:          `ALTER TABLE system.statement_diagnostics_requests ADD COLUMN IF NOT EXISTS max_execution_latency INTERVAL FAMILY "primary"`,
		schemaExistsFn: columnExists,
	}
	if err := migrateTable(ctx, cv, d, op,
		keys.StatementDiagnosticsRequestsTableID,
		systemschema.StatementDiagnosticsRequestsTable); err != nil {
		return err
	}

	// Step 2: Drop the old completed_idx_v2 (without max_execution_latency
	// in STORING).
	op = operation{
		name:           "drop-completed-idx-v2-from-stmt-diag-requests",
		schemaList:     []string{"completed_idx_v2"},
		query:          dropStmtDiagCompletedIdxV2,
		schemaExistsFn: doesNotHaveIndex,
	}
	if err := migrateTable(ctx, cv, d, op,
		keys.StatementDiagnosticsRequestsTableID,
		systemschema.StatementDiagnosticsRequestsTable); err != nil {
		return err
	}

	// Step 3: Create the new completed_idx_v2 with max_execution_latency in
	// the STORING clause.
	op = operation{
		name:           "create-completed-idx-v2-on-stmt-diag-requests",
		schemaList:     []string{"completed_idx_v2"},
		query:          createStmtDiagCompletedIdxV2,
		schemaExistsFn: hasIndex,
	}
	return migrateTable(ctx, cv, d, op,
		keys.StatementDiagnosticsRequestsTableID,
		systemschema.StatementDiagnosticsRequestsTable)
}
