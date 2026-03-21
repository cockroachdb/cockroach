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

// stmtDiagnosticsAddMaxLatencyColumn adds the max_execution_latency column
// to the system.statement_diagnostics_requests table.
func stmtDiagnosticsAddMaxLatencyColumn(
	ctx context.Context, cv clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	op := operation{
		name:           "add-stmt-diag-requests-max-execution-latency-column",
		schemaList:     []string{"max_execution_latency"},
		query:          `ALTER TABLE system.statement_diagnostics_requests ADD COLUMN IF NOT EXISTS max_execution_latency INTERVAL FAMILY "primary"`,
		schemaExistsFn: columnExists,
	}

	return migrateTable(ctx, cv, d, op,
		keys.StatementDiagnosticsRequestsTableID,
		systemschema.StatementDiagnosticsRequestsTable)
}
