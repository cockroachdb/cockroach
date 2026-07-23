// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const dropStmtDiagCompletedIdxV2 = `DROP INDEX IF EXISTS system.statement_diagnostics_requests@completed_idx_v2`

const createStmtDiagCompletedIdxV2 = `CREATE INDEX IF NOT EXISTS completed_idx_v2
	ON system.statement_diagnostics_requests (completed, id)
	STORING (statement_fingerprint, min_execution_latency, expires_at,
		sampling_probability, plan_gist, anti_plan_gist, redacted, username,
		max_execution_latency)`

const dropTxnDiagCompletedIdx = `DROP INDEX IF EXISTS system.transaction_diagnostics_requests@completed_idx`

const createTxnDiagCompletedIdx = `CREATE INDEX IF NOT EXISTS completed_idx
	ON system.transaction_diagnostics_requests (completed, id)
	STORING (transaction_fingerprint_id, statement_fingerprint_ids,
		min_execution_latency, expires_at, sampling_probability, redacted, username,
		max_execution_latency)`

// diagnosticsAddMaxLatencyColumn adds the max_execution_latency column to both
// system.statement_diagnostics_requests and
// system.transaction_diagnostics_requests, and recreates their completed
// indexes to include max_execution_latency in their STORING clauses.
func diagnosticsAddMaxLatencyColumn(
	ctx context.Context, cv clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Migrate statement_diagnostics_requests (hardcoded table ID).
	if err := migrateStmtDiagRequests(ctx, cv, d); err != nil {
		return err
	}
	// Migrate transaction_diagnostics_requests (dynamic table ID).
	return migrateTxnDiagRequests(ctx, cv, d)
}

func migrateStmtDiagRequests(
	ctx context.Context, cv clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
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

func migrateTxnDiagRequests(
	ctx context.Context, cv clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Resolve the dynamic table ID for transaction_diagnostics_requests.
	idRow, err := d.InternalExecutor.QueryRowEx(ctx,
		"get-txn-diag-requests-table-id", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`SELECT 'system.transaction_diagnostics_requests'::REGCLASS::OID`,
	)
	if err != nil {
		return err
	}
	tableID := descpb.ID(tree.MustBeDOid(idRow[0]).Oid)

	op := operation{
		name:           "add-txn-diag-requests-max-execution-latency-column",
		schemaList:     []string{"max_execution_latency"},
		query:          `ALTER TABLE system.transaction_diagnostics_requests ADD COLUMN IF NOT EXISTS max_execution_latency INTERVAL FAMILY "primary"`,
		schemaExistsFn: columnExists,
	}
	if err := migrateTable(ctx, cv, d, op,
		tableID, systemschema.TransactionDiagnosticsRequestsTable); err != nil {
		return err
	}

	op = operation{
		name:           "drop-completed-idx-from-txn-diag-requests",
		schemaList:     []string{"completed_idx"},
		query:          dropTxnDiagCompletedIdx,
		schemaExistsFn: doesNotHaveIndex,
	}
	if err := migrateTable(ctx, cv, d, op,
		tableID, systemschema.TransactionDiagnosticsRequestsTable); err != nil {
		return err
	}

	op = operation{
		name:           "create-completed-idx-on-txn-diag-requests",
		schemaList:     []string{"completed_idx"},
		query:          createTxnDiagCompletedIdx,
		schemaExistsFn: hasIndex,
	}
	return migrateTable(ctx, cv, d, op,
		tableID, systemschema.TransactionDiagnosticsRequestsTable)
}
