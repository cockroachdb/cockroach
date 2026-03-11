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

const addMaxExecLatencyToStmtDiagRequests = `
ALTER TABLE system.statement_diagnostics_requests
  ADD COLUMN IF NOT EXISTS "max_execution_latency" INTERVAL
  FAMILY "primary"
`

const addMaxExecLatencyToTxnDiagRequests = `
ALTER TABLE system.transaction_diagnostics_requests
  ADD COLUMN IF NOT EXISTS "max_execution_latency" INTERVAL
  FAMILY "primary"
`

func addMaxExecutionLatencyToStmtDiagMigration(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Add max_execution_latency column to statement_diagnostics_requests.
	op := operation{
		name:           "add-max-execution-latency-to-stmt-diag-requests",
		schemaList:     []string{"max_execution_latency"},
		query:          addMaxExecLatencyToStmtDiagRequests,
		schemaExistsFn: hasColumn,
	}
	if err := migrateTable(
		ctx, version, deps, op,
		keys.StatementDiagnosticsRequestsTableID,
		systemschema.StatementDiagnosticsRequestsTable,
	); err != nil {
		return err
	}

	// Add max_execution_latency column to transaction_diagnostics_requests.
	// This table uses a dynamic ID, so we need to look it up.
	idRow, err := deps.InternalExecutor.QueryRowEx(
		ctx, "get-txn-diag-table-id", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`SELECT 'system.transaction_diagnostics_requests'::REGCLASS::OID`,
	)
	if err != nil {
		return err
	}
	txnTableID := descpb.ID(tree.MustBeDOid(idRow[0]).Oid)

	op = operation{
		name:           "add-max-execution-latency-to-txn-diag-requests",
		schemaList:     []string{"max_execution_latency"},
		query:          addMaxExecLatencyToTxnDiagRequests,
		schemaExistsFn: hasColumn,
	}
	return migrateTable(
		ctx, version, deps, op,
		txnTableID,
		systemschema.TransactionDiagnosticsRequestsTable,
	)
}
