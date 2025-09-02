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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const (
	addTransactionDiagnosticsIDColumn = `
ALTER TABLE system.statement_diagnostics
	ADD COLUMN IF NOT EXISTS transaction_diagnostics_id INT8 FAMILY "primary"
`

	addTransactionDiagnosticsIDIndex = `
CREATE INDEX IF NOT EXISTS transaction_diagnostics_id_idx ON system.statement_diagnostics (transaction_diagnostics_id)
	`
)

// createTransactionDiagnosticsTables creates the transaction_diagnostics_requests and
// transaction_diagnostics system tables, and adds a transaction_diagnostics_id column
// to the statement_diagnostics table.
func createTransactionDiagnosticsTables(
	ctx context.Context, cv clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Create the transaction_diagnostics_requests table
	if err := createSystemTable(ctx, d.DB, d.Settings, d.Codec, systemschema.TransactionDiagnosticsRequestsTable, tree.LocalityLevelTable); err != nil {
		return err
	}

	// Create the transaction_diagnostics table
	if err := createSystemTable(ctx, d.DB, d.Settings, d.Codec, systemschema.TransactionDiagnosticsTable, tree.LocalityLevelTable); err != nil {
		return err
	}

	// Add transaction_diagnostics_id column to statement_diagnostics table
	for _, op := range []operation{
		{
			name:           "add-transaction-diagnostics-id-column-to-statement-diagnostics",
			schemaList:     []string{"transaction_diagnostics_id"},
			query:          addTransactionDiagnosticsIDColumn,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "add-transaction-diagnostics-id-index-to-statement-diagnostics",
			schemaList:     []string{"transaction_diagnostics_id_idx"},
			query:          addTransactionDiagnosticsIDIndex,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cv, d, op, keys.StatementDiagnosticsTableID, systemschema.StatementDiagnosticsTable); err != nil {
			return err
		}
	}
	return nil
}
