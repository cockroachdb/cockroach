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
// its index to the system.statement_diagnostics table. The request_id column
// links bundles to their originating diagnostic request, enabling multiple
// bundles per request.
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

	return nil
}
