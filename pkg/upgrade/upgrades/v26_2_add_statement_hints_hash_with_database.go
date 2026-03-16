// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const addStatementHintsHashWithDatabaseColumn = `
ALTER TABLE system.statement_hints
  ADD COLUMN IF NOT EXISTS "hash_with_database" INT8 NOT VISIBLE
  AS (fnv64(fingerprint, database)) STORED FAMILY "primary"
`

const addStatementHintsHashWithDatabaseIndex = `
CREATE INDEX IF NOT EXISTS hash_with_database_idx
  ON system.statement_hints (hash_with_database ASC)
`

func statementHintsHashWithDatabaseMigration(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Query the table ID for system.statement_hints since it is dynamically
	// assigned.
	idRow, err := deps.InternalExecutor.QueryRowEx(ctx, "get-table-id", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`SELECT 'system.statement_hints'::REGCLASS::OID`,
	)
	if err != nil {
		return err
	}
	tableID := descpb.ID(tree.MustBeDOid(idRow[0]).Oid)

	// Add the hash_with_database computed column.
	op := operation{
		name:           "add-hash-with-database-column-to-statement-hints",
		schemaList:     []string{"hash_with_database"},
		query:          addStatementHintsHashWithDatabaseColumn,
		schemaExistsFn: columnExists,
	}
	if err := migrateTable(ctx, version, deps, op, tableID,
		systemschema.StatementHintsTable); err != nil {
		return err
	}

	// Add the hash_with_database_idx index.
	op = operation{
		name:           "add-hash-with-database-index-to-statement-hints",
		schemaList:     []string{"hash_with_database_idx"},
		query:          addStatementHintsHashWithDatabaseIndex,
		schemaExistsFn: indexExists,
	}
	return migrateTable(ctx, version, deps, op, tableID,
		systemschema.StatementHintsTable)
}
