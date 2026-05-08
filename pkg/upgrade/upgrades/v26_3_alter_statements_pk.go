// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const (
	alterStatementsPKQuery = `ALTER TABLE system.statements ` +
		`ALTER PRIMARY KEY USING COLUMNS (fingerprint_id)`

	// CASCADE is required because ALTER PRIMARY KEY does not drop the
	// pre-existing unique secondary index on fingerprint_id; that index is
	// still backed by a UNIQUE constraint after the PK swap.
	dropStatementsFingerprintIDKey = `DROP INDEX IF EXISTS ` +
		`system.statements@statements_fingerprint_id_key CASCADE`

	dropStatementsIDColumn = `ALTER TABLE system.statements ` +
		`DROP COLUMN IF EXISTS id`
)

// alterStatementsTablePK changes the primary key of system.statements from
// the legacy id column to fingerprint_id, then drops the now-redundant
// unique index on fingerprint_id and the id column itself. The id column
// was unused: the only writer (statementstore) keys on fingerprint_id via
// ON CONFLICT.
func alterStatementsTablePK(
	ctx context.Context, cv clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	tableID, err := lookupSystemStatementsTableID(ctx, d)
	if err != nil {
		return err
	}

	op := operation{
		name:           "alter-statements-pk-to-fingerprint-id",
		schemaList:     []string{"primary"},
		query:          alterStatementsPKQuery,
		schemaExistsFn: primaryKeyIsOnFingerprintID,
	}
	if err := migrateTable(ctx, cv, d, op, tableID, systemschema.StatementsTable); err != nil {
		return err
	}

	op = operation{
		name:           "drop-statements-fingerprint-id-key",
		schemaList:     []string{"statements_fingerprint_id_key"},
		query:          dropStatementsFingerprintIDKey,
		schemaExistsFn: doesNotHaveIndex,
	}
	if err := migrateTable(ctx, cv, d, op, tableID, systemschema.StatementsTable); err != nil {
		return err
	}

	op = operation{
		name:           "drop-statements-id-column",
		schemaList:     []string{"id"},
		query:          dropStatementsIDColumn,
		schemaExistsFn: doesNotHaveColumn,
	}
	return migrateTable(ctx, cv, d, op, tableID, systemschema.StatementsTable)
}

// lookupSystemStatementsTableID resolves the dynamically-assigned descriptor
// ID of system.statements.
func lookupSystemStatementsTableID(ctx context.Context, d upgrade.TenantDeps) (descpb.ID, error) {
	row, err := d.InternalExecutor.QueryRowEx(ctx,
		"get-system-statements-table-id", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`SELECT 'system.statements'::REGCLASS::OID`,
	)
	if err != nil {
		return 0, err
	}
	return descpb.ID(tree.MustBeDOid(row[0]).Oid), nil
}

// primaryKeyIsOnFingerprintID returns true if the stored table's primary
// key is exactly (fingerprint_id), meaning the PK swap already happened.
func primaryKeyIsOnFingerprintID(storedTable, _ catalog.TableDescriptor, _ string) (bool, error) {
	pk := storedTable.GetPrimaryIndex()
	if pk == nil {
		return false, nil
	}
	cols := pk.IndexDesc().KeyColumnNames
	return len(cols) == 1 && cols[0] == "fingerprint_id", nil
}
