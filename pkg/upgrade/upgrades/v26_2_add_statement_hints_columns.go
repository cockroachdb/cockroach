// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/hintpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const (
	addHintTypeColumnTemplate = `
ALTER TABLE system.statement_hints
  ADD COLUMN IF NOT EXISTS "hint_type" STRING
  NOT NULL
  DEFAULT '%s'
  FAMILY "primary"
`
	addHintNameColumn = `
ALTER TABLE system.statement_hints
  ADD COLUMN IF NOT EXISTS "hint_name" STRING
  FAMILY "primary"
`
	addHintEnabledColumn = `
ALTER TABLE system.statement_hints
  ADD COLUMN IF NOT EXISTS "enabled" BOOL
  NOT NULL
  DEFAULT true
  FAMILY "primary"
`
	addHintDatabaseColumn = `
ALTER TABLE system.statement_hints
  ADD COLUMN IF NOT EXISTS "database" STRING
  FAMILY "primary"
`
)

func statementHintsAddColumnsTableMigration(
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

	for _, op := range []operation{
		{
			name:           "add-hint-type-column-to-statement-hints",
			schemaList:     []string{"hint_type"},
			query:          fmt.Sprintf(addHintTypeColumnTemplate, hintpb.HintTypeRewriteInlineHints),
			schemaExistsFn: hasColumn,
		},
		{
			name:           "add-hint-name-column-to-statement-hints",
			schemaList:     []string{"hint_name"},
			query:          addHintNameColumn,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "add-enabled-column-to-statement-hints",
			schemaList:     []string{"enabled"},
			query:          addHintEnabledColumn,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "add-database-column-to-statement-hints",
			schemaList:     []string{"database"},
			query:          addHintDatabaseColumn,
			schemaExistsFn: hasColumn,
		},
	} {
		if err := migrateTable(ctx, version, deps, op, tableID,
			systemschema.StatementHintsTable); err != nil {
			return err
		}
	}
	return nil
}

func statementHintsRemoveTypeDefaultTableMigration(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Execute the ALTER COLUMN DROP DEFAULT statement. This is idempotent.
	_, err := deps.InternalExecutor.ExecEx(
		ctx, "remove-hint-type-default-from-statement-hints", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`ALTER TABLE system.statement_hints ALTER COLUMN "hint_type" DROP DEFAULT`,
	)
	return err
}
