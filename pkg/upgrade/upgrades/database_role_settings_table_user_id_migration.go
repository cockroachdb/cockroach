// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const addRoleIDColumnToDatabaseRoleSettingsTableStmt = `
ALTER TABLE system.database_role_settings
ADD COLUMN IF NOT EXISTS role_id OID
FAMILY "primary"
`

const createUniqueIndexOnDatabaseIDAndRoleIDOnDatabaseRoleSettingsTableStmt = `
CREATE UNIQUE INDEX IF NOT EXISTS database_role_settings_database_id_role_id_key
ON system.database_role_settings (database_id, role_id)
STORING (settings)
`

func alterDatabaseRoleSettingsTableAddRoleIDColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	for _, op := range []operation{
		{
			name:           "add-role-id-column-database-role-settings-table",
			schemaList:     []string{"role_id"},
			query:          addRoleIDColumnToDatabaseRoleSettingsTableStmt,
			schemaExistsFn: columnExists,
		},
		{
			name:           "add-database-id-role-id-unique-index-database-role-settings-table",
			schemaList:     []string{"database_role_settings_database_id_role_id_key"},
			query:          createUniqueIndexOnDatabaseIDAndRoleIDOnDatabaseRoleSettingsTableStmt,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.DatabaseRoleSettingsTableID, systemschema.DatabaseRoleSettingsTable); err != nil {
			return err
		}
	}

	return nil
}

var backfillRoleIDColumnDatabaseRoleSettingsTableStmt = fmt.Sprintf(`
UPDATE system.database_role_settings
SET role_id = (
	SELECT CASE role_name
		WHEN '%s' THEN %d
		ELSE (SELECT user_id FROM system.users WHERE username = role_name)
	END
)
WHERE role_id IS NULL
LIMIT 1000
`,
	username.EmptyRole, username.EmptyRoleID)

const setRoleIDColumnToNotNullDatabaseRoleSettingsTableStmt = `
ALTER TABLE system.database_role_settings
ALTER COLUMN role_id SET NOT NULL
`

func backfillDatabaseRoleSettingsTableRoleIDColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	ie := d.DB.Executor()
	for {
		rowsAffected, err := ie.ExecEx(ctx, "backfill-role-id-database-role-settings-table", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			backfillRoleIDColumnDatabaseRoleSettingsTableStmt,
		)
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			break
		}
	}

	// After we finish backfilling, we can set the role_id column to be NOT NULL
	// since any existing rows will now have non-NULL values in the role_id column
	// and any new rows inserted after the previous version (when the role_id column
	// was added) will have had their role_id value populated at insertion time.
	op := operation{
		name:           "set-role-id-not-null-database-role-settings-table",
		schemaList:     []string{"role_id"},
		query:          setRoleIDColumnToNotNullDatabaseRoleSettingsTableStmt,
		schemaExistsFn: columnExistsAndIsNotNull,
	}
	if err := migrateTable(ctx, cs, d, op, keys.DatabaseRoleSettingsTableID, systemschema.DatabaseRoleSettingsTable); err != nil {
		return err
	}

	return nil
}
