// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
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

const backfillRoleIDColumnDatabaseRoleSettingsTableStmt = `
UPDATE system.database_role_settings
SET role_id = user_id
FROM system.users
WHERE role_id IS NULL AND role_name = username
LIMIT 1000
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

	return nil
}
