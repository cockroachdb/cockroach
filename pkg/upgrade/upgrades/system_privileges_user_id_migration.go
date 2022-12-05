// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const addUserIDColumnToSystemPrivilegesStmt = `
ALTER TABLE system.privileges
ADD COLUMN IF NOT EXISTS user_id OID
FAMILY "primary"
`

const createUniqueIndexOnUserIDAndPathOnSystemPrivilegesStmt = `
CREATE UNIQUE INDEX IF NOT EXISTS privileges_user_id_path_key ON system.privileges (user_id ASC, path ASC)
`

func alterSystemPrivilegesAddUserIDColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	ie := d.DB.Executor()

	// Query the table ID for the system.privileges table since it is dynamically
	// assigned.
	idRow, err := ie.QueryRowEx(ctx, "get-table-id", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`SELECT 'system.privileges'::regclass::oid`,
	)
	if err != nil {
		return err
	}
	tableID := descpb.ID(tree.MustBeDOid(idRow[0]).Oid)

	for _, op := range []operation{
		{
			name:           "add-user-id-column-system-privileges",
			schemaList:     []string{"user_id"},
			query:          addUserIDColumnToSystemPrivilegesStmt,
			schemaExistsFn: columnExists,
		},
		{
			name:           "add-user-id-path-unique-index-system-privileges",
			schemaList:     []string{"privileges_user_id_path_key"},
			query:          createUniqueIndexOnUserIDAndPathOnSystemPrivilegesStmt,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, tableID, systemschema.SystemPrivilegeTable); err != nil {
			return err
		}
	}

	return nil
}

const backfillUserIDColumnSystemPrivilegesStmt = `
UPDATE system.privileges AS p
SET user_id = u.user_id
FROM system.users AS u
WHERE p.user_id is NULL AND p.username = u.username
LIMIT 1000
`

func backfillSystemPrivilegesUserIDColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	ie := d.DB.Executor()
	for {
		rowsAffected, err := ie.ExecEx(ctx, "backfill-user-id-column-system-privileges", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			backfillUserIDColumnSystemPrivilegesStmt,
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
