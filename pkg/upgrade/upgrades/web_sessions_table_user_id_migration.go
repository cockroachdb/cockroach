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

const addUserIDColumnToWebSessionsTableStmt = `
ALTER TABLE system.web_sessions
ADD COLUMN IF NOT EXISTS user_id OID
FAMILY "fam_0_id_hashedSecret_username_createdAt_expiresAt_revokedAt_lastUsedAt_auditInfo"
`

func alterWebSessionsTableAddUserIDColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	for _, op := range []operation{
		{
			name:           "add-user-id-column-web-sessions-table",
			schemaList:     []string{"user_id"},
			query:          addUserIDColumnToWebSessionsTableStmt,
			schemaExistsFn: columnExists,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.WebSessionsTableID, systemschema.WebSessionsTable); err != nil {
			return err
		}
	}

	return nil
}

const backfillUserIDColumnWebSessionsTableStmt = `
UPDATE system.web_sessions AS w
SET user_id = u.user_id
FROM system.users AS u
WHERE w.user_id IS NULL AND w.username = u.username
LIMIT 1000
`

const setUserIDColumnToNotNullWebSessionsTableStmt = `
ALTER TABLE system.web_sessions
ALTER COLUMN user_id SET NOT NULL
`

func backfillWebSessionsTableUserIDColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	ie := d.DB.Executor()
	for {
		rowsAffected, err := ie.ExecEx(ctx, "backfill-user-id-col-web-sessions-table", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			backfillUserIDColumnWebSessionsTableStmt,
		)
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			break
		}
	}

	// There may stil be rows with no user_id, since it's possible that the
	// user was dropped, but left behind rows in the web_sessions table.
	if _, err := ie.ExecEx(ctx, "delete-rows-for-dropped-users-web-sessions-table", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride, `
	DELETE FROM system.web_sessions
	WHERE user_id IS NULL`,
	); err != nil {
		return err
	}

	// After we finish backfilling, we can set the user_id column to be NOT NULL
	// since any existing rows will now have non-NULL values in the user_id column
	// and any new rows inserted after the previous version (when the user_id column
	// was added) will have had their user_id value populated at insertion time.
	op := operation{
		name:           "set-user-id-not-null-web-sessions-table",
		schemaList:     []string{"user_id"},
		query:          setUserIDColumnToNotNullWebSessionsTableStmt,
		schemaExistsFn: columnExistsAndIsNotNull,
	}
	if err := migrateTable(ctx, cs, d, op, keys.WebSessionsTableID, systemschema.WebSessionsTable); err != nil {
		return err
	}

	return nil
}
