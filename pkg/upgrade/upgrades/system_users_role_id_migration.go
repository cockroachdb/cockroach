// Copyright 2021 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// The migration is broken down into four steps.
//  1. Adding the "user_id" column as NULL.
//     All newly created users will have an ID after step 1, this gives us a
//     cut off point on which users we need to backfill.
//  2. Manually backfill the id column.
//  3. Set the column to not null.
//
// We need to do this because we cannot add a column with a nextval call as a
// default expression.
// It results in: unimplemented: cannot evaluate scalar expressions
// containing sequence operations in this context.
// The column family is specified such that the bootstrapped table
// definition and the migration end up with the same table state.
const addUserIDColumn = `
ALTER TABLE system.users
ADD COLUMN IF NOT EXISTS "user_id" OID FAMILY "primary"
`

const updateUserIDColumnSetNotNull = `
ALTER TABLE system.users ALTER COLUMN "user_id" SET NOT NULL
`

const addUserIDIndex = `
CREATE UNIQUE INDEX users_user_id_idx ON system.users ("user_id" ASC)
`

func alterSystemUsersAddUserIDColumnWithIndex(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	for _, op := range []operation{
		{
			name:           "add-system-users-user-id-column",
			schemaList:     []string{"user_id"},
			query:          addUserIDColumn,
			schemaExistsFn: columnExists,
		},
		{
			name:           "alter-system-users-add-index",
			schemaList:     []string{"users_user_id_idx"},
			query:          addUserIDIndex,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.UsersTableID, systemschema.UsersTable); err != nil {
			return err
		}
	}

	return nil
}

func backfillSystemUsersIDColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	var upsertRootStmt = `
			       UPSERT INTO system.users (username, "isRole", "user_id") VALUES ($1, false, $2)
			       `

	var upsertAdminStmt = `
			       UPSERT INTO system.users (username, "isRole", "user_id") VALUES ($1, true, $2)
			       `

	_, err := d.InternalExecutor.ExecEx(ctx, "upsert-root-user-in-role-id-migration", nil,
		sessiondata.NodeUserSessionDataOverride, upsertRootStmt, username.RootUser, username.RootUserID)
	if err != nil {
		return err
	}

	_, err = d.InternalExecutor.ExecEx(ctx, "upsert-admin-role-in-role-id-migration", nil,
		sessiondata.NodeUserSessionDataOverride, upsertAdminStmt, username.AdminRole, username.AdminRoleID)
	if err != nil {
		return err
	}

	row, err := d.InternalExecutor.QueryRowEx(ctx, `get-num-null-user-ids`, nil, sessiondata.NodeUserSessionDataOverride,
		`SELECT count(1) FROM system.users WHERE user_id IS NULL`)
	if err != nil {
		return err
	}
	numUsersToUpdate := int(tree.MustBeDInt(row[0]))
	const batchSize = 10000
	for i := 0; i < numUsersToUpdate; i += batchSize {
		numIDs := batchSize
		if numUsersToUpdate-i < batchSize {
			numIDs = numUsersToUpdate - i
		}
		startID, err := descidgen.IncrementUniqueRoleID(ctx, d.DB.KV(), d.Codec, int64(numIDs))
		if err != nil {
			return err
		}

		ids := make([]int, numIDs)
		for j := range ids {
			ids[j] = int(startID) + j
		}

		updateUserIDs := fmt.Sprintf(`
UPDATE system.users
   SET user_id = id
  FROM (
        SELECT *
          FROM (
                SELECT row_number() OVER () AS rn, *
                  FROM system.users
                 WHERE user_id IS NULL
               ) AS t1
               JOIN (
                  SELECT row_number() OVER () AS rn,
                         unnest AS id
                    FROM ROWS FROM (unnest($1))
                ) AS t2 ON t1.rn = t2.rn
       ) AS temp
 WHERE system.users.username = temp.username
       AND system.users.user_id IS NULL
 LIMIT %d`, numIDs)

		_, err = d.InternalExecutor.ExecEx(ctx, "update-role-ids", nil,
			sessiondata.NodeUserSessionDataOverride, updateUserIDs, ids)
		if err != nil {
			return err
		}
	}
	return nil
}

func setUserIDNotNull(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	op := operation{
		name:           "alter-system-users-user-id-column-not-null",
		schemaList:     []string{"user_id"},
		query:          updateUserIDColumnSetNotNull,
		schemaExistsFn: columnExistsAndIsNotNull,
	}
	return migrateTable(ctx, cs, d, op, keys.UsersTableID, systemschema.UsersTable)
}
