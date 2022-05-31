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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// The migration is broken down into four steps.
// 1. Adding the "user_id" column as NULL.
// 	All newly created users will have an ID after step 1, this gives us a
//	cut off point on which users we need to backfill.
// 2. Manually backfill the id column.
// 3. Set the column to not null.
// We need to do this because we cannot add a column with a nextval call as a
// default expression.
// It results in: unimplemented: cannot evaluate scalar expressions
// containing sequence operations in this context.
// The column family is specified such that the bootstrapped table
// definition and the migration end up with the same table state.
const addUserIDColumn = `
ALTER TABLE system.users
ADD COLUMN IF NOT EXISTS "user_id" OID CREATE FAMILY "fam_4_user_id"
`

const updateUserIDColumnSetNotNull = `
ALTER TABLE system.users ALTER COLUMN "user_id" SET NOT NULL
`

const addUserIDIndex = `
CREATE UNIQUE INDEX users_user_id_idx ON system.users ("user_id" ASC)
`

func alterSystemUsersAddUserIDColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	op := operation{
		name:           "add-system-users-user-id-column",
		schemaList:     []string{"user_id"},
		query:          addUserIDColumn,
		schemaExistsFn: hasColumn,
	}
	return migrateTable(ctx, cs, d, op, keys.UsersTableID, systemschema.UsersTable)
}

func backfillSystemUsersIDColumnAndAddIndex(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
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

	// Try to be smart about batching, every query on NULL IDs is a full
	// table scan.
	// Let's query all the usernames upfront, and update per username.
	// Update 1000 usernames at a time.
	// Using usernames leverages the primary index.
	// This let's us do one full table scan whereas doing
	// UPDATE ... WHERE user_id IS NULL LIMIT 1000 requires
	// O(num users) table scans.
	// Run setting NULL values in a loop for batching.
	batchSize := 10000
	for {
		it, err := d.InternalExecutor.QueryIteratorEx(ctx, "get null user ids", nil, sessiondata.NodeUserSessionDataOverride,
			`SELECT username FROM system.users WHERE user_id IS NULL LIMIT $1`, batchSize)
		if err != nil {
			return err
		}
		usernames := make([]string, 0)
		for {
			ok, err := it.Next(ctx)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			usernames = append(usernames, string(tree.MustBeDString(it.Cur()[0])))
		}

		if len(usernames) == 0 {
			break
		}
		// Prealloc IDs in one call to kv.IncrementValRetryable (one
		// round trip).
		numIDs := int64(len(usernames))
		startID, err := descidgen.IncrementUniqueRoleID(ctx, d.DB, d.Codec, numIDs)
		if err != nil {
			return err
		}

		id := startID
		for _, username := range usernames {
			updateSequenceValues := `
			UPDATE system.users SET user_id = $1 WHERE
			username = $2`

			_, err = d.InternalExecutor.ExecEx(ctx, "update-role-ids", nil,
				sessiondata.NodeUserSessionDataOverride, updateSequenceValues, id, username)

			if err != nil {
				return err
			}

			id++
		}
	}

	for _, op := range []operation{
		{
			name:       "alter-system-users-user-id-column-not-null",
			schemaList: []string{"user_id"},
			query:      updateUserIDColumnSetNotNull,
			schemaExistsFn: func(storedTable, _ catalog.TableDescriptor, colName string) (bool, error) {
				storedCol, err := storedTable.FindColumnWithName(tree.Name(colName))
				if err != nil {
					if strings.Contains(err.Error(), "does not exist") {
						return false, nil
					}
					return false, err
				}

				return !storedCol.IsNullable(), nil
			},
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
