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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// The migration is broken down into four steps.
// 1. Adding the "user_id" column as NULL.
// 2. Update the column to use a default expression that uses a sequence.
// 3. Manually backfill the id column.
// 4. Set the column to not null.
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

const updateUserIDColumnDefaultExpr = `
ALTER TABLE system.users ALTER COLUMN "user_id" SET DEFAULT oid(nextval(48:::OID))
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
	for _, op := range []operation{
		{
			name:           "add-system-users-user-id-column",
			schemaList:     []string{"user_id"},
			query:          addUserIDColumn,
			schemaExistsFn: hasColumn,
		},
		{
			name:       "alter-system-users-user-id-default-expression",
			schemaList: []string{"user_id"},
			query:      updateUserIDColumnDefaultExpr,
			schemaExistsFn: func(storedTable, _ catalog.TableDescriptor, colName string) (bool, error) {
				storedCol, err := storedTable.FindColumnWithName(tree.Name(colName))
				if err != nil {
					if strings.Contains(err.Error(), "does not exist") {
						return false, nil
					}
					return false, err
				}

				return storedCol.HasDefault(), nil
			},
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.UsersTableID, systemschema.UsersTable); err != nil {
			return err
		}
	}

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
	for {
		it, err := d.InternalExecutor.QueryIteratorEx(ctx, "get null user ids", nil, sessiondata.NodeUserSessionDataOverride,
			`SELECT username FROM system.users WHERE user_id IS NULL`)
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
		batchSize := 100
		for i := 0; i < len(usernames); i += batchSize {
			// Update the usernames 100 at a time.
			end := i + batchSize
			if i+batchSize > len(usernames) {
				end = len(usernames)
			}

			updateSequenceValues := `
			UPDATE system.users SET user_id = oid(nextval('system.public.role_id_seq'::REGCLASS)) WHERE
			username = ANY $1`
			_, err = d.InternalExecutor.ExecEx(ctx, "update user ids", nil,
				sessiondata.NodeUserSessionDataOverride, updateSequenceValues, usernames[i:end])
			if err != nil {
				return err
			}
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
