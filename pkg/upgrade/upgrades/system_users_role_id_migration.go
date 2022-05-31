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

	var upsertRootStmt = fmt.Sprintf(`
		       UPSERT INTO system.users (username, "hashedPassword", "isRole", "user_id") VALUES ('%s', '', false, %d)
		       `, username.RootUser, username.RootUserID)

	var upsertAdminStmt = fmt.Sprintf(`
		       UPSERT INTO system.users (username, "hashedPassword", "isRole", "user_id") VALUES ('%s', '', true,  %d)
		       `, username.AdminRole, username.AdminRoleID)

	const updateSequenceValues = `
					UPDATE system.users SET user_id = nextval(48:::OID) WHERE user_id IS NULL`

	_, err := d.InternalExecutor.ExecEx(ctx, "upsert-root-user-in-role-id-migration", nil,
		sessiondata.NodeUserSessionDataOverride, upsertRootStmt)
	if err != nil {
		return err
	}

	_, err = d.InternalExecutor.ExecEx(ctx, "upsert-admin-role-in-role-id-migration", nil,
		sessiondata.NodeUserSessionDataOverride, upsertAdminStmt)
	if err != nil {
		return err
	}
	_, err = d.InternalExecutor.ExecEx(ctx, "update user ids", nil,
		sessiondata.NodeUserSessionDataOverride, updateSequenceValues)
	if err != nil {
		return err
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
