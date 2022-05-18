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

	const upsertRootStmt = `
		       UPSERT INTO system.users (username, "hashedPassword", "isRole", "user_id") VALUES ('root', '', false, 1)
		       `

	const upsertAdminStmt = `
		       UPSERT INTO system.users (username, "hashedPassword", "isRole", "user_id") VALUES ('admin', '', true,  2)
		       `

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

	op := operation{
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
	}

	return migrateTable(ctx, cs, d, op, keys.UsersTableID, systemschema.UsersTable)
}
