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
	"fmt"
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

const addRoleIDColumnToRoleMembers = `
ALTER TABLE system.role_members ADD COLUMN IF NOT EXISTS "role_id" OID
`

const addMemberIDColumnToRoleMembers = `
ALTER TABLE system.role_members ADD COLUMN IF NOT EXISTS "member_id" OID
`

const addIndexToRoleMembersRoleIDColumn = `
CREATE INDEX role_members_role_id_idx ON system.role_members ("role_id" ASC)
`

const addIndexToRoleMembersMemberIDColumn = `
CREATE INDEX role_members_member_id_idx ON system.role_members ("member_id" ASC)
`

const addUniqueIndexToRoleMembers = `
CREATE UNIQUE INDEX role_members_role_member_role_id_member_id_key ON system.role_members ("role", "member", "role_id", "member_id")
`

const updateRoleIDColumnRoleMembersSetNotNull = `
ALTER TABLE system.role_members ALTER COLUMN "role_id" SET NOT NULL
`

const updateMemberIDColumnRoleMembersSetNotNull = `
ALTER TABLE system.role_members ALTER COLUMN "member_id" SET NOT NULL
`

func alterSystemRoleMembersAddColumnsAndIndexes(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	for _, op := range []operation{
		{
			name:           "add-system-role-members-role-id-column",
			schemaList:     []string{"role_id"},
			query:          addRoleIDColumnToRoleMembers,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "add-system-role-members-member-id-column",
			schemaList:     []string{"member_id"},
			query:          addMemberIDColumnToRoleMembers,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "alter-system-role-members-add-index-on-role-id",
			schemaList:     []string{"role_members_role_id_idx"},
			query:          addIndexToRoleMembersRoleIDColumn,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "alter-system-role-members-add-index-on-member-id",
			schemaList:     []string{"role_members_member_id_idx"},
			query:          addIndexToRoleMembersMemberIDColumn,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "alter-system-role-members-add-index",
			schemaList:     []string{"role_members_role_member_role_id_member_id_key"},
			query:          addUniqueIndexToRoleMembers,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.RoleMembersTableID, systemschema.RoleMembersTable); err != nil {
			return err
		}
	}

	return nil
}

func backfillSystemRoleMembersIDColumns(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	row, err := d.InternalExecutor.QueryRowEx(ctx, `get-num-null-user-ids`, nil, sessiondata.NodeUserSessionDataOverride,
		`SELECT count(1) FROM system.role_members WHERE role_id IS NULL OR member_id IS NULL`)
	if err != nil {
		return err
	}
	numUsersToUpdate := int(tree.MustBeDInt(row[0]))
	const batchSize = 10000
	for i := 0; i < numUsersToUpdate; i += batchSize {
		updateUserIDs := fmt.Sprintf(`
UPDATE
    system.role_members
SET
    role_id = temp.role_id, member_id = temp.member_id
FROM
    (
        SELECT
            *
        FROM
            (
                SELECT
                    role,
                    member,
                    u1.user_id AS role_id,
                    u2.user_id AS member_id
                FROM
                    system.role_members AS rm
                    JOIN system.users AS u1 ON
                            rm.role = u1.username
                    JOIN system.users AS u2 ON
                            rm.member = u2.username
            )
    )
        AS temp
WHERE
    system.role_members.role = temp.role
    AND system.role_members.member = temp.member
    AND system.role_members.role_id IS NULL
LIMIT %d;
`, batchSize)

		_, err = d.InternalExecutor.ExecEx(ctx, "update-role-ids", nil,
			sessiondata.NodeUserSessionDataOverride, updateUserIDs)
		if err != nil {
			return err
		}
	}
	return nil
}

func setSystemRoleMembersIDColumnsNotNull(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	for _, op := range []operation{
		{
			name:       "alter-system-role-members-role-id-column-not-null",
			schemaList: []string{"role_id"},
			query:      updateRoleIDColumnRoleMembersSetNotNull,
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
			name:       "alter-system-role-members-member-id-column-not-null",
			schemaList: []string{"member_id"},
			query:      updateMemberIDColumnRoleMembersSetNotNull,
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
	} {
		if err := migrateTable(ctx, cs, d, op, keys.RoleMembersTableID, systemschema.RoleMembersTable); err != nil {
			return err
		}
	}
	return nil
}
