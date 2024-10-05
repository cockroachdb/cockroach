// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const addIDColumnsToRoleMembersStmt = `
ALTER TABLE system.role_members
ADD COLUMN IF NOT EXISTS role_id OID,
ADD COLUMN IF NOT EXISTS member_id OID
`

const addIndexOnRoleIDToRoleMembersStmt = `
CREATE INDEX IF NOT EXISTS role_members_role_id_idx ON system.role_members (role_id ASC)
`

const addIndexOnMemberIDToRoleMembersStmt = `
CREATE INDEX IF NOT EXISTS role_members_member_id_idx ON system.role_members (member_id ASC)
`

const addUniqueIndexOnIDsToRoleMembersStmt = `
CREATE UNIQUE INDEX IF NOT EXISTS role_members_role_id_member_id_key ON system.role_members (role_id ASC, member_id ASC)
`

func alterSystemRoleMembersAddIDColumns(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	for _, op := range []operation{
		{
			name:           "add-id-columns-system-role-members",
			schemaList:     []string{"role_id", "member_id"},
			query:          addIDColumnsToRoleMembersStmt,
			schemaExistsFn: columnExists,
		},
		{
			name:           "add-role-id-index-system-role-members",
			schemaList:     []string{"role_members_role_id_idx"},
			query:          addIndexOnRoleIDToRoleMembersStmt,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "add-member-id-index-system-role-members",
			schemaList:     []string{"role_members_member_id_idx"},
			query:          addIndexOnMemberIDToRoleMembersStmt,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "add-id-unique-index-system-role-members",
			schemaList:     []string{"role_members_role_id_member_id_key"},
			query:          addUniqueIndexOnIDsToRoleMembersStmt,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.RoleMembersTableID, systemschema.RoleMembersTable); err != nil {
			return err
		}
	}

	return nil
}

const backfillRoleIDColumnRoleMemberStmt = `
UPDATE system.role_members
SET role_id = u.user_id
FROM system.users AS u
WHERE role_id IS NULL AND role = u.username
LIMIT 1000
`

const backfillMemberIDColumnRoleMembersStmt = `
UPDATE system.role_members
SET member_id = u.user_id
FROM system.users AS u
WHERE member_id IS NULL AND member = u.username
LIMIT 1000
`

const setIDColumnsToNotNullRoleMembersStmt = `
ALTER TABLE system.role_members
ALTER COLUMN role_id SET NOT NULL,
ALTER COLUMN member_id SET NOT NULL
`

func backfillSystemRoleMembersIDColumns(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	ie := d.DB.Executor()
	for _, backfillStmt := range []string{backfillRoleIDColumnRoleMemberStmt, backfillMemberIDColumnRoleMembersStmt} {
		for {
			rowsAffected, err := ie.ExecEx(ctx, "backfill-id-columns-system-role-members", nil, /* txn */
				sessiondata.NodeUserSessionDataOverride,
				backfillStmt,
			)
			if err != nil {
				return err
			}
			if rowsAffected == 0 {
				break
			}
		}
	}

	// After we finish backfilling, we can set the ID columns to be NOT NULL
	// since any existing rows will now have non-NULL values in their ID columns
	// and any new rows inserted after the previous version (when the columns
	// were added) will also have their ID columns populated at insertion time.
	op := operation{
		name:           "set-id-columns-not-null-system-role-members",
		schemaList:     []string{"role_id", "member_id"},
		query:          setIDColumnsToNotNullRoleMembersStmt,
		schemaExistsFn: columnExistsAndIsNotNull,
	}
	if err := migrateTable(ctx, cs, d, op, keys.RoleMembersTableID, systemschema.RoleMembersTable); err != nil {
		return err
	}

	return nil
}
