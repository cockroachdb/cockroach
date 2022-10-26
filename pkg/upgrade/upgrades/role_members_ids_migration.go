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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const addIDColumnsToRoleMembersStmt = `
ALTER TABLE system.role_members
ADD COLUMN IF NOT EXISTS role_id OID,
ADD COLUMN IF NOT EXISTS member_id OID
`

func alterSystemRoleMembersAddIDColumns(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	op := operation{
		name:           "add-id-columns-system-role-members",
		schemaList:     []string{"role_id", "member_id"},
		query:          addIDColumnsToRoleMembersStmt,
		schemaExistsFn: columnExists,
	}
	return migrateTable(ctx, cs, d, op, keys.RoleMembersTableID, systemschema.RoleMembersTable)
}

const addIndexOnRoleIDToRoleMembersStmt = `
CREATE INDEX role_members_role_id_idx ON system.role_members (role_id ASC)
`

const addIndexOnMemberIDToRoleMembersStmt = `
CREATE INDEX role_members_member_id_idx ON system.role_members (member_id ASC)
`

const addUniqueIndexOnIDsToRoleMembersStmt = `
CREATE UNIQUE INDEX role_members_role_id_member_id_key ON system.role_members (role_id ASC, member_id ASC)
`

func alterSystemRoleMembersAddIndexesForIDColumns(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	for _, op := range []operation{
		{
			name:           "add-role-id-index-system-role-members",
			schemaList:     []string{"role_id"},
			query:          addIndexOnRoleIDToRoleMembersStmt,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "add-member-id-index-system-role-members",
			schemaList:     []string{"member_id"},
			query:          addIndexOnMemberIDToRoleMembersStmt,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "add-id-unique-index-system-role-members",
			schemaList:     []string{"role_id", "member_id"},
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

const backfillIDColumnsRoleMembersStmt = `
UPDATE system.role_members
SET (role_id, member_id) = (u1.user_id, u2.user_id)
FROM system.users AS u1, system.users AS u2
WHERE role_id IS NULL AND role = u1.username AND member = u2.username
LIMIT $1
`

func backfillSystemRoleMembersIDColumns(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	row, err := d.InternalExecutor.QueryRowEx(ctx, `get-num-null-role-ids-system-role-members`, nil,
		sessiondata.NodeUserSessionDataOverride,
		`SELECT count(1) FROM system.role_members WHERE role_id IS NULL`,
	)
	if err != nil {
		return err
	}
	numRowsToUpdate := int(tree.MustBeDInt(row[0]))
	const batchSize = 10000
	for i := 0; i < numRowsToUpdate; i += batchSize {
		_, err := d.InternalExecutor.ExecEx(ctx, "backfill-id-columns-system-role-members", nil,
			sessiondata.NodeUserSessionDataOverride,
			backfillIDColumnsRoleMembersStmt, batchSize,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

const alterIDColumnsNotNullRoleMembersStmt = `
ALTER TABLE system.role_members
ALTER COLUMN role_id SET NOT NULL,
ALTER COLUMN member_id SET NOT NULL
`

func alterSystemRoleMembersSetIDColumnsNotNull(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	op := operation{
		name:           "alter-id-columns-not-null-system-role-members",
		schemaList:     []string{"role_id", "member_id"},
		query:          alterIDColumnsNotNullRoleMembersStmt,
		schemaExistsFn: columnExistsAndIsNotNull,
	}
	return migrateTable(ctx, cs, d, op, keys.RoleMembersTableID, systemschema.RoleMembersTable)
}
