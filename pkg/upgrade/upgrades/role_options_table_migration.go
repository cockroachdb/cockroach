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

const addUserIDColumnToRoleOptions = `
ALTER TABLE system.role_options
ADD COLUMN IF NOT EXISTS "user_id" OID FAMILY "primary"
`

const addIndexToRoleOptionsUserIDColumn = `
CREATE INDEX users_user_id_idx ON system.role_options ("user_id" ASC)
`

const updateUserIDColumnRoleOptionsSetNotNull = `
ALTER TABLE system.role_options ALTER COLUMN "user_id" SET NOT NULL
`

func alterSystemRoleOptionsAddUserIDColumnWithIndex(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	for _, op := range []operation{
		{
			name:           "add-system-role-options-user-id-column",
			schemaList:     []string{"user_id"},
			query:          addUserIDColumnToRoleOptions,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "alter-system-role-options-add-index",
			schemaList:     []string{"users_user_id_idx"},
			query:          addIndexToRoleOptionsUserIDColumn,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.RoleOptionsTableID, systemschema.RoleOptionsTable); err != nil {
			return err
		}
	}

	return nil
}

func backfillSystemRoleOptionsIDColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	row, err := d.InternalExecutor.QueryRowEx(ctx, `get-num-null-user-ids`, nil, sessiondata.NodeUserSessionDataOverride,
		`SELECT count(1) FROM system.role_options WHERE user_id IS NULL`)
	if err != nil {
		return err
	}
	numUsersToUpdate := int(tree.MustBeDInt(row[0]))
	const batchSize = 10000
	for i := 0; i < numUsersToUpdate; i += batchSize {
		updateUserIDs := fmt.Sprintf(`
UPDATE system.role_options
   SET user_id = temp.user_id 
  FROM (
        SELECT *
          FROM (
                SELECT username, user_id
                  FROM system.users
               )
       ) AS temp
 WHERE system.role_options.username = temp.username
       AND system.role_options.user_id IS NULL
 LIMIT %d`, batchSize)

		_, err = d.InternalExecutor.ExecEx(ctx, "update-role-ids", nil,
			sessiondata.NodeUserSessionDataOverride, updateUserIDs)
		if err != nil {
			return err
		}
	}
	return nil
}

func setSystemRoleOptionsUserIDColumnNotNull(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	op := operation{
		name:       "alter-system-role-options-user-id-column-not-null",
		schemaList: []string{"user_id"},
		query:      updateUserIDColumnRoleOptionsSetNotNull,
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
	return migrateTable(ctx, cs, d, op, keys.RoleOptionsTableID, systemschema.RoleOptionsTable)
}
