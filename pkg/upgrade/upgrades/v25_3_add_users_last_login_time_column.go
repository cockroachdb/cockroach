// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const (
	addUsersLastLoginTimeColumn = `
ALTER TABLE system.users
	ADD COLUMN IF NOT EXISTS estimated_last_login_time TIMESTAMPTZ
  CREATE IF NOT EXISTS FAMILY "fam_5_estimated_last_login_time"
`
)

func usersLastLoginTimeTableMigration(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	op := operation{
		name:           "add-last-login-time-column-to-users",
		schemaList:     []string{"estimated_last_login_time"},
		query:          addUsersLastLoginTimeColumn,
		schemaExistsFn: hasColumn,
	}
	if err := migrateTable(ctx, version, deps, op, keys.UsersTableID,
		systemschema.UsersTable); err != nil {
		return err
	}
	return nil
}
