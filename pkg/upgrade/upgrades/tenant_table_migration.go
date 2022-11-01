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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const addTenantNameColumn = `
ALTER TABLE system.public.tenants ADD COLUMN name STRING
AS (crdb_internal.pb_to_json('cockroach.sql.sqlbase.TenantInfo', info)->>'name') VIRTUAL`

const addTenantNameIndex = `
CREATE UNIQUE INDEX tenants_name_idx ON system.public.tenants (name ASC)
`

const addSystemTenantEntry = `
UPSERT INTO system.public.tenants (id, active, info)
VALUES (1, true, crdb_internal.json_to_pb('cockroach.sql.sqlbase.TenantInfo', '{"id":1,"state":0,"name":"` + catconstants.SystemTenantName + `"}'))
`

func getTenantsTableID(
	ctx context.Context, d upgrade.TenantDeps,
) (hasTenantTable bool, tenantTableID descpb.ID, err error) {
	rows, err := d.InternalExecutor.QueryRowEx(ctx, "get-tenant-table-id", nil,
		sessiondata.NodeUserSessionDataOverride, `
SELECT n2.id
  FROM system.public.namespace n1,
       system.public.namespace n2
 WHERE n1.name = $1
   AND n1.id = n2."parentID"
   AND n2.name = $2`,
		catconstants.SystemDatabaseName,
		catconstants.TenantsTableName,
	)
	if err != nil {
		return false, 0, err
	}
	if rows == nil {
		// No system.tenants table. Nothing to do.
		return false, 0, nil
	}

	// Retrieve the tenant table ID from the query above.
	tenantsTableID := descpb.ID(int64(*rows[0].(*tree.DInt)))

	return true, tenantsTableID, nil
}

func addTenantNameColumnAndSystemTenantEntry(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	hasTenantsTable, tenantsTableID, err := getTenantsTableID(ctx, d)
	if err != nil || !hasTenantsTable {
		return err
	}

	for _, op := range []operation{
		{
			name:           "add-tenant-name-column",
			schemaList:     []string{"name"},
			query:          addTenantNameColumn,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "make-tenant-name-unique",
			schemaList:     []string{"tenants_name_idx"},
			query:          addTenantNameIndex,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, tenantsTableID, systemschema.TenantsTable); err != nil {
			return err
		}
	}

	_, err = d.InternalExecutor.ExecEx(ctx, "add-system-entry", nil,
		sessiondata.NodeUserSessionDataOverride, addSystemTenantEntry)
	return err
}

// addAppTenantEntrySQL creates a new tenant with the name given
// in placeholder $1. No new tenant is created if a tenant with
// the specified name already exists. The ID for the new tenant
// is the lowest unused tenant ID.
const addAppTenantEntrySQL = `
WITH tid AS (
  SELECT id+1 AS newid
    FROM (VALUES (1) UNION ALL SELECT id FROM system.tenants) AS u(id)
   WHERE NOT EXISTS (SELECT 1 FROM system.tenants t WHERE t.id=u.id+1)
     AND NOT EXISTS (SELECT 1 FROM system.tenants t WHERE t.name=$1)
   LIMIT 1
) SELECT "".crdb_internal.create_tenant(newid, $1) FROM tid
`

func addAppTenantEntry(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	hasTenantsTable, _, err := getTenantsTableID(ctx, d)
	if err != nil || !hasTenantsTable {
		return err
	}

	_, err = d.InternalExecutor.ExecEx(ctx, "add-app-entry", nil,
		sessiondata.NodeUserSessionDataOverride,
		addAppTenantEntrySQL, catconstants.AppTenantName)
	return err
}
