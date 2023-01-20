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
VALUES (1, true, crdb_internal.json_to_pb('cockroach.sql.sqlbase.TenantInfo', '{"id":1,"dataState":"READY","name":"` + catconstants.SystemTenantName + `"}'))
`

func addTenantNameColumnAndSystemTenantEntry(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	tenantsTableID, err := getTenantsTableID(ctx, d)
	if err != nil || tenantsTableID == 0 {
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

func getTenantsTableID(ctx context.Context, d upgrade.TenantDeps) (descpb.ID, error) {
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
		return 0, err
	}
	if rows == nil {
		// No system.tenants table. Nothing to do.
		return 0, nil
	}
	tenantsTableID := descpb.ID(int64(*rows[0].(*tree.DInt)))

	return tenantsTableID, nil
}

const addTenantServiceModeColumn = `
ALTER TABLE system.public.tenants ADD COLUMN service_mode STRING
AS lower(crdb_internal.pb_to_json('cockroach.sql.sqlbase.TenantInfo', info)->>'serviceMode') VIRTUAL`

const addTenantServiceModeIndex = `
CREATE INDEX tenants_service_mode_idx ON system.public.tenants (service_mode ASC)
`

const updateSystemTenantEntry = `
UPDATE system.public.tenants SET info =
crdb_internal.json_to_pb('cockroach.sql.sqlbase.TenantInfo',
crdb_internal.pb_to_json('cockroach.sql.sqlbase.TenantInfo', info) ||
pg_catalog.json_build_object('serviceMode', 'SHARED'))
WHERE id = 1
`

func addTenantServiceModeColumnAndUpdateSystemTenantEntry(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	tenantsTableID, err := getTenantsTableID(ctx, d)
	if err != nil || tenantsTableID == 0 {
		return err
	}

	for _, op := range []operation{
		{
			name:           "add-tenant-service-mode-column",
			schemaList:     []string{"service_mode"},
			query:          addTenantServiceModeColumn,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "make-tenant-service-mode-idx",
			schemaList:     []string{"tenants_service_mode_idx"},
			query:          addTenantServiceModeIndex,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, tenantsTableID, systemschema.TenantsTable); err != nil {
			return err
		}
	}

	_, err = d.InternalExecutor.ExecEx(ctx, "update-system-entry", nil,
		sessiondata.NodeUserSessionDataOverride, updateSystemTenantEntry)
	return err
}
