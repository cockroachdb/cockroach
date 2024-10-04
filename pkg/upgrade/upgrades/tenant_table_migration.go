// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// Note: the pre-existing column "active" becomes deprecated with the
// introduction of the new column data_state, but we cannot remove it
// right away because previous versions still depend on it.
const addTenantColumns = `
ALTER TABLE system.public.tenants
  ALTER COLUMN active SET NOT VISIBLE,
  ADD COLUMN name STRING FAMILY "primary",
  ADD COLUMN data_state INT FAMILY "primary",
  ADD COLUMN service_mode INT FAMILY "primary"`

const addTenantIndex1 = `CREATE UNIQUE INDEX tenants_name_idx ON system.public.tenants (name ASC)`
const addTenantIndex2 = `CREATE INDEX tenants_service_mode_idx ON system.public.tenants (service_mode ASC)`

func extendTenantsTable(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	tenantsTableID, err := getTenantsTableID(ctx, d)
	if err != nil || tenantsTableID == 0 {
		return err
	}

	for _, op := range []operation{
		{
			name:           "add-tenant-columns",
			schemaList:     []string{"name", "data_state", "service_mode"},
			query:          addTenantColumns,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "make-tenant-name-unique",
			schemaList:     []string{"tenants_name_idx"},
			query:          addTenantIndex1,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "add-service-mode-idx",
			schemaList:     []string{"tenants_service_mode_idx"},
			query:          addTenantIndex2,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, tenantsTableID, systemschema.TenantsTable); err != nil {
			return err
		}
	}

	// Note: the following UPSERT is guaranteed to never encounter a
	// duplicate key error on the "name" column. The reason for this is
	// that the name column is only ever populated after the version
	// field has been updated to support tenant names, and this only
	// happens after this migration completes.
	_, err = d.InternalExecutor.ExecEx(ctx, "add-system-entry", nil,
		sessiondata.NodeUserSessionDataOverride,
		`UPSERT INTO system.public.tenants (id, active, info, name, data_state, service_mode)
VALUES (1, true,
	crdb_internal.json_to_pb('cockroach.multitenant.ProtoInfo', '{"deprecatedId":1,"deprecatedDataState":"READY"}'),
  '`+catconstants.SystemTenantName+`',
  `+strconv.Itoa(int(mtinfopb.DataStateReady))+`,
  `+strconv.Itoa(int(mtinfopb.ServiceModeShared))+`)`,
	)
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
