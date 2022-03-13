// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowClusterSettingList(
	stmt *tree.ShowClusterSettingList,
) (tree.Statement, error) {
	isAdmin, err := d.catalog.HasAdminRole(d.ctx)
	if err != nil {
		return nil, err
	}
	hasModify, err := d.catalog.HasRoleOption(d.ctx, roleoption.MODIFYCLUSTERSETTING)
	if err != nil {
		return nil, err
	}
	hasView, err := d.catalog.HasRoleOption(d.ctx, roleoption.VIEWCLUSTERSETTING)
	if err != nil {
		return nil, err
	}
	if !hasModify && !hasView && !isAdmin {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"only users with either %s or %s privileges are allowed to SHOW CLUSTER SETTINGS",
			roleoption.MODIFYCLUSTERSETTING, roleoption.VIEWCLUSTERSETTING)
	}
	if stmt.All {
		return parse(
			`SELECT variable, value, type AS setting_type, public, description
       FROM   crdb_internal.cluster_settings`,
		)
	}
	return parse(
		`SELECT variable, value, type AS setting_type, description
     FROM   crdb_internal.cluster_settings
     WHERE  public IS TRUE`,
	)
}

func (d *delegator) delegateShowTenantClusterSettingList(
	stmt *tree.ShowTenantClusterSettingList,
) (tree.Statement, error) {
	// Viewing cluster settings for other tenants is a more
	// privileged operation than viewing local cluster settings. So we
	// shouldn't be allowing with just the role option
	// VIEWCLUSTERSETTINGS.
	//
	// TODO(knz): Using admin authz for now; we may want to introduce a
	// more specific role option later.
	if err := d.catalog.RequireAdminRole(d.ctx, "show a tenant cluster setting"); err != nil {
		return nil, err
	}

	if !d.evalCtx.Codec.ForSystemTenant() {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"SHOW CLUSTER SETTINGS FOR TENANT can only be called by system operators")
	}

	publicCol := `allsettings.public,`
	var publicFilter string
	if !stmt.All {
		publicCol = ``
		publicFilter = `WHERE public IS TRUE`
	}

	return parse(`
WITH
  tenant_id AS (SELECT (` + stmt.TenantID.String() + `) AS tenant_id),
  isvalid AS (
    SELECT
      IF(tenant_id=0,
         crdb_internal.force_error('22023', 'tenant ID must be non-zero'),
      IF(tenant_id=1,
         crdb_internal.force_error('22023', 'use SHOW CLUSTER SETTINGS to display settings for the system tenant'),
      IF(st.id IS NULL,
         crdb_internal.force_error('22023', 'no tenant found with ID '||tenant_id),
      0))) AS ok
    FROM      tenant_id
    LEFT JOIN system.tenants st ON id = tenant_id.tenant_id
  )
SELECT
  allsettings.variable || substr('', (SELECT ok FROM isvalid)) AS variable,
  crdb_internal.decode_cluster_setting(allsettings.variable,
     -- NB: careful not to coalesce with allsettings.value directly!
     -- This is the value fo the system tenant and is not relevant to other tenants.
     COALESCE(tenantspecific.value,
              overrideall.value,
              -- NB: we can't compute the actual value here, which is the entry in the tenant's settings table.
              -- See discussion on issue #77935.
              NULL)
  ) AS value,
  allsettings.type,
  ` + publicCol + `
  IF(tenantspecific.value IS NOT NULL, 'per-tenant-override',
    IF(overrideall.value IS NOT NULL, 'all-tenants-override',
     'no-override')) AS origin,
  allsettings.description
FROM
  (SELECT * FROM system.crdb_internal.cluster_settings ` + publicFilter + `) AS allsettings
  LEFT JOIN (SELECT * FROM system.tenant_settings t, tenant_id WHERE t.tenant_id = tenant_id.tenant_id) AS tenantspecific ON
                  allsettings.variable = tenantspecific.name
  LEFT JOIN system.tenant_settings AS overrideall ON
                  allsettings.variable = overrideall.name AND overrideall.tenant_id = 0
`)
}
