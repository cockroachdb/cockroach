// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
)

func (d *delegator) delegateShowClusterSettingList(
	stmt *tree.ShowClusterSettingList,
) (tree.Statement, error) {

	// First check system privileges.
	hasModify := false
	hasSqlModify := false
	hasView := false
	cat := d.catalog
	globalPrivObj := syntheticprivilege.GlobalPrivilegeObject
	user := cat.GetCurrentUser()
	if err := cat.CheckPrivilege(d.ctx, globalPrivObj, user, privilege.MODIFYCLUSTERSETTING); err == nil {
		hasModify = true
		hasSqlModify = true
		hasView = true
	} else if pgerror.GetPGCode(err) != pgcode.InsufficientPrivilege {
		return nil, err
	}
	if !hasSqlModify {
		if err := cat.CheckPrivilege(d.ctx, globalPrivObj, user, privilege.MODIFYSQLCLUSTERSETTING); err == nil {
			hasSqlModify = true
			hasView = true
		} else if pgerror.GetPGCode(err) != pgcode.InsufficientPrivilege {
			return nil, err
		}
	}
	if !hasView {
		if err := cat.CheckPrivilege(d.ctx, globalPrivObj, user, privilege.VIEWCLUSTERSETTING); err == nil {
			hasView = true
		} else if pgerror.GetPGCode(err) != pgcode.InsufficientPrivilege {
			return nil, err
		}
	}

	// Fallback to role option if the user doesn't have the privilege.
	if !hasModify {
		ok, err := cat.HasRoleOption(d.ctx, roleoption.MODIFYCLUSTERSETTING)
		if err != nil {
			return nil, err
		}
		hasModify = hasModify || ok
		hasView = hasView || ok
	}

	if !hasView {
		ok, err := cat.HasRoleOption(d.ctx, roleoption.VIEWCLUSTERSETTING)
		if err != nil {
			return nil, err
		}
		hasView = hasView || ok
	}

	// If user is not admin and has neither privilege, return an error.
	if !hasView && !hasModify && !hasSqlModify {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"only users with %s, %s or %s privileges are allowed to SHOW CLUSTER SETTINGS",
			privilege.MODIFYCLUSTERSETTING, privilege.MODIFYSQLCLUSTERSETTING, privilege.VIEWCLUSTERSETTING)
	}

	if stmt.All {
		return d.parse(
			`SELECT variable, value, type AS setting_type, public, description, default_value, origin
       FROM   crdb_internal.cluster_settings`,
		)
	}
	return d.parse(
		`SELECT variable, value, type AS setting_type, description, default_value, origin
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
	if err := d.catalog.CheckPrivilege(d.ctx, syntheticprivilege.GlobalPrivilegeObject, d.catalog.GetCurrentUser(), privilege.VIEWCLUSTERMETADATA); err != nil {
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

	// Note: we do the validation in SQL (via CASE...END) because the
	// TenantID expression may be complex (incl subqueries, etc) and we
	// cannot evaluate it in the go code.
	return d.parse(`
WITH
  tenant_id AS (SELECT id AS tenant_id FROM [SHOW TENANT ` + stmt.TenantSpec.String() + `]),
  isvalid AS (
    SELECT
      CASE
       WHEN tenant_id=1 THEN
         crdb_internal.force_error('22023', 'use SHOW CLUSTER SETTINGS to display settings for the system tenant')
       ELSE 0
      END AS ok
    FROM      tenant_id
    LEFT JOIN system.tenants st ON id = tenant_id.tenant_id
  ),
  tenantspecific AS (
     SELECT t.name AS setting_key, t.value
     FROM system.tenant_settings t, tenant_id
     WHERE t.tenant_id = tenant_id.tenant_id
  ),
  allsettings AS (
    SELECT variable AS setting_name, value, public, type, description, key AS setting_key
    FROM system.crdb_internal.cluster_settings ` + publicFilter + `
  )
SELECT
  allsettings.setting_name || substr('', (SELECT ok FROM isvalid)) AS variable,
  crdb_internal.decode_cluster_setting(allsettings.setting_name,
     -- NB: careful not to coalesce with allsettings.value directly!
     -- This is the value for the system tenant and is not relevant to other tenants.
     COALESCE(tenantspecific.value,
              overrideall.value,
              -- NB: we can't compute the actual value here, which is the entry in the tenant's settings table.
              -- See discussion on issue #77935.
              NULL)
  ) AS value,
  allsettings.type,
  ` + publicCol + `
  CASE
    WHEN tenantspecific.value IS NOT NULL THEN 'per-tenant-override'
    WHEN overrideall.value IS NOT NULL THEN 'all-tenants-override'
    ELSE 'no-override'
  END AS origin,
  allsettings.description
FROM
  allsettings
  LEFT JOIN tenantspecific ON
                  allsettings.setting_key = tenantspecific.setting_key
  LEFT JOIN system.tenant_settings AS overrideall ON
                  allsettings.setting_key = overrideall.name AND overrideall.tenant_id = 0
`)
}
