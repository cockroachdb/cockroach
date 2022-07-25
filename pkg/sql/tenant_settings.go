// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

// alterTenantSetClusterSettingNode represents an
// ALTER TENANT ... SET CLUSTER SETTING statement.
type alterTenantSetClusterSettingNode struct {
	name     string
	tenantID tree.TypedExpr // tenantID or nil for "all tenants"
	st       *cluster.Settings
	setting  settings.NonMaskedSetting
	// If value is nil, the setting should be reset.
	value tree.TypedExpr
}

// AlterTenantSetClusterSetting sets tenant level session variables.
// Privileges: super user.
func (p *planner) AlterTenantSetClusterSetting(
	ctx context.Context, n *tree.AlterTenantSetClusterSetting,
) (planNode, error) {
	// Changing cluster settings for other tenants is a more
	// privileged operation than changing local cluster settings. So we
	// shouldn't be allowing with just the role option
	// MODIFYCLUSTERSETTINGS.
	//
	// TODO(knz): Using admin authz for now; we may want to introduce a
	// more specific role option later.
	if err := p.RequireAdminRole(ctx, "change a tenant cluster setting"); err != nil {
		return nil, err
	}
	// Error out if we're trying to call this from a non-system tenant.
	if !p.execCfg.Codec.ForSystemTenant() {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"ALTER TENANT can only be called by system operators")
	}

	name := strings.ToLower(n.Name)
	st := p.EvalContext().Settings
	v, ok := settings.Lookup(name, settings.LookupForLocalAccess, true /* forSystemTenant - checked above already */)
	if !ok {
		return nil, errors.Errorf("unknown cluster setting '%s'", name)
	}
	// Error out if we're trying to set a system-only variable.
	if v.Class() == settings.SystemOnly {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s is a system-only setting and must be set in the admin tenant using SET CLUSTER SETTING", name)
	}

	var typedTenantID tree.TypedExpr
	if !n.TenantAll {
		var dummyHelper tree.IndexedVarHelper
		var err error
		typedTenantID, err = p.analyzeExpr(
			ctx, n.TenantID, nil, dummyHelper, types.Int, true, "ALTER TENANT SET CLUSTER SETTING "+name)
		if err != nil {
			return nil, err
		}
	}

	setting, ok := v.(settings.NonMaskedSetting)
	if !ok {
		return nil, errors.AssertionFailedf("expected writable setting, got %T", v)
	}

	// We don't support changing the version for another tenant.
	// See discussion on issue https://github.com/cockroachdb/cockroach/issues/77733 (wontfix).
	if _, isVersion := setting.(*settings.VersionSetting); isVersion {
		return nil, errors.Newf("cannot change the version of another tenant")
	}

	value, err := p.getAndValidateTypedClusterSetting(ctx, name, n.Value, setting)
	if err != nil {
		return nil, err
	}

	node := alterTenantSetClusterSettingNode{
		name: name, tenantID: typedTenantID, st: st,
		setting: setting, value: value,
	}
	return &node, nil
}

func (n *alterTenantSetClusterSettingNode) startExec(params runParams) error {
	var tenantIDi uint64
	var tenantID tree.Datum
	if n.tenantID == nil {
		// Processing for TENANT ALL.
		// We will be writing rows with tenant_id = 0 in
		// system.tenant_settings.
		tenantID = tree.NewDInt(0)
	} else {
		// Case for TENANT <tenant_id>. We'll check that the provided
		// tenant ID is non zero and refers to a tenant that exists in
		// system.tenants.
		var err error
		tenantIDi, tenantID, err = resolveTenantID(params.p, n.tenantID)
		if err != nil {
			return err
		}
		if err := assertTenantExists(params.ctx, params.p, tenantID); err != nil {
			return err
		}
	}

	// Write the setting.
	var reportedValue string
	if n.value == nil {
		// TODO(radu,knz): DEFAULT might be confusing, we really want to say "NO OVERRIDE"
		reportedValue = "DEFAULT"
		if _, err := params.p.execCfg.InternalExecutor.ExecEx(
			params.ctx, "reset-tenant-setting", params.p.Txn(),
			sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			"DELETE FROM system.tenant_settings WHERE tenant_id = $1 AND name = $2", tenantID, n.name,
		); err != nil {
			return err
		}
	} else {
		reportedValue = tree.AsStringWithFlags(n.value, tree.FmtBareStrings)
		value, err := eval.Expr(params.p.EvalContext(), n.value)
		if err != nil {
			return err
		}
		encoded, err := toSettingString(params.ctx, n.st, n.name, n.setting, value)
		if err != nil {
			return err
		}
		if _, err := params.p.execCfg.InternalExecutor.ExecEx(
			params.ctx, "update-tenant-setting", params.p.Txn(),
			sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			`UPSERT INTO system.tenant_settings (tenant_id, name, value, last_updated, value_type) VALUES ($1, $2, $3, now(), $4)`,
			tenantID, n.name, encoded, n.setting.Typ(),
		); err != nil {
			return err
		}
	}

	// Finally, log the event.
	return params.p.logEvent(
		params.ctx,
		0, /* no target */
		&eventpb.SetTenantClusterSetting{
			SettingName: n.name,
			Value:       reportedValue,
			TenantId:    tenantIDi,
			AllTenants:  tenantIDi == 0,
		})
}

func (n *alterTenantSetClusterSettingNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *alterTenantSetClusterSettingNode) Values() tree.Datums            { return nil }
func (n *alterTenantSetClusterSettingNode) Close(_ context.Context)        {}

func resolveTenantID(p *planner, expr tree.TypedExpr) (uint64, tree.Datum, error) {
	tenantIDd, err := eval.Expr(p.EvalContext(), expr)
	if err != nil {
		return 0, nil, err
	}
	tenantID, ok := tenantIDd.(*tree.DInt)
	if !ok {
		return 0, nil, errors.AssertionFailedf("expected int, got %T", tenantIDd)
	}
	if *tenantID == 0 {
		return 0, nil, pgerror.Newf(pgcode.InvalidParameterValue, "tenant ID must be non-zero")
	}
	if roachpb.MakeTenantID(uint64(*tenantID)) == roachpb.SystemTenantID {
		return 0, nil, errors.WithHint(pgerror.Newf(pgcode.InvalidParameterValue,
			"cannot use this statement to access cluster settings in system tenant"),
			"Use a regular SHOW/SET CLUSTER SETTING statement.")
	}
	return uint64(*tenantID), tenantIDd, nil
}

func assertTenantExists(ctx context.Context, p *planner, tenantID tree.Datum) error {
	exists, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx, "get-tenant", p.txn,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		`SELECT EXISTS(SELECT id FROM system.tenants WHERE id = $1)`, tenantID)
	if err != nil {
		return err
	}
	if exists[0] != tree.DBoolTrue {
		return pgerror.Newf(pgcode.InvalidParameterValue, "no tenant found with ID %v", tenantID)
	}
	return nil
}

// ShowTenantClusterSetting shows the value of a cluster setting for a tenant.
// Privileges: super user.
func (p *planner) ShowTenantClusterSetting(
	ctx context.Context, n *tree.ShowTenantClusterSetting,
) (planNode, error) {
	// Viewing cluster settings for other tenants is a more
	// privileged operation than viewing local cluster settings. So we
	// shouldn't be allowing with just the role option
	// VIEWCLUSTERSETTINGS.
	//
	// TODO(knz): Using admin authz for now; we may want to introduce a
	// more specific role option later.
	if err := p.RequireAdminRole(ctx, "view a tenant cluster setting"); err != nil {
		return nil, err
	}

	name := strings.ToLower(n.Name)
	val, ok := settings.Lookup(
		name, settings.LookupForLocalAccess, p.ExecCfg().Codec.ForSystemTenant(),
	)
	if !ok {
		return nil, errors.Errorf("unknown setting: %q", name)
	}
	setting, ok := val.(settings.NonMaskedSetting)
	if !ok {
		// If we arrive here, this means Lookup() did not properly
		// ignore the masked setting, which is a bug in Lookup().
		return nil, errors.AssertionFailedf("setting is masked: %v", name)
	}

	// Error out if we're trying to call this from a non-system tenant or if
	// we're trying to set a system-only variable.
	if !p.execCfg.Codec.ForSystemTenant() {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"SHOW CLUSTER SETTING FOR TENANT can only be called by system operators")
	}

	columns, err := getShowClusterSettingPlanColumns(setting, name)
	if err != nil {
		return nil, err
	}

	return planShowClusterSetting(
		setting, name, columns,
		func(ctx context.Context, p *planner) (bool, string, error) {
			// NB: we evaluate + check the tenant ID inside SQL using
			// the same pattern as used for SHOW CLUSTER SETTINGS FOR TENANT.
			lookupEncodedTenantSetting := `
WITH
  tenant_id AS (SELECT (` + n.TenantID.String() + `):::INT AS tenant_id),
  isvalid AS (
    SELECT
      CASE
       WHEN tenant_id=0 THEN
         crdb_internal.force_error('22023', 'tenant ID must be non-zero')
       WHEN tenant_id=1 THEN
         crdb_internal.force_error('22023', 'use SHOW CLUSTER SETTING to display a setting for the system tenant')
       WHEN st.id IS NULL THEN
         crdb_internal.force_error('22023', 'no tenant found with ID '||tenant_id)
       ELSE 0
      END AS ok
    FROM      tenant_id
    LEFT JOIN system.tenants st ON id = tenant_id.tenant_id
  ),
  tenantspecific AS (
     SELECT t.name, t.value
     FROM system.tenant_settings t, tenant_id
     WHERE t.tenant_id = tenant_id.tenant_id
  ),
  setting AS (
   SELECT $1 AS variable
  )
SELECT COALESCE(
   tenantspecific.value,
   overrideall.value,
   -- NB: we can't compute the actual value here, see discussion on issue #77935.
   NULL
   )
FROM
  setting
  LEFT JOIN tenantspecific
         ON setting.variable = tenantspecific.name
  LEFT JOIN system.tenant_settings AS overrideall
         ON setting.variable = overrideall.name AND overrideall.tenant_id = 0`

			datums, err := p.ExecCfg().InternalExecutor.QueryRowEx(
				ctx, "get-tenant-setting-value", p.txn,
				sessiondata.InternalExecutorOverride{User: username.RootUserName()},
				lookupEncodedTenantSetting,
				name)
			if err != nil {
				return false, "", err
			}
			if len(datums) != 1 {
				return false, "", errors.AssertionFailedf("expected 1 column, got %+v", datums)
			}
			if datums[0] == tree.DNull {
				return false, "", nil
			}
			encoded, ok := tree.AsDString(datums[0])
			if !ok {
				return false, "", errors.AssertionFailedf("expected string value, got %T", datums[0])
			}
			return true, string(encoded), nil
		})
}
