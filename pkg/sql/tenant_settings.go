// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

// alterTenantSetClusterSettingNode represents an
// ALTER VIRTUAL CLUSTER ... SET CLUSTER SETTING statement.
type alterTenantSetClusterSettingNode struct {
	zeroInputPlanNode
	name       settings.SettingName
	tenantSpec tenantSpec
	st         *cluster.Settings
	setting    settings.NonMaskedSetting
	// If value is nil, the setting should be reset.
	value tree.TypedExpr
}

// AlterTenantSetClusterSetting sets tenant level session variables.
// Privileges: MANAGEVIRTUALCLUSTER.
func (p *planner) AlterTenantSetClusterSetting(
	ctx context.Context, n *tree.AlterTenantSetClusterSetting,
) (planNode, error) {
	// Changing cluster settings for other tenants is a more
	// privileged operation than changing local cluster settings. So we
	// shouldn't be allowing with just the role option
	// MODIFYCLUSTERSETTINGS.
	if err := CanManageTenant(ctx, p); err != nil {
		return nil, err
	}
	// Error out if we're trying to call this from a non-system tenant.
	if !p.execCfg.Codec.ForSystemTenant() {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"ALTER VIRTUAL CLUSTER can only be called by system operators")
	}

	name := settings.SettingName(strings.ToLower(n.Name))
	// Disallow setting the 'version' setting for any tenant.
	if name == clusterversion.KeyVersionSetting {
		return nil, errors.Errorf("cannot set '%s' for tenants", name)
	}

	st := p.EvalContext().Settings
	setting, ok, nameStatus := settings.LookupForLocalAccess(name, true /* forSystemTenant - checked above already */)
	if !ok {
		return nil, errors.Errorf("unknown cluster setting '%s'", name)
	}
	if nameStatus != settings.NameActive {
		p.BufferClientNotice(ctx, settingAlternateNameNotice(name, setting.Name()))
		name = setting.Name()
	}
	// Error out if we're trying to set a system-only variable.
	if setting.Class() == settings.SystemOnly {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s is a system-only setting and must be set in the admin tenant using SET CLUSTER SETTING", name)
	}

	tspec, err := p.planTenantSpec(ctx, n.TenantSpec, "ALTER VIRTUAL CLUSTER SET CLUSTER SETTING "+string(name))
	if err != nil {
		return nil, err
	}

	value, err := p.getAndValidateTypedClusterSetting(ctx, name, n.Value, setting)
	if err != nil {
		return nil, err
	}

	node := alterTenantSetClusterSettingNode{
		name:       name,
		tenantSpec: tspec,
		st:         st,
		setting:    setting, value: value,
	}
	return &node, nil
}

func (n *alterTenantSetClusterSettingNode) startExec(params runParams) error {
	var tenantID uint64
	if _, ok := n.tenantSpec.(tenantSpecAll); ok {
		// Processing for TENANT ALL.
		// We will be writing rows with tenant_id = 0 in
		// system.tenant_settings.
		tenantID = 0
	} else {
		// Case for TENANT <tenant_id>. We'll check that the provided
		// tenant ID is non zero and refers to a tenant that exists in
		// system.tenants.
		rec, err := n.tenantSpec.getTenantInfo(params.ctx, params.p)
		if err != nil {
			return err
		}
		tenantID = rec.ID
		if roachpb.MustMakeTenantID(tenantID).IsSystem() {
			return errors.WithHint(pgerror.Newf(pgcode.InvalidParameterValue,
				"cannot use this statement to access cluster settings in system tenant"),
				"Use a regular SET CLUSTER SETTING statement.")
		}
	}

	// Write the setting.
	var reportedValue string
	if n.value == nil {
		// TODO(radu,knz): DEFAULT might be confusing, we really want to say "NO OVERRIDE"
		reportedValue = "DEFAULT"
		if _, err := params.p.InternalSQLTxn().ExecEx(
			params.ctx, "reset-tenant-setting", params.p.Txn(),
			sessiondata.NodeUserSessionDataOverride,
			"DELETE FROM system.tenant_settings WHERE tenant_id = $1 AND name = $2", tenantID, n.setting.InternalKey(),
		); err != nil {
			return err
		}
	} else {
		reportedValue = tree.AsStringWithFlags(n.value, tree.FmtBareStrings)
		value, err := eval.Expr(params.ctx, params.p.EvalContext(), n.value)
		if err != nil {
			return err
		}
		encoded, err := toSettingString(params.ctx, n.st, n.setting, value)
		if err != nil {
			return err
		}
		if _, err := params.p.InternalSQLTxn().ExecEx(
			params.ctx, "update-tenant-setting", params.p.Txn(),
			sessiondata.NodeUserSessionDataOverride,
			`UPSERT INTO system.tenant_settings (tenant_id, name, value, last_updated, value_type) VALUES ($1, $2, $3, now(), $4)`,
			tenantID, n.setting.InternalKey(), encoded, n.setting.Typ(),
		); err != nil {
			return err
		}
	}

	// Finally, log the event.
	return params.p.logEvent(
		params.ctx,
		0, /* no target */
		&eventpb.SetTenantClusterSetting{
			SettingName: string(n.name),
			Value:       reportedValue,
			TenantId:    tenantID,
			AllTenants:  tenantID == 0,
		})
}

func (n *alterTenantSetClusterSettingNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *alterTenantSetClusterSettingNode) Values() tree.Datums            { return nil }
func (n *alterTenantSetClusterSettingNode) Close(_ context.Context)        {}

// ShowTenantClusterSetting shows the value of a cluster setting for a tenant.
// Privileges: MANAGEVIRTUALCLUSTER
func (p *planner) ShowTenantClusterSetting(
	ctx context.Context, n *tree.ShowTenantClusterSetting,
) (planNode, error) {
	// Viewing cluster settings for other tenants is a more privileged operation
	// than viewing local cluster settings. So we shouldn't be allowing with just
	// the role option VIEWCLUSTERSETTING. We use MANAGEVIRTUALCLUSTER instead.
	// That privilege implicitly gives the ability to view non-redacted values for
	// sensitive cluster settings.
	if err := CanManageTenant(ctx, p); err != nil {
		return nil, err
	}

	name := settings.SettingName(strings.ToLower(n.Name))
	// NB: We use LookupForLocalAccess since the displayed value is computed in
	// the crdb_internal.decode_cluster_setting builtin function.
	setting, ok, nameStatus := settings.LookupForLocalAccess(name, p.ExecCfg().Codec.ForSystemTenant())
	if !ok {
		return nil, errors.Errorf("unknown setting: %q", name)
	}
	if nameStatus != settings.NameActive {
		p.BufferClientNotice(ctx, settingAlternateNameNotice(name, setting.Name()))
		name = setting.Name()
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

	tspec, err := p.planTenantSpec(ctx, n.TenantSpec, "SHOW CLUSTER SETTINGS FOR TENANT")
	if err != nil {
		return nil, err
	}

	return planShowClusterSetting(
		setting, name, columns,
		func(ctx context.Context, p *planner) (bool, string, error) {
			rec, err := tspec.getTenantInfo(ctx, p)
			if err != nil {
				return false, "", err
			}
			if roachpb.MustMakeTenantID(rec.ID).IsSystem() {
				return false, "", errors.WithHint(pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot use this statement to access cluster settings in system tenant"),
					"Use a regular SHOW CLUSTER SETTING statement.")
			}

			lookupEncodedTenantSetting := `
WITH
  tenantspecific AS (
     SELECT t.name, t.value
     FROM system.tenant_settings t
     WHERE t.tenant_id = $2
  ),
  setting AS (
   SELECT $1 AS variable
  )
SELECT crdb_internal.decode_cluster_setting(setting.variable,
     COALESCE(tenantspecific.value,
              overrideall.value,
              -- NB: we can't compute the actual value here, which is the entry in the tenant's settings table.
              -- See discussion on issue #77935.
              NULL)
  ) AS value
FROM
  setting
  LEFT JOIN tenantspecific
         ON setting.variable = tenantspecific.name
  LEFT JOIN system.tenant_settings AS overrideall
         ON setting.variable = overrideall.name AND overrideall.tenant_id = 0`

			datums, err := p.InternalSQLTxn().QueryRowEx(
				ctx, "get-tenant-setting-value", p.txn,
				sessiondata.NoSessionDataOverride,
				lookupEncodedTenantSetting,
				setting.InternalKey(), rec.ID)
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
