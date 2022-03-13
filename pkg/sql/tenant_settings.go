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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
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

	name := strings.ToLower(n.Name)

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

	st := p.EvalContext().Settings
	v, ok := settings.Lookup(name, settings.LookupForLocalAccess, p.ExecCfg().Codec.ForSystemTenant())
	if !ok {
		return nil, errors.Errorf("unknown cluster setting '%s'", name)
	}

	setting, ok := v.(settings.NonMaskedSetting)
	if !ok {
		return nil, errors.AssertionFailedf("expected writable setting, got %T", v)
	}

	// We don't support changing the version for another tenant just yet.
	if _, isVersion := setting.(*settings.VersionSetting); isVersion {
		return nil, unimplemented.NewWithIssue(77733, "cannot change the version of another tenant")
	}

	// Error out if we're trying to call this from a non-system tenant or if
	// we're trying to set a system-only variable.
	if !p.execCfg.Codec.ForSystemTenant() {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"ALTER TENANT can only be called by system operators")
	}
	if setting.Class() == settings.SystemOnly {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s is a system-only setting and must be set in the admin tenant using SET CLUSTER SETTING", name)
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
		tenantIDi, tenantID, err = resolveTenantID(params, n.tenantID)
		if err != nil {
			return err
		}
		if err := assertTenantExists(params, tenantID); err != nil {
			return err
		}
	}

	// Write the setting.
	_, err := writeSettingInternal(
		params.ctx,
		&tenantSettingWriter{currentTxn: params.p.Txn(), tenantID: tenantID},
		params.p.ExecCfg(),
		n.setting, n.name,
		params.p.User(),
		n.st,
		n.value,
		params.p.EvalContext(),
		false, /* forSystemTenant */
		func(ctx context.Context, settingName, reportedValue string) error {
			return params.p.logEvent(ctx,
				0, /* no target */
				&eventpb.SetTenantClusterSetting{
					SettingName: settingName,
					Value:       reportedValue,
					TenantId:    tenantIDi,
					AllTenants:  tenantIDi == 0,
				})
		},
	)
	return err
}

var _ settingWriter = (*tenantSettingWriter)(nil)

// tenantSettingWriter is a settingWriter that writes to system.tenant_settings.
type tenantSettingWriter struct {
	currentTxn *kv.Txn
	tenantID   tree.Datum
}

func (w *tenantSettingWriter) txn(
	ctx context.Context, execCfg *ExecutorConfig, f func(ctx context.Context, txn *kv.Txn) error,
) error {
	return f(ctx, w.currentTxn)
}

func (w *tenantSettingWriter) versionUpgradeHook(
	ctx context.Context,
	execCfg *ExecutorConfig,
	user security.SQLUsername,
	from, to clusterversion.ClusterVersion,
	updateSystemVersionSetting UpdateVersionSystemSettingHook,
) error {
	// See: https://github.com/cockroachdb/cockroach/issues/77733
	// This is the place where we'd like to run the SQL migrations
	// for the target tenant before writing the new value
	// of the version setting via updateSystemVersionSetting().
	return errors.AssertionFailedf("unimplemented")
}

func (w *tenantSettingWriter) deleteSetting(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, name string,
) error {
	_, err := execCfg.InternalExecutor.ExecEx(
		ctx, "reset-tenant-setting", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"DELETE FROM system.tenant_settings WHERE tenant_id = $1 AND name = $2", w.tenantID, name,
	)
	return err
}

func (w *tenantSettingWriter) getSetting(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, name string,
) ([]tree.Datum, error) {
	return execCfg.InternalExecutor.QueryRowEx(
		ctx, "retrieve-prev-tenant-setting", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"SELECT value FROM system.tenant_settings WHERE tenant_id = $1 AND name = $2", w.tenantID, name,
	)
}

func (w *tenantSettingWriter) upsertSetting(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, name string, encoded, typ string,
) error {
	_, err := execCfg.InternalExecutor.ExecEx(
		ctx, "update-tenant-setting", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		`UPSERT INTO system.tenant_settings (tenant_id, name, value, last_updated, value_type) VALUES ($1, $2, $3, now(), $4)`,
		w.tenantID, name, encoded, typ,
	)
	return err
}

func (w *tenantSettingWriter) postChangeTestingNotify(
	ctx context.Context,
	execCfg *ExecutorConfig,
	setting settings.NonMaskedSetting,
	name string,
	encoded string,
) error {
	// Nothing to do here for now.
	return nil
}

func (n *alterTenantSetClusterSettingNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *alterTenantSetClusterSettingNode) Values() tree.Datums            { return nil }
func (n *alterTenantSetClusterSettingNode) Close(_ context.Context)        {}

func resolveTenantID(params runParams, expr tree.TypedExpr) (uint64, tree.Datum, error) {
	tenantIDd, err := expr.Eval(params.p.EvalContext())
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
			"cannot use ALTER TENANT to change cluster settings in system tenant"),
			"Use a regular SET CLUSTER SETTING statement.")
	}
	return uint64(*tenantID), tenantIDd, nil
}

func assertTenantExists(params runParams, tenantID tree.Datum) error {
	exists, err := params.p.ExecCfg().InternalExecutor.QueryRowEx(
		params.ctx, "get-tenant", params.p.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
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
	return nil, unimplemented.NewWithIssue(73857,
		`unimplemented: tenant-level cluster settings not supported`)
}
