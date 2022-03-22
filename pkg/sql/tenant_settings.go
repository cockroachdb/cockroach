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

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
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

	if err := checkPrivilegesForSetting(ctx, p, name, "set"); err != nil {
		return nil, err
	}

	setting, ok := v.(settings.NonMaskedSetting)
	if !ok {
		return nil, errors.AssertionFailedf("expected writable setting, got %T", v)
	}

	// Error out if we're trying to call this from a non-system tenant or if
	// we're trying to set a system-only variable.
	if !p.execCfg.Codec.ForSystemTenant() {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"ALTER TENANT can only be called by system operators")
	}
	if setting.Class() == settings.SystemOnly {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s is a system-only setting and must be set at the host cluster level using SET CLUSTER SETTING", name)
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
	return unimplemented.NewWithIssue(73857,
		`unimplemented: tenant-level cluster settings not supported`)
}

func (n *alterTenantSetClusterSettingNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *alterTenantSetClusterSettingNode) Values() tree.Datums            { return nil }
func (n *alterTenantSetClusterSettingNode) Close(_ context.Context)        {}

// ShowTenantClusterSetting shows the value of a cluster setting for a tenant.
// Privileges: super user.
func (p *planner) ShowTenantClusterSetting(
	ctx context.Context, n *tree.ShowTenantClusterSetting,
) (planNode, error) {
	return nil, unimplemented.NewWithIssue(73857,
		`unimplemented: tenant-level cluster settings not supported`)
}
