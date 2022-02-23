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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// alterTenantSetClusterSettingNode represents an
// ALTER TENANT ... SET CLUSTER SETTING statement.
type alterTenantSetClusterSettingNode struct {
	name      string
	tenantID  roachpb.TenantID
	tenantAll bool
	st        *cluster.Settings
	setting   settings.NonMaskedSetting
	// If value is nil, the setting should be reset.
	value tree.TypedExpr
}

// AlterTenantSetClusterSetting sets tenant level session variables.
// Privileges: super user.
func (p *planner) AlterTenantSetClusterSetting(
	ctx context.Context, n *tree.AlterTenantSetClusterSetting,
) (planNode, error) {
	name := strings.ToLower(n.Name)
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

	// Error out if we're trying to set a system only variable, or if we're in
	// a non-system SQL server and we're trying to write to a setting that is
	// not tenant-writable.
	if setting.Class() == settings.SystemOnly {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s is a system-only setting and must be set using SET CLUSTER SETTING", name)
	}
	if !p.execCfg.Codec.ForSystemTenant() && setting.Class() != settings.TenantWritable {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s can only be set by system operators", name)
	}

	value, err := p.getAndValidateTypedClusterSetting(ctx, name, n.Value, setting)
	if err != nil {
		return nil, err
	}

	node := alterTenantSetClusterSettingNode{
		name: name, tenantID: n.TenantID, tenantAll: n.TenantAll, st: st,
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
