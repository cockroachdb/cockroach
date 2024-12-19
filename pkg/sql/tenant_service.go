// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type alterTenantServiceNode struct {
	zeroInputPlanNode
	tenantSpec tenantSpec
	newMode    mtinfopb.TenantServiceMode
}

func (p *planner) alterTenantService(
	ctx context.Context, n *tree.AlterTenantService,
) (planNode, error) {
	// Even though the call to Update in startExec also
	// performs this check, we need to do this early because otherwise
	// the lookup of the ID from the name will fail.
	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, "set tenant service", p.execCfg.Settings); err != nil {
		return nil, err
	}

	var newMode mtinfopb.TenantServiceMode
	switch n.Command {
	case tree.TenantStopService:
		newMode = mtinfopb.ServiceModeNone
	case tree.TenantStartServiceExternal:
		newMode = mtinfopb.ServiceModeExternal
	case tree.TenantStartServiceShared:
		newMode = mtinfopb.ServiceModeShared
	default:
		return nil, errors.AssertionFailedf("unhandled case: %+v", n)
	}

	tspec, err := p.planTenantSpec(ctx, n.TenantSpec, "ALTER VIRTUAL CLUSTER SERVICE")
	if err != nil {
		return nil, err
	}
	return &alterTenantServiceNode{
		tenantSpec: tspec,
		newMode:    newMode,
	}, nil
}

func (n *alterTenantServiceNode) startExec(params runParams) error {
	rec, err := n.tenantSpec.getTenantInfo(params.ctx, params.p)
	if err != nil {
		return err
	}
	return params.p.setTenantService(params.ctx, rec, n.newMode)
}

func (n *alterTenantServiceNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *alterTenantServiceNode) Values() tree.Datums            { return tree.Datums{} }
func (n *alterTenantServiceNode) Close(_ context.Context)        {}
