// Copyright 2023 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

const CanAdminSplitCapabilityName = "can_admin_split"
const CanAdminUnsplitCapabilityName = "can_admin_unsplit"

type alterTenantCapabilityNode struct {
	*tree.AlterTenantCapability
}

func (p *planner) AlterTenantCapability(
	_ context.Context, n *tree.AlterTenantCapability,
) (planNode, error) {
	return &alterTenantCapabilityNode{
		AlterTenantCapability: n,
	}, nil
}

func (n *alterTenantCapabilityNode) startExec(params runParams) error {
	const op = "ALTER TENANT CAPABILITY"
	execCfg := params.ExecCfg()
	if err := rejectIfCantCoordinateMultiTenancy(execCfg.Codec, op); err != nil {
		return err
	}
	planner := params.p
	ctx := params.ctx
	tSpec, err := planner.planTenantSpec(ctx, n.TenantSpec, op)
	if err != nil {
		return err
	}
	tenantInfo, err := tSpec.getTenantInfo(ctx, planner)
	if err != nil {
		return err
	}
	if err := rejectIfSystemTenant(tenantInfo.ID, op); err != nil {
		return err
	}
	isRevoke := n.IsRevoke
	capabilities := &tenantInfo.Capabilities
	for _, capability := range n.Capabilities {
		capabilityName := capability.Name
		switch capabilityName {
		case CanAdminSplitCapabilityName:
			capabilities.CanAdminSplit, err = capability.GetBoolValue(isRevoke)
		default:
			err = errors.Newf("invalid capability")
		}
		if err != nil {
			return pgerror.Wrapf(
				err,
				pgcode.InvalidParameterValue,
				"error parsing capability %q",
				capabilityName,
			)
		}
	}

	if err := UpdateTenantRecord(params.ctx, execCfg.Settings, planner.InternalSQLTxn(), tenantInfo); err != nil {
		return err
	}
	return nil
}

func (n *alterTenantCapabilityNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTenantCapabilityNode) Values() tree.Datums          { return nil }
func (n *alterTenantCapabilityNode) Close(context.Context)        {}
