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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type alterTenantCapabilityNode struct {
	*tree.AlterTenantCapability
}

func (n *alterTenantCapabilityNode) getBoolValue() (bool, error) {
	if n.IsReset() {
		return false, nil
	}
	dBool, ok := tree.AsDBool(n.CapabilityValue)
	if !ok {
		return false, errors.New("must be bool")
	}
	return bool(dBool), nil
}

func (p *planner) AlterTenantCapability(
	_ context.Context, n *tree.AlterTenantCapability,
) (planNode, error) {
	return &alterTenantCapabilityNode{
		AlterTenantCapability: n,
	}, nil
}

func (n *alterTenantCapabilityNode) startExec(params runParams) error {
	planner := params.p
	tenantIDExpr := n.TenantID
	var dummyHelper tree.IndexedVarHelper
	typedTenantID, err := planner.analyzeExpr(
		params.ctx,
		tenantIDExpr,
		nil, /* source */
		dummyHelper,
		types.Int,
		true, /* requireType */
		fmt.Sprintf("ALTER TENANT %s (RE)SET CAPABILITY", tenantIDExpr),
	)
	if err != nil {
		return err
	}

	tenantID, _, err := resolveTenantID(params.ctx, planner, typedTenantID)
	if err != nil {
		return err
	}
	const op = "update capability"
	execCfg := params.ExecCfg()
	if err := rejectIfCantCoordinateMultiTenancy(execCfg.Codec, op); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(tenantID, op); err != nil {
		return err
	}

	if err := execCfg.InternalExecutorFactory.DescsTxn(
		params.ctx,
		execCfg.DB,
		func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			tenantInfo, err := GetTenantRecordByID(params.ctx, execCfg, txn, roachpb.MustMakeTenantID(tenantID))
			if err != nil {
				return err
			}
			capabilityName := n.CapabilityName
			capabilities := &tenantInfo.Capabilities
			switch capabilityName {
			case "can_admin_split":
				capabilities.CanAdminSplit, err = n.getBoolValue()
			default:
				err = errors.New("unrecognized capability")
			}
			if err != nil {
				return pgerror.Wrapf(
					err,
					pgcode.InvalidParameterValue,
					"error parsing capability value for %s",
					capabilityName,
				)
			}
			if err := UpdateTenantRecord(params.ctx, execCfg, txn, tenantInfo); err != nil {
				return err
			}
			return nil
		},
	); err != nil {
		return err
	}

	return nil
}

func (n *alterTenantCapabilityNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTenantCapabilityNode) Values() tree.Datums          { return nil }
func (n *alterTenantCapabilityNode) Close(context.Context)        {}
