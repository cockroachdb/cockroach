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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiesapi"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

const alterTenantCapabilityOp = "ALTER TENANT CAPABILITY"

type alterTenantCapabilityNode struct {
	n          *tree.AlterTenantCapability
	tenantSpec tenantSpec

	// typedExprs contains the planned expressions for each capability
	// (the positions in the slice correspond 1-to-1 to the positions in
	// n.Capabilities).
	typedExprs []tree.TypedExpr
}

func (p *planner) AlterTenantCapability(
	ctx context.Context, n *tree.AlterTenantCapability,
) (planNode, error) {
	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, "grant/revoke capabilities to"); err != nil {
		return nil, err
	}
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.V23_1TenantCapabilities) {
		return nil, errors.New("cannot alter tenant capabilities until version is finalized")
	}

	tSpec, err := p.planTenantSpec(ctx, n.TenantSpec, alterTenantCapabilityOp)
	if err != nil {
		return nil, err
	}

	exprs := make([]tree.TypedExpr, len(n.Capabilities))
	for i, capability := range n.Capabilities {
		capabilityNameString := capability.Name
		capabilityValue := capability.Value
		if n.IsRevoke {
			// In REVOKE, we do not support a value assignment.
			if capabilityValue != nil {
				return nil, pgerror.Newf(pgcode.Syntax, "no value allowed in revoke: %q", capabilityNameString)
			}
		} else {
			var desiredType *types.T
			if _, ok := tenantcapabilitiesapi.BoolCapabilityNameFromString(capabilityNameString); ok {
				desiredType = types.Bool
			} else if _, _, ok := tenantcapabilitiesapi.Int32RangeCapabilityNameFromString(capabilityNameString); ok {
				desiredType = types.Int
			} else {
				return nil, errors.Newf("unknown capability: %q", capabilityNameString)
			}
			var typedValue tree.TypedExpr
			if capabilityValue == nil {
				if desiredType != types.Bool {
					return nil, pgerror.Newf(pgcode.Syntax, "value required for capability: %q", capabilityNameString)
				}
				// Bool capabilities are a special case that default to true if no value is provided.
				typedValue = tree.DBoolTrue
			} else {
				// Type check the expression on the right-hand side of the assignment.
				var dummyHelper tree.IndexedVarHelper
				typedValue, err = p.analyzeExpr(
					ctx,
					capabilityValue,
					nil, /* source */
					dummyHelper,
					desiredType,
					true, /* requireType */
					fmt.Sprintf("%s %s", alterTenantCapabilityOp, capabilityNameString),
				)
				if err != nil {
					return nil, err
				}
			}
			exprs[i] = typedValue
		}
	}

	return &alterTenantCapabilityNode{
		n:          n,
		tenantSpec: tSpec,
		typedExprs: exprs,
	}, nil
}

func (n *alterTenantCapabilityNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx

	// Privilege check.
	if err := p.RequireAdminRole(ctx, "update tenant capabilities"); err != nil {
		return err
	}

	// Refuse to work in read-only transactions.
	if p.EvalContext().TxnReadOnly {
		return readOnlyError(alterTenantCapabilityOp)
	}

	// Look up the enant.
	tenantInfo, err := n.tenantSpec.getTenantInfo(ctx, p)
	if err != nil {
		return err
	}

	// Refuse to modify the system tenant.
	if err := rejectIfSystemTenant(tenantInfo.ID, alterTenantCapabilityOp); err != nil {
		return err
	}

	dst := &tenantInfo.Capabilities
	capabilities := n.n.Capabilities
	for i, capability := range capabilities {
		capabilityNameString := capability.Name
		typedExpr := n.typedExprs[i]
		if capabilityName, ok := tenantcapabilitiesapi.BoolCapabilityNameFromString(capabilityNameString); ok {
			capabilityValue := false
			if !n.n.IsRevoke {
				capabilityValue, err = paramparse.DatumAsBool(ctx, p.EvalContext(), capabilityNameString, typedExpr)
				if err != nil {
					return err
				}
			}
			dst.SetBoolCapability(capabilityName, capabilityValue)
		} else if capabilityName, isMin, ok := tenantcapabilitiesapi.Int32RangeCapabilityNameFromString(capabilityNameString); ok {
			capabilityValue := dst.GetInt32RangeCapability(capabilityName)
			int32Value := int32(0)
			if !n.n.IsRevoke {
				int64Value, err := paramparse.DatumAsInt(ctx, p.EvalContext(), capabilityNameString, typedExpr)
				if err != nil {
					return err
				}
				int32Value = int32(int64Value)
			}
			if isMin {
				capabilityValue.Min = int32Value
			} else {
				capabilityValue.Max = int32Value
			}
			dst.SetInt32RangeCapability(capabilityName, capabilityValue)
		} else {
			return errors.Newf("unknown capability: %q", capabilityNameString)
		}
	}

	return UpdateTenantRecord(ctx, p.ExecCfg().Settings, p.InternalSQLTxn(), tenantInfo)
}

func (n *alterTenantCapabilityNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTenantCapabilityNode) Values() tree.Datums          { return nil }
func (n *alterTenantCapabilityNode) Close(context.Context)        {}
