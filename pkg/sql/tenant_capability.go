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

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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

	tSpec, err := p.planTenantSpec(ctx, n.TenantSpec, alterTenantCapabilityOp)
	if err != nil {
		return nil, err
	}

	exprs := make([]tree.TypedExpr, len(n.Capabilities))
	for i, capability := range n.Capabilities {
		capabilityName, err := tenantcapabilitiespb.TenantCapabilityNameFromString(capability.Name)
		if err != nil {
			return nil, err
		}

		// In REVOKE, we do not support a value assignment.
		capabilityValue := capability.Value
		if n.IsRevoke {
			if capabilityValue != nil {
				return nil, pgerror.Newf(pgcode.Syntax, "no value allowed in revoke: %q", capabilityName)
			}
			continue
		}

		// Type check the expression on the right-hand side of the
		// assignment.
		if capabilityValue != nil {
			// Currently only bool types are supported.
			desiredType := types.Bool
			var dummyHelper tree.IndexedVarHelper
			typedValue, err := p.analyzeExpr(
				ctx,
				capabilityValue,
				nil, /* source */
				dummyHelper,
				desiredType,
				true, /* requireType */
				fmt.Sprintf("%s %s", alterTenantCapabilityOp, capabilityName),
			)
			if err != nil {
				return nil, err
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
		capabilityName, err := tenantcapabilitiespb.TenantCapabilityNameFromString(capability.Name)
		if err != nil {
			return err
		}

		capabilityValue := false
		if !n.n.IsRevoke {
			// Default capability value to true if no value is set.
			capabilityValue = true
			typedExpr := n.typedExprs[i]
			if typedExpr != nil {
				capabilityValue, err = paramparse.DatumAsBool(ctx, p.EvalContext(), capabilityName.String(), typedExpr)
				if err != nil {
					return err
				}
			}
		}
		dst.SetFlagCapability(capabilityName, capabilityValue)
	}

	return UpdateTenantRecord(ctx, p.ExecCfg().Settings, p.InternalSQLTxn(), tenantInfo)
}

func (n *alterTenantCapabilityNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTenantCapabilityNode) Values() tree.Datums          { return nil }
func (n *alterTenantCapabilityNode) Close(context.Context)        {}
