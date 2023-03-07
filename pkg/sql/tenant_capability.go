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
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
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
		return nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "cannot alter tenant capabilities until version is finalized")
	}

	tSpec, err := p.planTenantSpec(ctx, n.TenantSpec, alterTenantCapabilityOp)
	if err != nil {
		return nil, err
	}

	exprs := make([]tree.TypedExpr, len(n.Capabilities))
	for i, capability := range n.Capabilities {
		capID, ok := tenantcapabilities.CapabilityIDFromString(capability.Name)
		if !ok {
			return nil, pgerror.Newf(pgcode.Syntax, "unknown capability: %q", capability.Name)
		}

		var desiredType *types.T
		var missingValueDefault, revokeValue tree.TypedExpr
		capType := capID.CapabilityType()
		switch capType {
		case tenantcapabilities.Bool:
			desiredType = types.Bool
			// Bool capabilities are a special case that default to true if no value is provided.
			missingValueDefault = tree.DBoolTrue
			revokeValue = tree.DBoolFalse
		default:
			return nil, errors.AssertionFailedf("programming error: capability type not handled: %d capability ID: %d", capType, capID)
		}

		if n.IsRevoke {
			// In REVOKE, we do not support a value assignment.
			if capability.Value != nil {
				return nil, pgerror.Newf(pgcode.Syntax, "no value allowed in revoke: %q", capability.Name)
			}
			exprs[i] = revokeValue
		} else {
			var typedValue tree.TypedExpr
			if capability.Value == nil {
				// TODO: Uncomment this block when a new capability type is added above.
				//  It is commented out to prevent a linter error.
				// if missingValueDefault == nil {
				// 	return nil, pgerror.Newf(pgcode.Syntax, "value required for capability: %q", capability.Name)
				// }
				typedValue = missingValueDefault
			} else {
				// Type check the expression on the right-hand side of the assignment.
				var dummyHelper tree.IndexedVarHelper
				typedValue, err = p.analyzeExpr(
					ctx,
					capability.Value,
					nil, /* source */
					dummyHelper,
					desiredType,
					true, /* requireType */
					fmt.Sprintf("%s %s", alterTenantCapabilityOp, capability.Name),
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
		typedExpr := n.typedExprs[i]
		capID, ok := tenantcapabilities.CapabilityIDFromString(capability.Name)
		if !ok {
			// We've already checked this above.
			return errors.AssertionFailedf("programming error: %q", capability.Name)
		}

		var value interface{}
		capType := capID.CapabilityType()
		switch capType {
		case tenantcapabilities.Bool:
			boolValue, err := paramparse.DatumAsBool(ctx, p.EvalContext(), capability.Name, typedExpr)
			if err != nil {
				return err
			}
			value = boolValue

		default:
			return errors.AssertionFailedf("programming error: capability type not handled: %d capability ID: %d", capType, capID)
		}

		dst.Cap(capID).Set(value)
	}

	return UpdateTenantRecord(ctx, p.ExecCfg().Settings, p.InternalSQLTxn(), tenantInfo)
}

func (n *alterTenantCapabilityNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTenantCapabilityNode) Values() tree.Datums          { return nil }
func (n *alterTenantCapabilityNode) Close(context.Context)        {}
