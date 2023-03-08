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
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigbounds"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

var capabilityTypes = map[tenantcapabilitiespb.TenantCapabilityName]*types.T{
	tenantcapabilitiespb.CanAdminSplit:          types.Bool,
	tenantcapabilitiespb.CanViewNodeInfo:        types.Bool,
	tenantcapabilitiespb.CanViewTSDBMetrics:     types.Bool,
	tenantcapabilitiespb.TenantSpanConfigBounds: types.Bytes,
}

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
		capabilityName, err := tenantcapabilitiespb.TenantCapabilityNameFromString(capability.Name)
		if err != nil {
			return nil, pgerror.WithCandidateCode(err, pgcode.Syntax)
		}
		desiredType, ok := capabilityTypes[capabilityName]
		if !ok {
			return nil, pgerror.Newf(pgcode.Syntax, "unknown capability: %q", capabilityName)
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
	for i, capability := range n.n.Capabilities {
		capabilityName, err := tenantcapabilitiespb.TenantCapabilityNameFromString(capability.Name)
		if err != nil {
			return err
		}
		typedExpr := n.typedExprs[i]
		switch capabilityName {
		case tenantcapabilitiespb.CanAdminSplit:
			if n.n.IsRevoke {
				dst.CanAdminSplit = false
			} else {
				b := true
				if typedExpr != nil {
					b, err = paramparse.DatumAsBool(ctx, p.EvalContext(), capabilityName.String(), typedExpr)
					if err != nil {
						return err
					}
				}
				dst.CanAdminSplit = b
			}

		case tenantcapabilitiespb.CanViewNodeInfo:
			if n.n.IsRevoke {
				dst.CanViewNodeInfo = false
			} else {
				b := true
				if typedExpr != nil {
					b, err = paramparse.DatumAsBool(ctx, p.EvalContext(), capabilityName.String(), typedExpr)
					if err != nil {
						return err
					}
				}
				dst.CanViewNodeInfo = b
			}

		case tenantcapabilitiespb.CanViewTSDBMetrics:
			if n.n.IsRevoke {
				dst.CanViewTSDBMetrics = false
			} else {
				b := true
				if typedExpr != nil {
					b, err = paramparse.DatumAsBool(ctx, p.EvalContext(), capabilityName.String(), typedExpr)
					if err != nil {
						return err
					}
				}
				dst.CanViewTSDBMetrics = b
			}

		case tenantcapabilitiespb.TenantSpanConfigBounds:
			if n.n.IsRevoke {
				return pgerror.Newf(
					pgcode.InvalidParameterValue, "cannot REVOKE CAPABILITY %q", capabilityName,
				)
			}
			datum, err := eval.Expr(ctx, p.EvalContext(), typedExpr)
			if err != nil {
				return err
			}
			var bounds *tenantcapabilitiespb.SpanConfigBounds
			// Allow NULL, and use it to clear the SpanConfigBounds.
			if datum == tree.DNull {

			} else if dBytes, ok := datum.(*tree.DBytes); ok {
				bounds = new(tenantcapabilitiespb.SpanConfigBounds)
				if err := protoutil.Unmarshal([]byte(*dBytes), bounds); err != nil {
					return errors.WithDetail(
						pgerror.Wrapf(
							err, pgcode.InvalidParameterValue, "invalid %q value",
							capabilityName,
						),
						"cannot decode into cockroach.multitenant.tenantcapabilitiespb.SpanConfigBounds",
					)
				}
				// MakeBounds will sort the constraints.
				//
				// TODO(ajwerner): Validate some properties of the bounds. We'll also
				// want to make sure that kvserver sanity checks the values it uses
				// when clamping -- we don't want to clamp the range sizes to be tiny
				// or GC TTL to be too short because of operator error. Some of this
				// checking could be pushed into MakeBounds.
				spanconfigbounds.MakeBounds(bounds)
			} else {
				return errors.WithDetailf(
					pgerror.Newf(
						pgcode.InvalidParameterValue, "parameter %q requires bytes value",
					),
					"%s is a %s", datum, datum.ResolvedType(),
				)
			}
			dst.SpanConfigBounds = bounds
		default:
			return errors.AssertionFailedf("unhandled: %q", capabilityName)
		}
	}

	return UpdateTenantRecord(ctx, p.ExecCfg().Settings, p.InternalSQLTxn(), tenantInfo)
}

func (n *alterTenantCapabilityNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTenantCapabilityNode) Values() tree.Datums          { return nil }
func (n *alterTenantCapabilityNode) Close(context.Context)        {}
