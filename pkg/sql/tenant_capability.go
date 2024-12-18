// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
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

const alterTenantCapabilityOp = "ALTER VIRTUAL CLUSTER CAPABILITY"

type alterTenantCapabilityNode struct {
	zeroInputPlanNode
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
	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, "grant/revoke capabilities to", p.execCfg.Settings); err != nil {
		return nil, err
	}

	tSpec, err := p.planTenantSpec(ctx, n.TenantSpec, alterTenantCapabilityOp)
	if err != nil {
		return nil, err
	}

	exprs := make([]tree.TypedExpr, len(n.Capabilities))
	for i, update := range n.Capabilities {
		capability, ok := tenantcapabilities.FromName(update.Name)
		if !ok {
			return nil, pgerror.Newf(pgcode.Syntax, "unknown capability: %q", update.Name)
		}

		var desiredType *types.T
		var missingValueDefault, revokeValue tree.TypedExpr
		switch capability.(type) {
		case tenantcapabilities.BoolCapability:
			desiredType = types.Bool
			// Granting a boolean capability without providing an explicit value
			// translates to true.
			missingValueDefault = tree.DBoolTrue
			revokeValue = tree.DBoolFalse
		case tenantcapabilities.SpanConfigBoundsCapability:
			desiredType = types.Bytes
		default:
			return nil, errors.AssertionFailedf(
				"programming error: capability %v type %T not handled: capability ID: %d",
				capability, capability, capability.ID(),
			)
		}

		if n.IsRevoke {
			// In REVOKE, we do not support a value assignment.
			if update.Value != nil {
				return nil, pgerror.Newf(pgcode.Syntax, "no value allowed in revoke: %q", update.Name)
			}
			exprs[i] = revokeValue
		} else {
			var typedValue tree.TypedExpr
			if update.Value == nil {
				if missingValueDefault == nil {
					return nil, pgerror.Newf(pgcode.Syntax, "value required for capability: %q", capability)
				}
				typedValue = missingValueDefault
			} else {
				// Type check the expression on the right-hand side of the assignment.
				var dummyHelper tree.IndexedVarHelper
				typedValue, err = p.analyzeExpr(
					ctx,
					update.Value,
					dummyHelper,
					desiredType,
					true, /* requireType */
					fmt.Sprintf("%s %s", alterTenantCapabilityOp, update.Name),
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
	if err := CanManageTenant(ctx, p); err != nil {
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

	if n.n.AllCapabilities {
		for capID := tenantcapabilities.ID(1); capID <= tenantcapabilities.MaxCapabilityID; capID++ {
			cap, _ := tenantcapabilities.FromID(capID)
			switch c := cap.(type) {
			case tenantcapabilities.BoolCapability:
				val := true
				if n.n.IsRevoke {
					val = false
				}
				c.Value(dst).Set(val)

			case tenantcapabilities.SpanConfigBoundsCapability:
				// "REVOKE" on span config bounds has no meaning currently.
				if !n.n.IsRevoke {
					c.Value(dst).Set(nil)
				}

			default:
				return errors.AssertionFailedf(
					"programming error: capability %v type %T not handled: capability ID: %d",
					cap, cap, cap.ID(),
				)
			}
		}
	} else {
		for i, update := range n.n.Capabilities {
			typedExpr := n.typedExprs[i]
			capability, ok := tenantcapabilities.FromName(update.Name)
			if !ok {
				// We've already checked this above.
				return errors.AssertionFailedf("programming error: %q", update.Name)
			}

			switch c := capability.(type) {
			case tenantcapabilities.BoolCapability:
				boolValue, err := paramparse.DatumAsBool(ctx, p.EvalContext(), update.Name, typedExpr)
				if err != nil {
					return err
				}
				c.Value(dst).Set(boolValue)
			case tenantcapabilities.SpanConfigBoundsCapability:
				if n.n.IsRevoke {
					return pgerror.Newf(pgcode.InvalidParameterValue, "cannot REVOKE CAPABILITY %q", capability)
				}
				datum, err := eval.Expr(ctx, p.EvalContext(), typedExpr)
				if err != nil {
					return err
				}
				var bounds *spanconfigbounds.Bounds
				// Allow NULL, and use it to clear the SpanConfigBounds.
				if datum == tree.DNull {

				} else if dBytes, ok := datum.(*tree.DBytes); ok {
					boundspb := new(tenantcapabilitiespb.SpanConfigBounds)
					if err := protoutil.Unmarshal([]byte(*dBytes), boundspb); err != nil {
						return errors.WithDetail(
							pgerror.Wrapf(
								err, pgcode.InvalidParameterValue, "invalid %q value",
								capability,
							),
							"cannot decode into cockroach.multitenant.tenantcapabilitiespb.SpanConfigBounds",
						)
					}
					// Converting the raw proto to spanconfigbounds.Bounds will ensure
					// constraints are sorted.
					//
					// TODO(ajwerner,arul): Validate some properties of the bounds.
					// We'll also want to make sure that kvserver sanity checks the values
					// it uses when clamping -- we don't want to clamp the range sizes to be
					// tiny or GC TTL to be too short because of operator error. Some of
					// this checking could be pushed into spanconfigbounds.New. We might
					// also want to check tandem fields to ensure they make sense -- for
					// example, the range for min/max range sizes should have some overlap.
					bounds = spanconfigbounds.New(boundspb)
				} else {
					return errors.WithDetailf(
						pgerror.Newf(
							pgcode.InvalidParameterValue, "parameter %q requires bytes value",
						),
						"%s is a %s", datum, datum.ResolvedType(),
					)
				}
				c.Value(dst).Set(bounds)

			default:
				return errors.AssertionFailedf(
					"programming error: capability %v type %v not handled: capability ID: %d",
					capability, capability, capability.ID(),
				)
			}
		}
	}

	return UpdateTenantRecord(ctx, p.ExecCfg().Settings, p.InternalSQLTxn(), tenantInfo)
}

func (n *alterTenantCapabilityNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTenantCapabilityNode) Values() tree.Datums          { return nil }
func (n *alterTenantCapabilityNode) Close(context.Context)        {}
