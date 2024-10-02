// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type tenantSpec interface {
	fmt.Stringer
	getTenantInfo(ctx context.Context, p *planner) (ret *mtinfopb.TenantInfo, err error)
	getTenantParameters(ctx context.Context, p *planner) (tid roachpb.TenantID, tenantName roachpb.TenantName, err error)
}

type tenantSpecAll struct{}
type tenantSpecName struct{ tree.TypedExpr }
type tenantSpecId struct{ tree.TypedExpr }

var _ tenantSpec = tenantSpecAll{}
var _ tenantSpec = (*tenantSpecName)(nil)
var _ tenantSpec = (*tenantSpecId)(nil)

func (p *planner) planTenantSpec(
	ctx context.Context, ts *tree.TenantSpec, op string,
) (tenantSpec, error) {
	if ts.All {
		return tenantSpecAll{}, nil
	}
	var dummyHelper tree.IndexedVarHelper
	if !ts.IsName {
		// By-ID reference.
		typedTenantID, err := p.analyzeExpr(
			ctx, ts.Expr, dummyHelper, types.Int, true, op)
		if err != nil {
			return nil, err
		}
		return &tenantSpecId{TypedExpr: typedTenantID}, nil
	}

	// By-name reference.
	e := ts.Expr

	// If the expression is a simple identifier, handle
	// that specially: we promote that identifier to a SQL string.
	// This is alike what is done for CREATE USER.
	if s, ok := e.(*tree.UnresolvedName); ok {
		e = tree.NewStrVal(tree.AsStringWithFlags(s, tree.FmtBareIdentifiers))
	}

	typedTenantName, err := p.analyzeExpr(
		ctx, e, dummyHelper, types.String, true, op)
	if err != nil {
		return nil, err
	}
	return &tenantSpecName{TypedExpr: typedTenantName}, nil
}

func (tenantSpecAll) String() string      { return "all" }
func (ts *tenantSpecName) String() string { return ts.TypedExpr.String() }
func (ts *tenantSpecId) String() string   { return "[" + ts.TypedExpr.String() + "]" }

func (tenantSpecAll) getTenantParameters(
	ctx context.Context, p *planner,
) (tid roachpb.TenantID, tenantName roachpb.TenantName, err error) {
	return tid, tenantName, errors.AssertionFailedf("programming error: cannot use all in this context")
}

func (ts *tenantSpecName) getTenantParameters(
	ctx context.Context, p *planner,
) (tid roachpb.TenantID, tenantName roachpb.TenantName, err error) {
	tenantNamed, err := eval.Expr(ctx, p.EvalContext(), ts.TypedExpr)
	if err != nil {
		return tid, tenantName, err
	}
	tenantName, err = validateTenantName(ctx, tenantNamed)
	return tid, tenantName, err
}

func validateTenantName(ctx context.Context, tenantNamed tree.Datum) (roachpb.TenantName, error) {
	if tenantNamed == tree.DNull {
		return "", pgerror.New(pgcode.Syntax, "tenant name cannot be NULL")
	}
	tenantName := roachpb.TenantName(tree.MustBeDString(tenantNamed))
	if tenantName == "" {
		return "", pgerror.New(pgcode.Syntax, "tenant name cannot be empty")
	}
	return tenantName, nil
}

func (ts *tenantSpecId) getTenantParameters(
	ctx context.Context, p *planner,
) (tid roachpb.TenantID, tenantName roachpb.TenantName, err error) {
	tenantIDd, err := eval.Expr(ctx, p.EvalContext(), ts.TypedExpr)
	if err != nil {
		return tid, tenantName, err
	}
	if tenantIDd == tree.DNull {
		return tid, tenantName, pgerror.New(pgcode.Syntax, "tenant ID cannot be NULL")
	}
	tenantID := uint64(tree.MustBeDInt(tenantIDd))
	tid, err = roachpb.MakeTenantID(tenantID)
	return tid, tenantName, err
}

func (tenantSpecAll) getTenantInfo(
	ctx context.Context, p *planner,
) (ret *mtinfopb.TenantInfo, err error) {
	return nil, errors.AssertionFailedf("programming error: cannot use all in this context")
}

func (ts *tenantSpecName) getTenantInfo(
	ctx context.Context, p *planner,
) (ret *mtinfopb.TenantInfo, err error) {
	_, tenantName, err := ts.getTenantParameters(ctx, p)
	if err != nil {
		return nil, err
	}
	return GetTenantRecordByName(ctx, p.ExecCfg().Settings, p.InternalSQLTxn(), tenantName)
}

func (ts *tenantSpecId) getTenantInfo(
	ctx context.Context, p *planner,
) (ret *mtinfopb.TenantInfo, err error) {
	tid, _, err := ts.getTenantParameters(ctx, p)
	if err != nil {
		return nil, err
	}
	return GetTenantRecordByID(ctx, p.InternalSQLTxn(), tid, p.ExecCfg().Settings)
}

// LookupTenantInfo implements PlanHookState for the benefits of CCL statements.
func (p *planner) LookupTenantInfo(
	ctx context.Context, ts *tree.TenantSpec, op string,
) (*mtinfopb.TenantInfo, error) {
	tspec, err := p.planTenantSpec(ctx, ts, op)
	if err != nil {
		return nil, err
	}
	return tspec.getTenantInfo(ctx, p)
}
