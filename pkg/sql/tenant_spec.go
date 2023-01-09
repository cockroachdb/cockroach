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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type tenantSpec struct {
	expr   tree.TypedExpr
	all    bool
	isName bool
}

func (p *planner) planTenantSpec(
	ctx context.Context, ts *tree.TenantSpec, op string,
) (*tenantSpec, error) {
	ret := &tenantSpec{
		all:    ts.All,
		isName: ts.IsName,
	}
	if ret.all {
		return ret, nil
	}
	var dummyHelper tree.IndexedVarHelper
	if !ret.isName {
		// By-ID reference.
		typedTenantID, err := p.analyzeExpr(
			ctx, ts.Expr, nil, dummyHelper, types.Int, true, op)
		if err != nil {
			return nil, err
		}
		ret.expr = typedTenantID
	} else {
		// By-name reference.
		e := ts.Expr

		// If the expression is a simple identifier, handle
		// that specially: we promote that identifier to a SQL string.
		// This is alike what is done for CREATE USER.
		if s, ok := e.(*tree.UnresolvedName); ok {
			e = tree.NewStrVal(tree.AsStringWithFlags(s, tree.FmtBareIdentifiers))
		}

		typedTenantName, err := p.analyzeExpr(
			ctx, e, nil, dummyHelper, types.String, true, op)
		if err != nil {
			return nil, err
		}
		ret.expr = typedTenantName
	}
	return ret, nil
}

func (ts *tenantSpec) String() string {
	if ts.all {
		return "all"
	}
	if ts.isName {
		return ts.expr.String()
	}
	return "[" + ts.expr.String() + "]"
}

func (ts *tenantSpec) getTenantParameters(
	ctx context.Context, p *planner,
) (tid roachpb.TenantID, tenantName roachpb.TenantName, err error) {
	if !ts.isName {
		tenantIDd, err := eval.Expr(ctx, p.EvalContext(), ts.expr)
		if err != nil {
			return tid, tenantName, err
		}
		tenantID := uint64(tree.MustBeDInt(tenantIDd))
		tid, err = roachpb.MakeTenantID(tenantID)
		return tid, tenantName, err
	}
	tenantNamed, err := eval.Expr(ctx, p.EvalContext(), ts.expr)
	if err != nil {
		return tid, tenantName, err
	}
	tenantName = roachpb.TenantName(tree.MustBeDString(tenantNamed))
	return tid, tenantName, nil
}

func (ts *tenantSpec) getTenantInfo(
	ctx context.Context, p *planner,
) (ret *descpb.TenantInfo, err error) {
	tid, tenantName, err := ts.getTenantParameters(ctx, p)
	if err != nil {
		return nil, err
	}
	if !ts.isName {
		return GetTenantRecordByID(ctx, p.ExecCfg(), p.Txn(), tid)
	}

	// The logic for by-name lookup follows.
	return GetTenantRecordByName(ctx, p.ExecCfg(), p.Txn(), tenantName)
}

// LookupTenantInfo implements PlanHookState for the benefits of CCL statements.
func (p *planner) LookupTenantInfo(
	ctx context.Context, ts *tree.TenantSpec, op string,
) (*descpb.TenantInfo, error) {
	if ts.All {
		return nil, errors.AssertionFailedf("programming error: cannot use ALL in this context")
	}
	tspec, err := p.planTenantSpec(ctx, ts, op)
	if err != nil {
		return nil, err
	}
	return tspec.getTenantInfo(ctx, p)
}
