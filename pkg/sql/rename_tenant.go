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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type renameTenantNode struct {
	tenantSpec tenantSpec
	newName    tree.TypedExpr
}

func (p *planner) alterRenameTenant(
	ctx context.Context, n *tree.AlterTenantRename,
) (planNode, error) {
	// Even though the call to renameTenant in startExec also
	// performs this check, we need to do this early because otherwise
	// the lookup of the ID from the name will fail.
	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, "rename"); err != nil {
		return nil, err
	}

	e := n.NewName
	// If the expression is a simple identifier, handle
	// that specially: we promote that identifier to a SQL string.
	// This is alike what is done for CREATE USER.
	if s, ok := e.(*tree.UnresolvedName); ok {
		e = tree.NewStrVal(tree.AsStringWithFlags(s, tree.FmtBareIdentifiers))
	}
	tname, err := p.analyzeExpr(
		ctx, e, nil, tree.IndexedVarHelper{}, types.String, true, "ALTER TENANT RENAME")
	if err != nil {
		return nil, err
	}

	tspec, err := p.planTenantSpec(ctx, n.TenantSpec, "ALTER TENANT RENAME")
	if err != nil {
		return nil, err
	}
	return &renameTenantNode{
		tenantSpec: tspec,
		newName:    tname,
	}, nil
}

func (n *renameTenantNode) startExec(params runParams) error {
	newNamed, err := eval.Expr(params.ctx, params.p.EvalContext(), n.newName)
	if err != nil {
		return err
	}
	newName, err := validateTenantName(params.ctx, newNamed)
	if err != nil {
		return err
	}
	rec, err := n.tenantSpec.getTenantInfo(params.ctx, params.p)
	if err != nil {
		return err
	}
	return params.p.renameTenant(params.ctx, rec, newName)
}

func (n *renameTenantNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *renameTenantNode) Values() tree.Datums            { return tree.Datums{} }
func (n *renameTenantNode) Close(_ context.Context)        {}
