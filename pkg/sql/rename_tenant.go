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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type renameTenantNode struct {
	tenantSpec *tenantSpec
	newName    tree.Name
}

func (p *planner) alterRenameTenant(
	ctx context.Context, n *tree.AlterTenantRename,
) (planNode, error) {
	tspec, err := p.planTenantSpec(ctx, n.TenantSpec, "ALTER TENANT RENAME")
	if err != nil {
		return nil, err
	}
	return &renameTenantNode{
		tenantSpec: tspec,
		newName:    n.NewName,
	}, nil
}

func (n *renameTenantNode) startExec(params runParams) error {
	rec, err := n.tenantSpec.getTenantInfo(params.ctx, params.p)
	if err != nil {
		return err
	}
	return params.p.RenameTenant(params.ctx, rec.ID, roachpb.TenantName(n.newName))
}

func (n *renameTenantNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *renameTenantNode) Values() tree.Datums            { return tree.Datums{} }
func (n *renameTenantNode) Close(_ context.Context)        {}
