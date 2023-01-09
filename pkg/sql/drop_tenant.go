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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type dropTenantNode struct {
	tenantSpec *tenantSpec
	ifExists   bool
}

func (p *planner) DropTenant(ctx context.Context, n *tree.DropTenant) (planNode, error) {
	tspec, err := p.planTenantSpec(ctx, n.TenantSpec, "DROP TENANT")
	if err != nil {
		return nil, err
	}
	return &dropTenantNode{
		tenantSpec: tspec,
		ifExists:   n.IfExists,
	}, nil
}

func (n *dropTenantNode) startExec(params runParams) error {
	tenInfo, err := n.tenantSpec.getTenantInfo(params.ctx, params.p)
	if err != nil {
		if pgerror.GetPGCode(err) == pgcode.UndefinedObject && n.ifExists {
			return nil
		}
		return err
	}
	return params.p.DestroyTenantByID(params.ctx, tenInfo.ID, false /* synchronous */)
}

func (n *dropTenantNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *dropTenantNode) Values() tree.Datums            { return tree.Datums{} }
func (n *dropTenantNode) Close(_ context.Context)        {}
