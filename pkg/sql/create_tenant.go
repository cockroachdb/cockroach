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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type createTenantNode struct {
	tenantSpec *tenantSpec
}

func (p *planner) CreateTenantNode(ctx context.Context, n *tree.CreateTenant) (planNode, error) {
	tspec, err := p.planTenantSpec(ctx, n.TenantSpec, "CREATE TENANT")
	if err != nil {
		return nil, err
	}
	return &createTenantNode{
		tenantSpec: tspec,
	}, nil
}

func (n *createTenantNode) startExec(params runParams) error {
	tid, tenantName, err := n.tenantSpec.getTenantParameters(params.ctx, params.p)
	if err != nil {
		return err
	}
	var tenantID uint64
	if !n.tenantSpec.isName {
		tenantID = tid.ToUint64()
	}
	_, err = params.p.CreateTenant(params.ctx, tenantID, tenantName)
	return err
}

func (n *createTenantNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *createTenantNode) Values() tree.Datums            { return tree.Datums{} }
func (n *createTenantNode) Close(_ context.Context)        {}
