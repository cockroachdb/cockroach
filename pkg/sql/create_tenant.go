// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type createTenantNode struct {
	zeroInputPlanNode
	ifNotExists bool
	tenantSpec  tenantSpec
}

func (p *planner) CreateTenantNode(ctx context.Context, n *tree.CreateTenant) (planNode, error) {
	tspec, err := p.planTenantSpec(ctx, n.TenantSpec, "CREATE VIRTUAL CLUSTER")
	if err != nil {
		return nil, err
	}
	return &createTenantNode{
		ifNotExists: n.IfNotExists,
		tenantSpec:  tspec,
	}, nil
}

func (n *createTenantNode) startExec(params runParams) error {
	tid, tenantName, err := n.tenantSpec.getTenantParameters(params.ctx, params.p)
	if err != nil {
		return err
	}

	var ctcfg createTenantConfig
	if tenantName != "" {
		ctcfg.Name = (*string)(&tenantName)
	}
	if tid.IsSet() {
		tenantID := tid.ToUint64()
		ctcfg.ID = &tenantID
	}
	ctcfg.IfNotExists = n.ifNotExists
	_, err = params.p.createTenantInternal(params.ctx, ctcfg)
	return err
}

func (n *createTenantNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *createTenantNode) Values() tree.Datums            { return tree.Datums{} }
func (n *createTenantNode) Close(_ context.Context)        {}
