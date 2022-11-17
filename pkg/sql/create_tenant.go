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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type createTenantNode struct {
	name roachpb.TenantName
}

func (p *planner) CreateTenantNode(_ context.Context, n *tree.CreateTenant) (planNode, error) {
	return &createTenantNode{
		name: roachpb.TenantName(n.Name),
	}, nil
}

func (n *createTenantNode) startExec(params runParams) error {
	_, err := params.p.CreateTenant(params.ctx, n.name)
	return err
}

func (n *createTenantNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *createTenantNode) Values() tree.Datums            { return tree.Datums{} }
func (n *createTenantNode) Close(_ context.Context)        {}
