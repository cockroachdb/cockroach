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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type showTenantNode struct {
	columns colinfo.ResultColumns
	name    roachpb.TenantName
	info    *descpb.TenantInfo
	done    bool
}

func (p *planner) ShowTenant(_ context.Context, n *tree.ShowTenant) (planNode, error) {
	return &showTenantNode{
		name:    roachpb.TenantName(n.Name),
		columns: colinfo.TenantColumns,
	}, nil
}

func (n *showTenantNode) startExec(params runParams) error {
	info, err := params.p.GetTenantInfo(params.ctx, n.name)
	if err != nil {
		return err
	}
	n.info = info
	return nil
}

func (n *showTenantNode) Next(_ runParams) (bool, error) {
	if n.done {
		return false, nil
	}
	n.done = true
	return true, nil
}
func (n *showTenantNode) Values() tree.Datums {
	return tree.Datums{
		tree.NewDInt(tree.DInt(n.info.ID)),
		tree.NewDString(string(n.info.Name)),
		tree.NewDString(n.info.State.String()),
	}
}
func (n *showTenantNode) Close(_ context.Context) {}
