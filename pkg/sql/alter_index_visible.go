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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

type alterIndexVisibleNode struct {
	n         *tree.AlterIndexVisible
	tableDesc *tabledesc.Mutable
	index     catalog.Index
}

func (p *planner) AlterIndexVisible(
	ctx context.Context, n *tree.AlterIndexVisible,
) (planNode, error) {
	return nil, unimplemented.Newf(
		"Not Visible Index",
		"altering an index to visible or not visible is not supported yet")
}

func (n *alterIndexVisibleNode) ReadingOwnWrites() {}

func (n *alterIndexVisibleNode) startExec(params runParams) error {
	return unimplemented.Newf(
		"Not Visible Index",
		"altering an index to visible or not visible is not supported yet")
}
func (n *alterIndexVisibleNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterIndexVisibleNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterIndexVisibleNode) Close(context.Context)        {}
