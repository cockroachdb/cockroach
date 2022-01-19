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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type alterTableSetEphemeralDataNode struct {
	tableDesc *tabledesc.Mutable
	n         *tree.AlterTableSetEphemeralData
}

func (a *alterTableSetEphemeralDataNode) startExec(params runParams) error {
	return nil
}

func (a *alterTableSetEphemeralDataNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (a *alterTableSetEphemeralDataNode) Values() tree.Datums {
	return nil
}

func (a *alterTableSetEphemeralDataNode) Close(ctx context.Context) {
	return
}

func (p *planner) AlterTableSetEphemeralData(
	ctx context.Context, n *tree.AlterTableSetEphemeralData,
) (planNode, error) {
	return &zeroNode{}, nil
}
