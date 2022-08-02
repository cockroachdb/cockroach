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
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

type dropFunctionNode struct {
	n *tree.DropFunction
}

// DropFunction drops a function.
func (p *planner) DropFunction(ctx context.Context, n *tree.DropFunction) (planNode, error) {
	return &dropFunctionNode{n: n}, nil
}

func (n *dropFunctionNode) startExec(params runParams) error {
	return unimplemented.NewWithIssue(83235, "drop function not supported")
}

func (n *dropFunctionNode) Next(params runParams) (bool, error) { return false, nil }
func (n *dropFunctionNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *dropFunctionNode) Close(ctx context.Context)           {}
