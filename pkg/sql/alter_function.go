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

type alterFunctionOptionsNode struct {
	n *tree.AlterFunctionOptions
}

// AlterFunctionOptions alters a function's options.
func (p *planner) AlterFunctionOptions(
	ctx context.Context, n *tree.AlterFunctionOptions,
) (planNode, error) {
	return &alterFunctionOptionsNode{n: n}, nil
}

func (n *alterFunctionOptionsNode) startExec(params runParams) error {
	return unimplemented.NewWithIssue(85532, "alter function not supported")
}

func (n *alterFunctionOptionsNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionOptionsNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionOptionsNode) Close(ctx context.Context)           {}
