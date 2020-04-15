// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/errors"
)

type dropTypeNode struct {
	n *tree.DropType
}

// Use to satisfy the linter.
var _ planNode = &dropTypeNode{n: nil}

func (p *planner) DropType(ctx context.Context, n *tree.DropType) (planNode, error) {
	return nil, unimplemented.NewWithIssue(27793, "DROP TYPE")
}

func (n *dropTypeNode) startExec(params runParams) error {
	return errors.AssertionFailedf(
		"should not be calling startExec on DROP TYPE node",
	)
}

func (n *dropTypeNode) Next(params runParams) (bool, error) { return false, nil }
func (n *dropTypeNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *dropTypeNode) Close(ctx context.Context)           {}
func (n *dropTypeNode) ReadingOwnWrites()                   {}
