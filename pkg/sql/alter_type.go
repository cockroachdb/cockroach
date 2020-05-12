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

type alterTypeNode struct {
	n *tree.AlterType
}

// alterTypeNode implements planNode. We set n here to satisfy the linter.
var _ planNode = &alterTypeNode{n: nil}

func (p *planner) AlterType(ctx context.Context, n *tree.AlterType) (planNode, error) {
	return &alterTypeNode{n: n}, nil
}

func (n *alterTypeNode) startExec(params runParams) error {
	switch t := n.n.Cmd.(type) {
	case *tree.AlterTypeAddValue:
		return unimplemented.NewWithIssue(48670, "ALTER TYPE ADD VALUE unsupported")
	case *tree.AlterTypeRenameValue:
		return unimplemented.NewWithIssue(48697, "ALTER TYPE RENAME VALUE unsupported")
	case *tree.AlterTypeRename:
		return unimplemented.NewWithIssue(48671, "ALTER TYPE RENAME unsupported")
	case *tree.AlterTypeSetSchema:
		return unimplemented.NewWithIssue(48672, "ALTER TYPE SET SCHEMA unsupported")
	default:
		return errors.AssertionFailedf("unknown alter type cmd %s", t)
	}
}

func (n *alterTypeNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterTypeNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterTypeNode) Close(ctx context.Context)           {}
func (n *alterTypeNode) ReadingOwnWrites()                   {}
