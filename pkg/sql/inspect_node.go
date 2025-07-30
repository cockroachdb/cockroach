// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

type inspectNode struct {
	zeroInputPlanNode

	n *tree.Inspect
}

// Inspect checks the database.
// Privileges: superuser.
func (p *planner) Inspect(ctx context.Context, n *tree.Inspect) (planNode, error) {
	if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.ALL); err != nil {
		return nil, err
	}

	return &inspectNode{n: n}, nil
}

func (n *inspectNode) startExec(params runParams) error {
	switch n.n.Typ {
	case tree.InspectTable:
		if !params.p.extendedEvalCtx.SessionData().EnableInspectCommand {
			return unimplemented.New("INSPECT TABLE", "INSPECT TABLE is not enabled by enable_inspect_command")
		}
	case tree.InspectDatabase:
		if !params.p.extendedEvalCtx.SessionData().EnableInspectCommand {
			return unimplemented.New("INSPECT DATABASE", "INSPECT DATABASE is not enabled by enable_inspect_command")
		}
	default:
		return errors.AssertionFailedf("unexpected INSPECT type received, got: %v", n.n.Typ)
	}

	return nil
}

func (n *inspectNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (n *inspectNode) Values() tree.Datums {
	return nil
}

func (n *inspectNode) Close(ctx context.Context) {
}
