// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type showInspectErrorsNode struct {
	zeroInputPlanNode

	n *tree.ShowInspectErrors
}

// ShowInspectErrors shows records from INSPECT consistency validation.
// Privileges: INSPECT.
func (p *planner) ShowInspectErrors(
	ctx context.Context, n *tree.ShowInspectErrors,
) (planNode, error) {
	if n.TableName != nil {
		_, tableDesc, err := p.ResolveMutableTableDescriptor(ctx, n.TableName, true /* required */, tree.ResolveRequireTableDesc)
		if err != nil {
			return nil, err
		}

		if err := p.CheckPrivilege(ctx, tableDesc, privilege.INSPECT); err != nil {
			return nil, err
		}
	}

	if n.JobID != nil {
		// TODO: implement job ID privilege check
	}

	// TODO: if just `SHOW INSPECT ERRORS`

	return &showInspectErrorsNode{n: n}, nil
}

func (n *showInspectErrorsNode) startExec(params runParams) error {
	return nil
}

func (n *showInspectErrorsNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (n *showInspectErrorsNode) Values() tree.Datums {
	return nil
}

func (n *showInspectErrorsNode) Close(ctx context.Context) {
}
