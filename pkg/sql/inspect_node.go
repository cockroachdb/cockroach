// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

type inspectNode struct {
	zeroInputPlanNode

	n *tree.Inspect
}

// Inspect checks the database.
// Privileges: INSPECT on table or database.
func (p *planner) Inspect(ctx context.Context, n *tree.Inspect) (planNode, error) {
	var desc catalog.Descriptor
	switch n.Typ {
	case tree.InspectTable:
		tableName := n.Table.ToTableName()
		_, tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &tableName, true /* required */, tree.ResolveRequireTableDesc)
		if err != nil {
			return nil, err
		}

		desc = tableDesc
	case tree.InspectDatabase:
		dbDesc, err := p.Descriptors().ByName(p.txn).Get().Database(ctx, n.Database.ToUnresolvedName().String())
		if err != nil {
			return nil, err
		}

		desc = dbDesc
	default:
		return nil, errors.AssertionFailedf("unexpected INSPECT type received, got: %v", n.Typ)
	}

	if err := p.CheckPrivilege(ctx, desc, privilege.INSPECT); err != nil {
		return nil, err
	}

	return &inspectNode{n: n}, nil
}

func (n *inspectNode) startExec(params runParams) error {
	switch n.n.Typ {
	case tree.InspectTable:
		if !params.p.extendedEvalCtx.SessionData().EnableInspectCommand {
			return unimplemented.New("INSPECT TABLE", "INSPECT TABLE requires the enable_inspect_command setting to be enabled")
		}
	case tree.InspectDatabase:
		if !params.p.extendedEvalCtx.SessionData().EnableInspectCommand {
			return unimplemented.New("INSPECT DATABASE", "INSPECT DATABASE requires the enable_inspect_command setting to be enabled")
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
