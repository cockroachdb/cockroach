// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type alterIndexVisibleNode struct {
	zeroInputPlanNode
	n         *tree.AlterIndexVisible
	tableDesc *tabledesc.Mutable
	index     catalog.Index
}

func (p *planner) AlterIndexVisible(
	ctx context.Context, n *tree.AlterIndexVisible,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER INDEX VISIBILITY",
	); err != nil {
		return nil, err
	}

	// Check if the table actually exists. expandMutableIndexName returns the
	// underlying table.
	_, tableDesc, err := expandMutableIndexName(ctx, p, &n.Index, !n.IfExists /* requireTable */)
	if err != nil {
		// Error if no table is found and IfExists is false.
		return nil, err
	}

	if tableDesc == nil {
		// No error if no table but IfExists is true.
		return newZeroNode(nil /* columns */), nil
	}

	// Check if the index actually exists. MustFindIndexByName returns the first
	// catalog.Index in tableDesc.AllIndexes().
	idx, err := catalog.MustFindIndexByName(tableDesc, string(n.Index.Index))
	if err != nil {
		if n.IfExists {
			// Nothing needed if no index exists and IfExists is true.
			return newZeroNode(nil /* columns */), nil
		}
		// Error if no index exists and IfExists is not specified.
		return nil, pgerror.WithCandidateCode(err, pgcode.UndefinedObject)
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	// Disallow schema changes if this table's schema is locked.
	if err := checkSchemaChangeIsAllowed(tableDesc, n); err != nil {
		return nil, err
	}

	return &alterIndexVisibleNode{n: n, tableDesc: tableDesc, index: idx}, nil
}

func (n *alterIndexVisibleNode) ReadingOwnWrites() {}

func (n *alterIndexVisibleNode) startExec(params runParams) error {
	if n.n.Invisibility.Value != 0.0 && n.index.Primary() {
		return pgerror.Newf(pgcode.FeatureNotSupported, "primary index cannot be invisible")
	}

	// Warn if this invisible index may still be used to enforce constraint check
	// behind the scene.
	if n.n.Invisibility.Value != 0.0 {
		if notVisibleIndexNotice := tabledesc.ValidateNotVisibleIndex(n.index, n.tableDesc); notVisibleIndexNotice != nil {
			params.p.BufferClientNotice(
				params.ctx,
				notVisibleIndexNotice,
			)
		}
	}

	if n.index.GetInvisibility() == n.n.Invisibility.Value {
		// Nothing needed if the index is already what they want.
		return nil
	}

	n.index.IndexDesc().NotVisible = n.n.Invisibility.Value != 0.0
	n.index.IndexDesc().Invisibility = n.n.Invisibility.Value

	if err := validateDescriptor(params.ctx, params.p, n.tableDesc); err != nil {
		return err
	}

	if err := params.p.writeSchemaChange(
		params.ctx, n.tableDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	return params.p.logEvent(params.ctx,
		n.tableDesc.ID,
		&eventpb.AlterIndexVisible{
			TableName:    n.n.Index.Table.FQString(),
			IndexName:    n.index.GetName(),
			NotVisible:   n.n.Invisibility.Value != 0.0,
			Invisibility: n.n.Invisibility.Value,
		})
}
func (n *alterIndexVisibleNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterIndexVisibleNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterIndexVisibleNode) Close(context.Context)        {}
