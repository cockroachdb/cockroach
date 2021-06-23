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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type alterTableOwnerNode struct {
	owner security.SQLUsername
	desc  *tabledesc.Mutable
	n     *tree.AlterTableOwner
}

// AlterTableOwner sets the owner for a table, view, or sequence.
func (p *planner) AlterTableOwner(ctx context.Context, n *tree.AlterTableOwner) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER TABLE/VIEW/SEQUENCE OWNER",
	); err != nil {
		return nil, err
	}

	tn := n.Name.ToTableName()

	// ALTER TABLE [table] OWNER TO command applies to VIEWS and SEQUENCES
	// so we must resolve any table kind.
	requiredTableKind := tree.ResolveAnyTableKind
	if n.IsView {
		requiredTableKind = tree.ResolveRequireViewDesc
	} else if n.IsSequence {
		requiredTableKind = tree.ResolveRequireSequenceDesc
	}
	_, tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &tn, !n.IfExists, requiredTableKind)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		// Noop.
		return newZeroNode(nil /* columns */), nil
	}

	if err := checkViewMatchesMaterialized(tableDesc, n.IsView, n.IsMaterialized); err != nil {
		return nil, err
	}

	return &alterTableOwnerNode{
		owner: n.Owner,
		desc:  tableDesc,
		n:     n,
	}, nil
}

func (n *alterTableOwnerNode) startExec(params runParams) error {
	telemetry.Inc(n.n.TelemetryCounter())
	ctx := params.ctx
	p := params.p
	tableDesc := n.desc
	newOwner := n.owner
	oldOwner := n.desc.GetPrivileges().Owner()

	if err := p.checkCanAlterTableAndSetNewOwner(ctx, tableDesc, newOwner); err != nil {
		return err
	}

	// If the owner we want to set to is the current owner, do a no-op.
	if newOwner == oldOwner {
		return nil
	}

	if err := p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	return nil
}

// checkCanAlterTableAndSetNewOwner handles privilege checking
// and setting new owner.
func (p *planner) checkCanAlterTableAndSetNewOwner(
	ctx context.Context, desc *tabledesc.Mutable, newOwner security.SQLUsername,
) error {
	if err := p.checkCanAlterToNewOwner(ctx, desc, newOwner); err != nil {
		return err
	}

	// Ensure the new owner has CREATE privilege on the table's schema.
	if err := p.canCreateOnSchema(
		ctx, desc.GetParentSchemaID(), desc.ParentID, newOwner, checkPublicSchema); err != nil {
		return err
	}

	privs := desc.GetPrivileges()
	privs.SetOwner(newOwner)

	tn, err := p.getQualifiedTableName(ctx, desc)
	if err != nil {
		return err
	}

	return p.logEvent(ctx,
		desc.ID,
		&eventpb.AlterTableOwner{
			TableName: tn.FQString(),
			Owner:     newOwner.Normalized(),
		})
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because SET SCHEMA performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *alterTableOwnerNode) ReadingOwnWrites() {}

func (n *alterTableOwnerNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTableOwnerNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterTableOwnerNode) Close(context.Context)        {}
