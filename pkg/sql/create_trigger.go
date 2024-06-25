// Copyright 2024 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type createTriggerNode struct {
	n         *tree.CreateTrigger
	tableDesc *tabledesc.Mutable
}

// CreateTrigger creates an index.
// Privileges: CREATE on table.
//
//	notes: postgres requires CREATE on the table.
//	       mysql requires INDEX on the table.
func (p *planner) CreateTrigger(ctx context.Context, n *tree.CreateTrigger) (planNode, error) {
	_, tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &n.Table, true /*required*/, tree.ResolveRequireTableOrViewDesc,
	)
	if err != nil {
		return nil, err
	}

	if tableDesc.IsView() {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "%q is not a table or materialized view", tableDesc.Name)
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	// Disallow schema changes if this table's schema is locked.
	if err := checkTableSchemaUnlocked(tableDesc); err != nil {
		return nil, err
	}

	return &createTriggerNode{tableDesc: tableDesc, n: n}, nil
}

func (n *createTriggerNode) startExec(params runParams) error {
	n.tableDesc.TableDesc().Triggers = append(n.tableDesc.TableDesc().Triggers, n.n.Func.String())

	if err := validateDescriptor(params.ctx, params.p, n.tableDesc); err != nil {
		return err
	}

	if err := params.p.writeSchemaChange(
		params.ctx, n.tableDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	return nil
}

func (*createTriggerNode) Next(runParams) (bool, error) { return false, nil }
func (*createTriggerNode) Values() tree.Datums          { return tree.Datums{} }
func (*createTriggerNode) Close(context.Context)        {}
