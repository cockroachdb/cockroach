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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type alterTableSetEphemeralDataNode struct {
	tableDesc *tabledesc.Mutable
	n         *tree.AlterTableSetEphemeralData
}

func (n *alterTableSetEphemeralDataNode) startExec(params runParams) error {
	fmt.Println("coming out here")
	ctx := params.ctx
	p := params.p
	tableDesc := n.tableDesc
	// If the table descriptor being changed has the same value for the
	// `ephemeral` flag, do a no-op.
	if tableDesc.IsEphemeral() == n.n.IsEphemeralData {
		return nil
	}

	tableDesc.SetEphemeralData(n.n.IsEphemeralData)
	b := p.txn.NewBatch()

	if err := p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	tableName, err := p.getQualifiedTableName(ctx, tableDesc)
	if err != nil {
		return err
	}

	return p.logEvent(ctx,
		tableDesc.GetID(),
		&eventpb.SetEphemeral{
			CommonEventDetails:    eventpb.CommonEventDetails{},
			CommonSQLEventDetails: eventpb.CommonSQLEventDetails{},
			DescriptorName:        tableName.FQString(),
			Ephemeral:             n.n.IsEphemeralData,
		})
}

func (n *alterTableSetEphemeralDataNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTableSetEphemeralDataNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterTableSetEphemeralDataNode) Close(context.Context)        {}

func (p *planner) AlterTableSetEphemeralData(
	ctx context.Context, n *tree.AlterTableSetEphemeralData,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER TABLE/VIEW/SEQUENCE SET SCHEMA",
	); err != nil {
		return nil, err
	}

	tn := n.Name.ToTableName()
	requiredTableKind := tree.ResolveAnyTableKind
	_, tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &tn, true /* required */, requiredTableKind)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		// Noop.
		return newZeroNode(nil /* columns */), nil
	}

	if tableDesc.Temporary {
		return nil, pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot set data in a temporary table to be ephemeral")
	}

	// TODO(adityamaru): Do we need to check any privileges?

	// Check that the table does not have any incoming FK references. During a
	// backup, the rows of a table with ephemeral data will not be backed up, and
	// could result in a violation of FK constraints on restore. To prevent this,
	// we only allow a table with no incoming FK references to be marked as
	// ephemeral.
	//
	// TODO(adityamaru): Maybe add a link to the docs pointing users to drop their
	// FKs.
	if len(tableDesc.InboundFKs) != 0 {
		return nil, errors.New("cannot set data in a table with inbound foreign key constraints to be ephemeral")
	}

	return &alterTableSetEphemeralDataNode{tableDesc: tableDesc, n: n}, nil
}
