// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type dropSequenceNode struct {
	n  *tree.DropSequence
	td []toDelete
}

func (p *planner) DropSequence(ctx context.Context, n *tree.DropSequence) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"DROP SEQUENCE",
	); err != nil {
		return nil, err
	}

	td := make([]toDelete, 0, len(n.Names))
	for i := range n.Names {
		tn := &n.Names[i]
		droppedDesc, err := p.prepareDrop(ctx, tn, !n.IfExists, tree.ResolveRequireSequenceDesc)
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			// IfExists specified and descriptor does not exist.
			continue
		}

		if depErr := p.sequenceDependencyError(ctx, droppedDesc, n.DropBehavior); depErr != nil {
			return nil, depErr
		}

		td = append(td, toDelete{tn, droppedDesc})
	}

	if len(td) == 0 {
		return newZeroNode(nil /* columns */), nil
	}

	return &dropSequenceNode{
		n:  n,
		td: td,
	}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because DROP SEQUENCE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *dropSequenceNode) ReadingOwnWrites() {}

func (n *dropSequenceNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("sequence"))

	ctx := params.ctx
	for _, toDel := range n.td {
		droppedDesc := toDel.desc
		err := params.p.dropSequenceImpl(
			ctx, droppedDesc, true /* queueJob */, tree.AsStringWithFQNames(n.n, params.Ann()), n.n.DropBehavior,
		)
		if err != nil {
			return err
		}
		// Log a Drop Sequence event for this table. This is an auditable log event
		// and is recorded in the same transaction as the table descriptor
		// update.
		if err := params.p.logEvent(params.ctx,
			droppedDesc.ID,
			&eventpb.DropSequence{
				SequenceName: toDel.tn.FQString(),
			}); err != nil {
			return err
		}
	}
	return nil
}

func (*dropSequenceNode) Next(runParams) (bool, error) { return false, nil }
func (*dropSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropSequenceNode) Close(context.Context)        {}

func (p *planner) dropSequenceImpl(
	ctx context.Context,
	seqDesc *tabledesc.Mutable,
	queueJob bool,
	jobDesc string,
	behavior tree.DropBehavior,
) error {
	if err := removeSequenceOwnerIfExists(ctx, p, seqDesc.ID, seqDesc.GetSequenceOpts()); err != nil {
		return err
	}
	if behavior == tree.DropCascade {
		if err := dropDependentOnSequence(ctx, p, seqDesc); err != nil {
			return err
		}
	}
	return p.initiateDropTable(ctx, seqDesc, queueJob, jobDesc, true /* drainName */)
}

// sequenceDependency error returns an error if the given sequence cannot be dropped because
// a table uses it in a DEFAULT expression on one of its columns, or nil if there is no
// such dependency.
func (p *planner) sequenceDependencyError(
	ctx context.Context, droppedDesc *tabledesc.Mutable, behavior tree.DropBehavior,
) error {
	if behavior != tree.DropCascade && len(droppedDesc.DependedOnBy) > 0 {
		return pgerror.Newf(
			pgcode.DependentObjectsStillExist,
			"cannot drop sequence %s because other objects depend on it",
			droppedDesc.Name,
		)
	}
	return nil
}

func (p *planner) canRemoveAllTableOwnedSequences(
	ctx context.Context, desc *tabledesc.Mutable, behavior tree.DropBehavior,
) error {
	for _, col := range desc.PublicColumns() {
		err := p.canRemoveOwnedSequencesImpl(ctx, desc, col, behavior, false /* isColumnDrop */)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) canRemoveAllColumnOwnedSequences(
	ctx context.Context, desc *tabledesc.Mutable, col catalog.Column, behavior tree.DropBehavior,
) error {
	return p.canRemoveOwnedSequencesImpl(ctx, desc, col, behavior, true /* isColumnDrop */)
}

func (p *planner) canRemoveOwnedSequencesImpl(
	ctx context.Context,
	desc *tabledesc.Mutable,
	col catalog.Column,
	behavior tree.DropBehavior,
	isColumnDrop bool,
) error {
	for i := 0; i < col.NumOwnsSequences(); i++ {
		sequenceID := col.GetOwnsSequenceID(i)
		seqDesc, err := p.LookupTableByID(ctx, sequenceID)
		if err != nil {
			// Special case error swallowing for #50711 and #50781, which can cause a
			// column to own sequences that have been dropped/do not exist.
			if errors.Is(err, catalog.ErrDescriptorDropped) ||
				pgerror.GetPGCode(err) == pgcode.UndefinedTable {
				log.Eventf(ctx, "swallowing error ensuring owned sequences can be removed: %s", err.Error())
				continue
			}
			return err
		}

		var firstDep *descpb.TableDescriptor_Reference
		multipleIterationErr := seqDesc.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
			if firstDep != nil {
				return iterutil.StopIteration()
			}
			firstDep = dep
			return nil
		})

		if firstDep == nil {
			// This sequence is not depended on by anything, it's safe to remove.
			continue
		}

		if multipleIterationErr == nil && firstDep.ID == desc.ID {
			// This sequence is depended on only by columns in the table of interest.
			if !isColumnDrop {
				// Either we're dropping the whole table and thereby also anything
				// that might depend on this sequence, making it safe to remove...
				continue
			}
			// ...or we're dropping a column in the table of interest.
			if len(firstDep.ColumnIDs) == 1 && firstDep.ColumnIDs[0] == col.GetID() {
				// The sequence is safe to remove iff it's not depended on by any other
				// columns in the table other than that one.
				continue
			}
		}

		// If cascade is enabled, allow the sequences to be dropped.
		if behavior == tree.DropCascade {
			continue
		}
		// If Cascade is not enabled, and more than 1 columns depend on it, and the
		return pgerror.Newf(
			pgcode.DependentObjectsStillExist,
			"cannot drop table %s because other objects depend on it",
			desc.Name,
		)
	}
	return nil
}

// dropDependentOnSequence drops the default values of any columns that depend on the
// given sequence descriptor being dropped, and if the dependent object
// is a view, it drops the views.
// This is called when the DropBehavior is DropCascade.
func dropDependentOnSequence(ctx context.Context, p *planner, seqDesc *tabledesc.Mutable) error {
	for _, dependent := range seqDesc.DependedOnBy {
		tblDesc, err := p.Descriptors().GetMutableTableByID(ctx, p.txn, dependent.ID,
			tree.ObjectLookupFlags{
				CommonLookupFlags: tree.CommonLookupFlags{
					IncludeOffline: true,
					IncludeDropped: true,
				},
			})
		if err != nil {
			return err
		}

		// If the table that uses the sequence has been dropped already,
		// no need to update, so skip.
		if tblDesc.Dropped() {
			continue
		}

		// If the dependent object is a view, drop the view.
		if tblDesc.IsView() {
			_, err = p.dropViewImpl(ctx, tblDesc, false /* queueJob */, "", tree.DropCascade)
			if err != nil {
				return err
			}
			continue
		}

		// Set of column IDs which will have their default values dropped.
		colsToDropDefault := make(map[descpb.ColumnID]struct{})
		for _, colID := range dependent.ColumnIDs {
			colsToDropDefault[colID] = struct{}{}
		}

		// Iterate over all columns in the table, drop affected columns' default values
		// and update back references.
		for _, column := range tblDesc.PublicColumns() {
			if _, ok := colsToDropDefault[column.GetID()]; ok {
				column.ColumnDesc().DefaultExpr = nil
				if err := p.removeSequenceDependencies(ctx, tblDesc, column); err != nil {
					return err
				}
			}
		}

		jobDesc := fmt.Sprintf(
			"removing default expressions using sequence %q since it is being dropped",
			seqDesc.Name,
		)
		if err := p.writeSchemaChange(
			ctx, tblDesc, descpb.InvalidMutationID, jobDesc,
		); err != nil {
			return err
		}
	}
	return nil
}
