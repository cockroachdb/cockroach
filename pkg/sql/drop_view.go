// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type dropViewNode struct {
	zeroInputPlanNode
	n  *tree.DropView
	td []toDelete
}

// DropView drops a view.
// Privileges: DROP on view.
//
//	Notes: postgres allows only the view owner to DROP a view.
//	       mysql requires the DROP privilege on the view.
func (p *planner) DropView(ctx context.Context, n *tree.DropView) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"DROP VIEW",
	); err != nil {
		return nil, err
	}

	td := make([]toDelete, 0, len(n.Names))
	for i := range n.Names {
		tn := &n.Names[i]
		droppedDesc, err := p.prepareDrop(ctx, tn, !n.IfExists, tree.ResolveRequireViewDesc)
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			// IfExists specified and the view did not exist.
			continue
		}
		if err := checkViewMatchesMaterialized(droppedDesc, true /* requireView */, n.IsMaterialized); err != nil {
			return nil, err
		}

		td = append(td, toDelete{tn, droppedDesc})
	}

	// Ensure this view isn't depended on by any other views, or that if it is
	// then `cascade` was specified or it was also explicitly specified in the
	// DROP VIEW command.
	for _, toDel := range td {
		droppedDesc := toDel.desc
		for _, ref := range droppedDesc.DependedOnBy {
			// Don't verify that we can remove a dependent view if that dependent
			// view was explicitly specified in the DROP VIEW command.
			if descInSlice(ref.ID, td) {
				continue
			}
			if err := p.canRemoveDependentFromTable(ctx, droppedDesc, ref, n.DropBehavior); err != nil {
				return nil, err
			}
		}
	}

	if len(td) == 0 {
		return newZeroNode(nil /* columns */), nil
	}
	return &dropViewNode{n: n, td: td}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because DROP VIEW performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *dropViewNode) ReadingOwnWrites() {}

func (n *dropViewNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter(
		tree.GetTableType(false /* isSequence */, true /* isView */, n.n.IsMaterialized),
	))

	ctx := params.ctx
	for _, toDel := range n.td {
		droppedDesc := toDel.desc
		if droppedDesc == nil {
			continue
		}

		cascadeDroppedViews, err := params.p.dropViewImpl(
			ctx, droppedDesc, true /* queueJob */, tree.AsStringWithFQNames(n.n, params.Ann()), n.n.DropBehavior,
		)
		if err != nil {
			return err
		}
		// Log a Drop View event for this table. This is an auditable log event
		// and is recorded in the same transaction as the table descriptor
		// update.
		if err := params.p.logEvent(ctx,
			droppedDesc.ID,
			&eventpb.DropView{
				ViewName:            toDel.tn.FQString(),
				CascadeDroppedViews: cascadeDroppedViews}); err != nil {
			return err
		}
	}
	return nil
}

func (*dropViewNode) Next(runParams) (bool, error) { return false, nil }
func (*dropViewNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropViewNode) Close(context.Context)        {}

func descInSlice(descID descpb.ID, td []toDelete) bool {
	for _, toDel := range td {
		if descID == toDel.desc.ID {
			return true
		}
	}
	return false
}

func (p *planner) canRemoveDependent(
	ctx context.Context,
	typeName string,
	objName string,
	parentID descpb.ID,
	ref descpb.TableDescriptor_Reference,
	behavior tree.DropBehavior,
) error {
	desc, err := p.Descriptors().MutableByID(p.txn).Desc(ctx, ref.ID)
	if err != nil {
		return err
	}

	switch t := desc.(type) {
	case *tabledesc.Mutable:
		return p.canRemoveDependentViewGeneric(ctx, typeName, objName, parentID, t, behavior)
	case *funcdesc.Mutable:
		return p.canRemoveDependentFunctionGeneric(ctx, typeName, objName, t, behavior)
	default:
		return errors.AssertionFailedf(
			"unexpected dependent %s %s on %s %s",
			desc.DescriptorType(), desc.GetName(), typeName, objName,
		)
	}
}

func (p *planner) canRemoveDependentFromTable(
	ctx context.Context,
	from *tabledesc.Mutable,
	ref descpb.TableDescriptor_Reference,
	behavior tree.DropBehavior,
) error {
	if p.trackDependency == nil {
		p.trackDependency = make(map[catid.DescID]bool)
	}
	if p.trackDependency[ref.ID] {
		// This table's dependencies are already tracked.
		return nil
	}
	p.trackDependency[ref.ID] = true
	defer func() {
		p.trackDependency[ref.ID] = false
	}()

	return p.canRemoveDependent(ctx, string(from.DescriptorType()), from.Name, from.ParentID, ref, behavior)
}

func (p *planner) canRemoveDependentViewGeneric(
	ctx context.Context,
	typeName string,
	objName string,
	parentID descpb.ID,
	viewDesc *tabledesc.Mutable,
	behavior tree.DropBehavior,
) error {
	if behavior != tree.DropCascade {
		return p.dependentViewError(ctx, typeName, objName, parentID, viewDesc, "drop")
	}

	if err := p.CheckPrivilege(ctx, viewDesc, privilege.DROP); err != nil {
		return err
	}
	// If this view is depended on by other views, we have to check them as well.
	for _, ref := range viewDesc.DependedOnBy {
		if err := p.canRemoveDependentFromTable(ctx, viewDesc, ref, behavior); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) canRemoveDependentFunctionGeneric(
	ctx context.Context,
	typeName string,
	objName string,
	fnDesc *funcdesc.Mutable,
	behavior tree.DropBehavior,
) error {
	if behavior != tree.DropCascade {
		return p.dependentFunctionError(typeName, objName, fnDesc, "drop")
	}
	// TODO(chengxiong): check backreference dependents for drop cascade. This is
	// needed when we start allowing references on UDFs.
	return p.canDropFunction(ctx, fnDesc)
}

// Drops the view and any additional views that depend on it.
// Returns the names of any additional views that were also dropped
// due to `cascade` behavior.
func (p *planner) removeDependentView(
	ctx context.Context, tableDesc, viewDesc *tabledesc.Mutable, jobDesc string,
) ([]string, error) {
	// In the table whose index is being removed, filter out all back-references
	// that refer to the view that's being removed.
	tableDesc.DependedOnBy = removeMatchingReferences(tableDesc.DependedOnBy, viewDesc.ID)
	// Then proceed to actually drop the view and log an event for it.
	return p.dropViewImpl(ctx, viewDesc, true /* queueJob */, jobDesc, tree.DropCascade)
}

// dropViewImpl does the work of dropping a view (and views that depend on it
// if `cascade is specified`). Returns the names of any additional views that
// were also dropped due to `cascade` behavior.
func (p *planner) dropViewImpl(
	ctx context.Context,
	viewDesc *tabledesc.Mutable,
	queueJob bool,
	jobDesc string,
	behavior tree.DropBehavior,
) ([]string, error) {
	var cascadeDroppedViews []string

	// Exit early with an error if the table is undergoing a declarative schema
	// change, before we try to get job IDs and update job statuses later. See
	// createOrUpdateSchemaChangeJob.
	if catalog.HasConcurrentDeclarativeSchemaChange(viewDesc) {
		return nil, scerrors.ConcurrentSchemaChangeError(viewDesc)
	}
	// Remove back-references from the tables/views this view depends on.
	dependedOn := append([]descpb.ID(nil), viewDesc.DependsOn...)
	for _, depID := range dependedOn {
		dependencyDesc, err := p.Descriptors().MutableByID(p.txn).Table(ctx, depID)
		if err != nil {
			return cascadeDroppedViews,
				errors.Wrapf(err, "error resolving dependency relation ID %d", depID)
		}
		// The dependency is also being deleted, so we don't have to remove the
		// references.
		if dependencyDesc.Dropped() {
			continue
		}
		dependencyDesc.DependedOnBy = removeMatchingReferences(dependencyDesc.DependedOnBy, viewDesc.ID)
		if err := p.writeSchemaChange(
			ctx, dependencyDesc, descpb.InvalidMutationID,
			fmt.Sprintf("removing references for view %s from table %s(%d)",
				viewDesc.Name, dependencyDesc.Name, dependencyDesc.ID),
		); err != nil {
			return cascadeDroppedViews, err
		}

	}
	viewDesc.DependsOn = nil

	// Remove back-references from the types this view depends on.
	typesDependedOn := append([]descpb.ID(nil), viewDesc.DependsOnTypes...)
	backRefJobDesc := fmt.Sprintf("updating type back references %v for table %d", typesDependedOn, viewDesc.ID)
	if err := p.removeTypeBackReferences(ctx, typesDependedOn, viewDesc.ID, backRefJobDesc); err != nil {
		return cascadeDroppedViews, err
	}

	if behavior == tree.DropCascade {
		dependedOnBy := append([]descpb.TableDescriptor_Reference(nil), viewDesc.DependedOnBy...)
		for _, ref := range dependedOnBy {
			depDesc, err := p.getDescForCascade(
				ctx, string(viewDesc.DescriptorType()), viewDesc.Name, viewDesc.ParentID, ref.ID, behavior,
			)
			if err != nil {
				return cascadeDroppedViews, err
			}

			if depDesc.Dropped() {
				continue
			}

			switch t := depDesc.(type) {
			case *tabledesc.Mutable:
				qualifiedView, err := p.getQualifiedTableName(ctx, t)
				if err != nil {
					return cascadeDroppedViews, err
				}
				// Check if the dependency was already marked as dropped,
				// while dealing with any earlier dependent views.
				cascadedViews, err := p.dropViewImpl(ctx, t, queueJob, "dropping dependent view", behavior)
				if err != nil {
					return cascadeDroppedViews, err
				}
				cascadeDroppedViews = append(cascadeDroppedViews, cascadedViews...)
				cascadeDroppedViews = append(cascadeDroppedViews, qualifiedView.FQString())
			case *funcdesc.Mutable:
				if err := p.dropFunctionImpl(ctx, t); err != nil {
					return cascadeDroppedViews, err
				}
			}
		}
	}

	// Remove any references to types that this view has.
	if err := p.removeBackRefsFromAllTypesInTable(ctx, viewDesc); err != nil {
		return cascadeDroppedViews, err
	}

	if err := p.initiateDropTable(ctx, viewDesc, queueJob, jobDesc); err != nil {
		return cascadeDroppedViews, err
	}

	return cascadeDroppedViews, nil
}

func (p *planner) getDescForCascade(
	ctx context.Context,
	typeName string,
	objName string,
	parentID, descID descpb.ID,
	behavior tree.DropBehavior,
) (catalog.MutableDescriptor, error) {
	desc, err := p.Descriptors().MutableByID(p.txn).Desc(ctx, descID)
	if err != nil {
		log.Warningf(ctx, "unable to retrieve descriptor for %d: %v", descID, err)
		return nil, errors.Wrapf(err, "error resolving dependent ID %d", descID)
	}
	if behavior != tree.DropCascade {
		return nil, p.dependentError(ctx, typeName, objName, parentID, descID, "drop")
	}
	return desc, nil
}
