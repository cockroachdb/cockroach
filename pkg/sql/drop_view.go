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

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type dropViewNode struct {
	n  *tree.DropView
	td []toDelete
}

// DropView drops a view.
// Privileges: DROP on view.
//   Notes: postgres allows only the view owner to DROP a view.
//          mysql requires the DROP privilege on the view.
func (p *planner) DropView(ctx context.Context, n *tree.DropView) (planNode, error) {
	td := make([]toDelete, 0, len(n.Names))
	for i := range n.Names {
		tn := &n.Names[i]
		droppedDesc, err := p.prepareDrop(ctx, tn, !n.IfExists, resolver.ResolveRequireViewDesc)
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			// IfExists specified and the view did not exist.
			continue
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
			if err := p.canRemoveDependentView(ctx, droppedDesc, ref, n.DropBehavior); err != nil {
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
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("view"))

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
		if err := MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
			ctx,
			params.p.txn,
			EventLogDropView,
			int32(droppedDesc.ID),
			int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
			struct {
				ViewName            string
				Statement           string
				User                string
				CascadeDroppedViews []string
			}{toDel.tn.FQString(), n.n.String(), params.SessionData().User, cascadeDroppedViews},
		); err != nil {
			return err
		}
	}
	return nil
}

func (*dropViewNode) Next(runParams) (bool, error) { return false, nil }
func (*dropViewNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropViewNode) Close(context.Context)        {}

func descInSlice(descID sqlbase.ID, td []toDelete) bool {
	for _, toDel := range td {
		if descID == toDel.desc.ID {
			return true
		}
	}
	return false
}

func (p *planner) canRemoveDependentView(
	ctx context.Context,
	from *sqlbase.MutableTableDescriptor,
	ref sqlbase.TableDescriptor_Reference,
	behavior tree.DropBehavior,
) error {
	return p.canRemoveDependentViewGeneric(ctx, from.TypeName(), from.Name, from.ParentID, ref, behavior)
}

func (p *planner) canRemoveDependentViewGeneric(
	ctx context.Context,
	typeName string,
	objName string,
	parentID sqlbase.ID,
	ref sqlbase.TableDescriptor_Reference,
	behavior tree.DropBehavior,
) error {
	viewDesc, err := p.getViewDescForCascade(ctx, typeName, objName, parentID, ref.ID, behavior)
	if err != nil {
		return err
	}
	if err := p.CheckPrivilege(ctx, viewDesc, privilege.DROP); err != nil {
		return err
	}
	// If this view is depended on by other views, we have to check them as well.
	for _, ref := range viewDesc.DependedOnBy {
		if err := p.canRemoveDependentView(ctx, viewDesc, ref, behavior); err != nil {
			return err
		}
	}
	return nil
}

// Drops the view and any additional views that depend on it.
// Returns the names of any additional views that were also dropped
// due to `cascade` behavior.
func (p *planner) removeDependentView(
	ctx context.Context, tableDesc, viewDesc *sqlbase.MutableTableDescriptor, jobDesc string,
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
	viewDesc *sqlbase.MutableTableDescriptor,
	queueJob bool,
	jobDesc string,
	behavior tree.DropBehavior,
) ([]string, error) {
	var cascadeDroppedViews []string

	// Remove back-references from the tables/views this view depends on.
	for _, depID := range viewDesc.DependsOn {
		dependencyDesc, err := p.Tables().GetMutableTableVersionByID(ctx, depID, p.txn)
		if err != nil {
			return cascadeDroppedViews,
				errors.Errorf("error resolving dependency relation ID %d: %v", depID, err)
		}
		// The dependency is also being deleted, so we don't have to remove the
		// references.
		if dependencyDesc.Dropped() {
			continue
		}
		dependencyDesc.DependedOnBy = removeMatchingReferences(dependencyDesc.DependedOnBy, viewDesc.ID)
		// TODO (lucy): Have more consistent/informative names for dependent jobs.
		if err := p.writeSchemaChange(
			ctx, dependencyDesc, sqlbase.InvalidMutationID, "removing references for view",
		); err != nil {
			return cascadeDroppedViews, err
		}
	}
	viewDesc.DependsOn = nil

	if behavior == tree.DropCascade {
		for _, ref := range viewDesc.DependedOnBy {
			dependentDesc, err := p.getViewDescForCascade(
				ctx, viewDesc.TypeName(), viewDesc.Name, viewDesc.ParentID, ref.ID, behavior,
			)
			if err != nil {
				return cascadeDroppedViews, err
			}
			// TODO (lucy): Have more consistent/informative names for dependent jobs.
			cascadedViews, err := p.dropViewImpl(ctx, dependentDesc, queueJob, "dropping dependent view", behavior)
			if err != nil {
				return cascadeDroppedViews, err
			}
			cascadeDroppedViews = append(cascadeDroppedViews, cascadedViews...)
			cascadeDroppedViews = append(cascadeDroppedViews, dependentDesc.Name)
		}
	}

	if err := p.initiateDropTable(ctx, viewDesc, queueJob, jobDesc, true /* drainName */); err != nil {
		return cascadeDroppedViews, err
	}

	return cascadeDroppedViews, nil
}

func (p *planner) getViewDescForCascade(
	ctx context.Context,
	typeName string,
	objName string,
	parentID, viewID sqlbase.ID,
	behavior tree.DropBehavior,
) (*sqlbase.MutableTableDescriptor, error) {
	viewDesc, err := p.Tables().GetMutableTableVersionByID(ctx, viewID, p.txn)
	if err != nil {
		log.Warningf(ctx, "unable to retrieve descriptor for view %d: %v", viewID, err)
		return nil, errors.Wrapf(err, "error resolving dependent view ID %d", viewID)
	}
	if behavior != tree.DropCascade {
		viewName := viewDesc.Name
		if viewDesc.ParentID != parentID {
			var err error
			viewName, err = p.getQualifiedTableName(ctx, viewDesc.TableDesc())
			if err != nil {
				log.Warningf(ctx, "unable to retrieve qualified name of view %d: %v", viewID, err)
				return nil, sqlbase.NewDependentObjectErrorf(
					"cannot drop %s %q because a view depends on it", typeName, objName)
			}
		}
		return nil, errors.WithHintf(
			sqlbase.NewDependentObjectErrorf("cannot drop %s %q because view %q depends on it",
				typeName, objName, viewName),
			"you can drop %s instead.", viewName)
	}
	return viewDesc, nil
}
