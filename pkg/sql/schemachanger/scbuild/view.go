// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// maybeDropViewAndDependents goes over the dependencies in a give view
// and validates that they can be dropped for cascade behavior.
func (b *buildContext) maybeDropViewAndDependents(
	ctx context.Context, view catalog.TableDescriptor, behavior tree.DropBehavior,
) {
	// Any elements added below will be children of this view.
	lastSourceID := b.setSourceElementID(b.newSourceElementID())
	// Validate we have drop privileges.
	onErrPanic(b.AuthorizationAccessor().CheckPrivilege(ctx, view, privilege.DROP))
	// Create a node for the view we are going to drop.
	viewNode := &scpb.View{
		TableID:      view.GetID(),
		DependedOnBy: make([]descpb.ID, 0, len(view.GetDependedOnBy())),
		DependsOn:    make([]descpb.ID, 0, len(view.TableDesc().DependsOn)),
	}
	// Add dependencies in an ordered manner for reliable
	// unit testing.
	for _, dep := range view.GetDependedOnBy() {
		viewNode.DependedOnBy = append(viewNode.DependedOnBy, dep.ID)
	}
	viewNode.DependsOn = append(viewNode.DependsOn, view.TableDesc().DependsOn...)
	sort.SliceStable(viewNode.DependsOn, func(i, j int) bool {
		return viewNode.DependsOn[i] < viewNode.DependsOn[j]
	})
	sort.SliceStable(viewNode.DependedOnBy, func(i, j int) bool {
		return viewNode.DependedOnBy[i] < viewNode.DependedOnBy[j]
	})
	// Clean up any back references to the tables.
	for _, dep := range viewNode.DependsOn {
		tableBackRef := &scpb.RelationDependedOnBy{
			TableID:      dep,
			DependedOnBy: viewNode.TableID,
		}
		if exists, _ := b.checkIfNodeExists(scpb.Target_DROP, tableBackRef); !exists {
			b.addNode(scpb.Target_DROP,
				tableBackRef)
		}
	}
	// Only add the node if it wasn't added to avoid cycles.
	if exists, _ := b.checkIfNodeExists(scpb.Target_DROP, viewNode); exists {
		return
	}
	// This views element will be under the original
	// parent ID. lastSourceID should be the new parent
	// we just used.
	lastSourceID = b.setSourceElementID(lastSourceID)
	b.addNode(scpb.Target_DROP,
		viewNode)
	// Revert to our newly allocated ID, last now points to the parent.
	lastSourceID = b.setSourceElementID(lastSourceID)
	// Remove any type back refs.
	b.removeTypeBackRefDeps(ctx, view)
	// Drop any dependent views next.
	_ = view.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
		dependentDesc := mustReadTable(ctx, b, dep.ID)
		onErrPanic(b.AuthorizationAccessor().CheckPrivilege(ctx, dependentDesc, privilege.DROP))
		if behavior != tree.DropCascade {
			name, err := b.CatalogReader().GetQualifiedTableNameByID(ctx, int64(view.GetID()), tree.ResolveRequireViewDesc)
			onErrPanic(err)
			depViewName, err := b.CatalogReader().GetQualifiedTableNameByID(ctx, int64(dep.ID), tree.ResolveRequireViewDesc)
			onErrPanic(err)
			panic(errors.WithHintf(
				sqlerrors.NewDependentObjectErrorf("cannot drop view %q because view %q depends on it",
					name, depViewName.FQString()),
				"you can drop %s instead.", depViewName.FQString()))
		}
		b.maybeDropViewAndDependents(ctx, dependentDesc, behavior)
		return nil
	})
	// Go back to the original parent ID.
	b.setSourceElementID(lastSourceID)
}

// dropView builds targets and transforms the provided schema change nodes
// accordingly, given an DROP VIEW statement.
func (b *buildContext) dropView(ctx context.Context, n *tree.DropView) {
	// Find the view first.
	for _, name := range n.Names {
		_, table := b.CatalogReader().MayResolveTable(ctx, *name.ToUnresolvedObjectName())
		if table == nil {
			if n.IfExists {
				continue
			}
			panic(sqlerrors.NewUndefinedRelationError(&name))
		}
		if !table.IsView() {
			panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a view", table.GetName()))
		}
		if table.MaterializedView() && !n.IsMaterialized {
			panic(errors.WithHint(pgerror.Newf(pgcode.WrongObjectType, "%q is a materialized view", table.GetName()),
				"use the corresponding MATERIALIZED VIEW command"))
		}
		if !table.MaterializedView() && n.IsMaterialized {
			panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a materialized view", table.GetName()))
		}
		onErrPanic(b.AuthorizationAccessor().CheckPrivilege(ctx, table, privilege.DROP))
		b.maybeDropViewAndDependents(ctx, table, n.DropBehavior)
		b.incrementSubWorkID()
	}
}
