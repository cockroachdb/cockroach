// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
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

// DropView implements DROP VIEW.
func DropView(b BuildCtx, n *tree.DropView) {
	for _, name := range n.Names {
		_, view := b.ResolveView(name.ToUnresolvedObjectName(), ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.DROP,
		})
		if view == nil {
			continue
		}
		if view.MaterializedView() && !n.IsMaterialized {
			panic(errors.WithHint(pgerror.Newf(pgcode.WrongObjectType, "%q is a materialized view", view.GetName()),
				"use the corresponding MATERIALIZED VIEW command"))
		}
		if !view.MaterializedView() && n.IsMaterialized {
			panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a materialized view", view.GetName()))
		}
		dropView(b, view, n.DropBehavior)
		b.IncrementSubWorkID()
	}
}

func dropView(b BuildCtx, view catalog.TableDescriptor, behavior tree.DropBehavior) {
	var viewNode *scpb.View
	c := b.WithNewSourceElementID()
	// Any elements added with c will be children of this view.
	{
		// Validate we have drop privileges.
		onErrPanic(c.AuthorizationAccessor().CheckPrivilege(b, view, privilege.DROP))
		// Create a node for the view we are going to drop.
		viewNode = &scpb.View{
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
			c.EnqueueDropIfNotExists(&scpb.RelationDependedOnBy{
				TableID:      dep,
				DependedOnBy: viewNode.TableID,
			})
		}
	}
	// Only add the node if it wasn't added to avoid cycles.
	b.EnqueueDropIfNotExists(viewNode)
	// Use the c BuildCtx again.
	{
		// Remove any type back refs.
		removeTypeBackRefDeps(c, view)
		// Drop any dependent views next.
		_ = view.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
			dependentDesc := c.MustReadTable(dep.ID)
			if behavior != tree.DropCascade {
				name, err := c.CatalogReader().GetQualifiedTableNameByID(b, int64(view.GetID()), tree.ResolveRequireViewDesc)
				onErrPanic(err)
				depViewName, err := c.CatalogReader().GetQualifiedTableNameByID(b, int64(dep.ID), tree.ResolveRequireViewDesc)
				onErrPanic(err)
				panic(errors.WithHintf(
					sqlerrors.NewDependentObjectErrorf("cannot drop view %q because view %q depends on it",
						name, depViewName.FQString()),
					"you can drop %s instead.", depViewName.FQString()))
			}
			dropView(c, dependentDesc, behavior)
			return nil
		})
	}
}
