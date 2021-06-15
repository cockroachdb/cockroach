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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
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
	// Validate we have drop privileges.
	err := b.AuthAccessor.CheckPrivilege(ctx, view, privilege.DROP)
	if err != nil {
		panic(err)
	}
	// Create a node for the view we are going to drop
	viewNode := &scpb.View{
		TableID:      view.GetID(),
		DependedOnBy: make([]descpb.ID, 0, len(view.GetDependedOnBy())),
		DependsOn:    make([]descpb.ID, 0, len(view.TableDesc().DependsOn)),
	}
	// Add dependencies in a order manner for reliable
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
	// Only add the node if it wasn't added to avoid cycles.
	if exists, _ := b.checkIfNodeExists(scpb.Target_DROP, viewNode); exists {
		return
	}
	b.addNode(scpb.Target_DROP,
		viewNode)
	// Remove any type back refs.
	b.removeTypeBackRefDeps(ctx, view)
	// Drop any dependent views next.
	err = view.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
		dependentDesc, err := b.Descs.GetImmutableTableByID(ctx, b.EvalCtx.Txn, dep.ID, tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			return err
		}
		err = b.AuthAccessor.CheckPrivilege(ctx, dependentDesc, privilege.DROP)
		if err != nil {
			return err
		}
		if behavior != tree.DropCascade {
			name, err := b.Res.GetQualifiedTableNameByID(ctx, int64(view.GetID()), tree.ResolveRequireViewDesc)
			if err != nil {
				return err
			}

			depViewName, err := b.Res.GetQualifiedTableNameByID(ctx, int64(dep.ID), tree.ResolveRequireViewDesc)
			if err != nil {
				return err
			}
			panic(errors.WithHintf(
				sqlerrors.NewDependentObjectErrorf("cannot drop view %q because view %q depends on it",
					name, depViewName.FQString()),
				"you can drop %s instead.", depViewName.FQString()))
		}
		b.maybeDropViewAndDependents(ctx, dependentDesc, behavior)
		return nil
	})
	if err != nil {
		panic(err)
	}

}

// dropView builds targets and transforms the provided schema change nodes
// accordingly, given an DROP VIEW statement.
func (b *buildContext) dropView(ctx context.Context, n *tree.DropView) {
	// Find the view first.
	for _, name := range n.Names {
		_, view, err := resolver.ResolveExistingTableObject(ctx, b.Res, &name,
			tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			if errors.Is(err, catalog.ErrDescriptorNotFound) && n.IfExists {
				continue
			}
			panic(err)
		}
		if view == nil {
			panic(errors.AssertionFailedf("Unable to resolve view %s",
				name.FQString()))
		}
		// Check if its materialized or not correctly.
		isMaterialized := view.MaterializedView()
		if isMaterialized && !n.IsMaterialized {
			panic(errors.WithHint(pgerror.Newf(pgcode.WrongObjectType, "%q is a materialized view", view.GetName()),
				"use the corresponding MATERIALIZED VIEW command"))
		} else if !isMaterialized && n.IsMaterialized {
			panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a materialized view", view.GetName()))
		}
		b.maybeDropViewAndDependents(ctx, view, n.DropBehavior)
	}
}
