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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
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
	onErrPanic(b.AuthorizationAccessor().CheckPrivilege(ctx, view, privilege.DROP))
	// Convert the table descriptor into elements.
	b.decomposeTableDescToElements(ctx, view, scpb.Target_DROP)
	// Any elements added below will be children of this view.
	lastSourceID := b.setSourceElementID(b.newSourceElementID())
	// Go over the dependencies and generate drop targets
	// for them. In our case they should only be views.
	scpb.ForEachRelationDependedOnBy(b.output,
		func(dep *scpb.RelationDependedOnBy) error {
			if dep.TableID != view.GetID() {
				return nil
			}
			dependentDesc := mustReadTable(ctx, b.Dependencies, dep.DependedOnBy)
			if !dependentDesc.IsView() {
				panic(errors.AssertionFailedf("descriptor :%s is not a view", dependentDesc.GetName()))
			}
			if behavior != tree.DropCascade {
				name, err := b.Dependencies.CatalogReader().GetQualifiedTableNameByID(ctx, int64(view.GetID()), tree.ResolveRequireViewDesc)
				onErrPanic(err)

				depViewName, err := b.Dependencies.CatalogReader().GetQualifiedTableNameByID(ctx, int64(dep.DependedOnBy), tree.ResolveRequireViewDesc)
				onErrPanic(err)
				panic(errors.WithHintf(
					sqlerrors.NewDependentObjectErrorf("cannot drop view %q because view %q depends on it",
						name, depViewName.FQString()),
					"you can drop %s instead.", depViewName.FQString()))
			}
			// Decompose and recursively attempt to drop
			b.maybeDropViewAndDependents(ctx, dependentDesc, behavior)
			return nil
		})
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
