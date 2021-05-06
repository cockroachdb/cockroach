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

// dropDependentView goes over the dependencies in a give view and validates
// that they can be dropped for cascade behavior.
func (b *buildContext) dropDependentView(ctx context.Context, table catalog.TableDescriptor) {
	// If this object is already in a dropped state,
	// then nothing needs to be done for this dependency.
	if table.Dropped() {
		return
	}
	err := table.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
		dependentDesc, err := b.Descs.GetImmutableTableByID(ctx, b.EvalCtx.Txn, dep.ID, tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			panic(err)
		}
		err = b.AuthAccessor.CheckPrivilege(ctx, dependentDesc, privilege.DROP)
		if err != nil {
			panic(err)
		}
		b.dropDependentView(ctx, dependentDesc)
		return nil
	})
	if err != nil {
		panic(err)
	}
	viewNode := &scpb.View{
		TableID:      table.GetID(),
		DependedOnBy: make([]descpb.ID, 0, len(table.GetDependedOnBy())),
	}
	for _, dep := range table.GetDependedOnBy() {
		if dep.ID != descpb.InvalidID {
			viewNode.DependedOnBy = append(viewNode.DependedOnBy, dep.ID)
		}
	}

	// Add a node to drop the view
	b.addNodeOnce(scpb.Target_DROP,
		viewNode)

}

// dropView builds targets and transforms the provided schema change nodes
// accordingly, given an DROP VIEW statement.
func (b *buildContext) dropView(ctx context.Context, n *tree.DropView) {
	// Find the view first
	for _, name := range n.Names {
		table, err := resolver.ResolveExistingTableObject(ctx, b.Res, &name,
			tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			if errors.Is(err, catalog.ErrDescriptorNotFound) && n.IfExists {
				continue
			}
			panic(err)
		}
		if table == nil {
			panic(errors.AssertionFailedf("Unable to resolve view %s",
				name.FQString()))
		}
		// Check if its materialized or not correctly.
		isMaterialized := table.MaterializedView()
		if isMaterialized && !n.IsMaterialized {
			panic(errors.WithHint(pgerror.Newf(pgcode.WrongObjectType, "%q is a materialized view", table.GetName()),
				"use the corresponding MATERIALIZED VIEW command"))
		} else if !isMaterialized && n.IsMaterialized {
			panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a materialized view", table.GetName()))
		}
		// Validate we have drop privileges
		err = b.AuthAccessor.CheckPrivilege(ctx, table, privilege.DROP)
		if err != nil {
			panic(err)
		}
		// Check if there are dependencies.
		err = table.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
			desc, err := b.Descs.GetImmutableTableByID(ctx, b.EvalCtx.Txn, dep.ID, tree.ObjectLookupFlagsWithRequired())
			if err != nil {
				panic(err)
			}
			if n.DropBehavior != tree.DropCascade {
				depViewName, err := b.Res.GetQualifiedTableNameByID(ctx, int64(dep.ID), tree.ResolveRequireViewDesc)
				if err != nil {
					panic(err)
				}
				panic(errors.WithHintf(
					sqlerrors.NewDependentObjectErrorf("cannot drop view %q because view %q depends on it",
						name.FQString(), depViewName.FQString()),
					"you can drop %s instead.", depViewName.FQString()))
			}
			b.dropDependentView(ctx, desc)
			return nil
		})
		if err != nil {
			panic(err)
		}

		viewNode := &scpb.View{
			TableID:      table.GetID(),
			DependedOnBy: make([]descpb.ID, 0, len(table.GetDependedOnBy())),
		}
		for _, dep := range table.GetDependedOnBy() {
			if dep.ID != descpb.InvalidID {
				viewNode.DependedOnBy = append(viewNode.DependedOnBy, dep.ID)
			}
		}
		// Add a node to drop the view, assuming
		// a dependency didn't do it already.
		b.addNodeOnce(scpb.Target_DROP,
			viewNode)
	}
}
