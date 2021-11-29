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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
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
	// Convert the table descriptor into elements.	b.EnqueueDropIfNotExists(viewNode)
	decomposeTableDescToElements(b, view, scpb.Target_DROP)
	// Use the c BuildCtx again.
	{
		c := b.WithNewSourceElementID()
		// Go over the dependencies and generate drop targets
		// for them. In our case they should only be views.
		scpb.ForEachRelationDependedOnBy(b,
			func(_ scpb.Status,
				_ scpb.Target_Direction,
				dep *scpb.RelationDependedOnBy) {
				if dep.TableID != view.GetID() {
					return
				}
				dependentDesc := c.MustReadTable(dep.DependedOnBy)
				if !dependentDesc.IsView() {
					panic(errors.AssertionFailedf("descriptor :%s is not a view", dependentDesc.GetName()))
				}
				if behavior != tree.DropCascade {
					name, err := b.CatalogReader().GetQualifiedTableNameByID(b.EvalCtx().Context, int64(view.GetID()), tree.ResolveRequireViewDesc)
					onErrPanic(err)

					depViewName, err := b.CatalogReader().GetQualifiedTableNameByID(b.EvalCtx().Context, int64(dep.DependedOnBy), tree.ResolveRequireViewDesc)
					onErrPanic(err)
					panic(errors.WithHintf(
						sqlerrors.NewDependentObjectErrorf("cannot drop view %q because view %q depends on it",
							name, depViewName.FQString()),
						"you can drop %s instead.", depViewName.FQString()))
				}
				// Decompose and recursively attempt to drop
				dropView(b, dependentDesc, behavior)
			})

	}
}
