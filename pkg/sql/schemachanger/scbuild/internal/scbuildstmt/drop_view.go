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
	type viewDropCtx struct {
		view     catalog.TableDescriptor
		buildCtx BuildCtx
	}
	views := make([]viewDropCtx, 0, len(n.Names))
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
		newCtx := dropViewBasic(b, view)
		views = append(views, viewDropCtx{
			view:     view,
			buildCtx: newCtx,
		})
		b.IncrementSubWorkID()
	}
	// Validate if the dependent objects need to be dropped, if necessary
	// this will cascade.
	for _, viewCtx := range views {
		dropViewDependents(viewCtx.buildCtx, viewCtx.view, n.DropBehavior)
	}
}

// dropTable drops a view and its dependencies, if the cascade behavior is not
// specified the appropriate error will be generated.
func dropView(b BuildCtx, view catalog.TableDescriptor, behavior tree.DropBehavior) {
	dropViewDependents(dropViewBasic(b, view), view, behavior)
}

// dropTableBasic drops the view and does not validate or deal with
// any objects that may need to be dealt with when cascading. The BuildCtx for
// cascaded drops is returned.
func dropViewBasic(b BuildCtx, view catalog.TableDescriptor) BuildCtx {
	decomposeTableDescToElements(b, view, scpb.Status_ABSENT)
	return b.WithNewSourceElementID()
}

// dropTableDependents drops any dependent objects for the view if possible,
// if a cascade is not specified an appropriate error is returned.
func dropViewDependents(b BuildCtx, view catalog.TableDescriptor, behavior tree.DropBehavior) {
	// Go over the dependencies and generate drop targets
	// for them. In our case they should only be views.
	scpb.ForEachRelationDependedOnBy(b, func(_, _ scpb.Status, dep *scpb.RelationDependedOnBy) {
		if dep.TableID != view.GetID() {
			return
		}
		dependentDesc := b.MustReadTable(dep.DependedOnBy)
		if !dependentDesc.IsView() {
			panic(errors.AssertionFailedf("descriptor :%s is not a view", dependentDesc.GetName()))
		}
		if behavior != tree.DropCascade &&
			!checkIfDescOrElementAreDropped(b, dep.DependedOnBy) {
			name, err := b.CatalogReader().GetQualifiedTableNameByID(b, int64(view.GetID()), tree.ResolveRequireViewDesc)
			onErrPanic(err)

			depViewName, err := b.CatalogReader().GetQualifiedTableNameByID(b, int64(dep.DependedOnBy), tree.ResolveRequireViewDesc)
			onErrPanic(err)
			if dependentDesc.GetParentID() != view.GetParentID() {
				panic(errors.WithHintf(sqlerrors.NewDependentObjectErrorf("cannot drop relation %q because view %q depends on it",
					name.Object(), depViewName.FQString()),
					"you can drop %s instead.", depViewName.Object()))
			} else {
				panic(sqlerrors.NewDependentObjectErrorf("cannot drop relation %q because view %q depends on it",
					name.Object(), depViewName.Object()))
			}
		}
		// Decompose and recursively attempt to drop.
		dropView(b, dependentDesc, behavior)
	})
}
