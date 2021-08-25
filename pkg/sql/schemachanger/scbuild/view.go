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
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
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
	if err := b.AuthAccessor.CheckPrivilege(ctx, view, privilege.DROP); err != nil {
		panic(err)
	}
	// Convert the table descriptor into elements.
	b.decomposeTableDescToElements(ctx, view, scpb.Target_DROP)
	// Go over the dependencies and generate drop targets
	// for them. In our case they should only be views.
	err := b.forEachNodeOfType(scpb.Target_DROP, reflect.TypeOf((*scpb.RelationDependedOnBy)(nil)),
		func(element scpb.Element) error {
			dep := element.(*scpb.RelationDependedOnBy)
			if dep.TableID != view.GetID() {
				return nil
			}
			dependentDesc, err := b.Descs.GetImmutableTableByID(ctx, b.EvalCtx.Txn,
				dep.DependedOnBy, tree.ObjectLookupFlagsWithRequired())
			if err != nil {
				return err
			}
			if !dependentDesc.IsView() {
				panic(errors.AssertionFailedf("descriptor :%s is not a view", dependentDesc.GetName()))
			}
			if behavior != tree.DropCascade {
				name, err := b.Res.GetQualifiedTableNameByID(ctx, int64(view.GetID()), tree.ResolveRequireViewDesc)
				if err != nil {
					return err
				}

				depViewName, err := b.Res.GetQualifiedTableNameByID(ctx, int64(dep.DependedOnBy), tree.ResolveRequireViewDesc)
				if err != nil {
					return err
				}
				panic(errors.WithHintf(
					sqlerrors.NewDependentObjectErrorf("cannot drop view %q because view %q depends on it",
						name, depViewName.FQString()),
					"you can drop %s instead.", depViewName.FQString()))
			}
			// Decompose and recursively attempt to drop
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
