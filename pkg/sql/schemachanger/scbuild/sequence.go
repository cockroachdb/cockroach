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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
)

// dropSequenceDesc builds targets and transformations using a descriptor.
func (b *buildContext) dropSequenceDesc(
	ctx context.Context, seq catalog.TableDescriptor, cascade tree.DropBehavior,
) {
	onErrPanic(b.Dependencies.AuthorizationAccessor().CheckPrivilege(ctx, seq, privilege.DROP))
	// Any elements added below will be children of this view.
	lastSourceID := b.setSourceElementID(b.newSourceElementID())
	// Add a node to drop the sequence
	b.decomposeTableDescToElements(ctx, seq, scpb.Target_DROP)
	// Check if there are dependencies.
	scpb.ForEachRelationDependedOnBy(b.output, func(dep *scpb.RelationDependedOnBy) error {
		if dep.TableID != seq.GetID() {
			return nil
		}
		if cascade != tree.DropCascade {
			return pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"cannot drop sequence %s because other objects depend on it",
				seq.GetName(),
			)
		}
		desc := mustReadTable(ctx, b.Dependencies, dep.TableID)
		for _, col := range desc.PublicColumns() {
			if dep.ColumnID != descpb.ColumnID(descpb.InvalidID) && col.GetID() != dep.ColumnID {
				continue
			}
			// Convert the default expression into elements.
			b.decomposeDefaultExprToElements(desc, col, scpb.Target_DROP)
		}
		return nil
	})
	b.setSourceElementID(lastSourceID)
}

// dropSequence builds targets and transforms the provided schema change nodes
// accordingly, given an DROP SEQUENCE statement.
func (b *buildContext) dropSequence(ctx context.Context, n *tree.DropSequence) {
	// Find the sequence first.
	for _, name := range n.Names {
		_, table := b.CatalogReader().MayResolveTable(ctx, *name.ToUnresolvedObjectName())
		if table == nil {
			if n.IfExists {
				continue
			}
			panic(sqlerrors.NewUndefinedRelationError(&name))
		}
		if !table.IsSequence() {
			panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a sequence", table.GetName()))
		}
		onErrPanic(b.AuthorizationAccessor().CheckPrivilege(ctx, table, privilege.DROP))
		b.dropSequenceDesc(ctx, table, n.DropBehavior)
		b.incrementSubWorkID()
	}
}
