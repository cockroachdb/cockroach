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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// DropSequence implements DROP SEQUENCE.
func DropSequence(b BuildCtx, n *tree.DropSequence) {
	for idx := range n.Names {
		name := &n.Names[idx]
		prefix, seq := b.ResolveSequence(name.ToUnresolvedObjectName(), ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.DROP,
		})
		if seq == nil {
			b.MarkNameAsNonExistent(name)
			continue
		}
		// Mutate the AST to have the fully resolved name from above, which will be
		// used for both event logging and errors.
		name.ObjectNamePrefix = prefix.NamePrefix()
		dropSequence(b, seq, n.DropBehavior)
		b.IncrementSubWorkID()
	}
}

// dropSequence builds targets and transformations using a descriptor.
func dropSequence(b BuildCtx, seq catalog.TableDescriptor, cascade tree.DropBehavior) {
	onErrPanic(b.AuthorizationAccessor().CheckPrivilege(b, seq, privilege.DROP))
	// Add a node to drop the sequence
	decomposeTableDescToElements(b, seq, scpb.Target_DROP)
	// Check if there are dependencies.
	scpb.ForEachRelationDependedOnBy(b, func(_ scpb.Status,
		_ scpb.Target_Direction,
		dep *scpb.RelationDependedOnBy) {
		if dep.TableID != seq.GetID() {
			return
		}
		if cascade != tree.DropCascade &&
			!checkIfDescOrElementAreDropped(b, dep.DependedOnBy) {
			panic(pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"cannot drop sequence %s because other objects depend on it",
				seq.GetName(),
			))
		}
		desc := b.MustReadTable(dep.DependedOnBy)
		if desc.IsTable() {
			for _, col := range desc.PublicColumns() {
				if col.GetID() != dep.ColumnID {
					continue
				}
				// Convert the default expression into elements.
				decomposeDefaultExprToElements(b, desc, col, scpb.Target_DROP)
			}
		} else if desc.IsView() {
			if dep.ColumnID != descpb.ColumnID(descpb.InvalidID) {
				panic(errors.AssertionFailedf("views dependencies should not"+
					"have column IDs specified (observed: %d)\n",
					dep.ColumnID))
			}
			dropView(b, desc, tree.DropCascade)
		}
	})
}
