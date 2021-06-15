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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// dropSequenceDesc builds targets and transformations using a descriptor.
func (b *buildContext) dropSequenceDesc(
	ctx context.Context, seq catalog.TableDescriptor, cascade tree.DropBehavior,
) {
	// Check if there are dependencies.
	err := seq.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
		if cascade != tree.DropCascade {
			return pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"cannot drop sequence %s because other objects depend on it",
				seq.GetName(),
			)
		}
		desc, err := b.Descs.GetImmutableTableByID(ctx, b.EvalCtx.Txn, dep.ID, tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			return err
		}
		for _, col := range desc.PublicColumns() {
			for _, id := range dep.ColumnIDs {
				if col.GetID() != id {
					continue
				}
				defaultExpr := &scpb.DefaultExpression{
					TableID:         dep.ID,
					ColumnID:        col.GetID(),
					UsesSequenceIDs: col.ColumnDesc().UsesSequenceIds,
					DefaultExpr:     ""}
				if exists, _ := b.checkIfNodeExists(scpb.Target_DROP, defaultExpr); !exists {
					b.addNode(scpb.Target_DROP, defaultExpr)
				}
				b.removeColumnTypeBackRefs(desc, col.GetID())
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Add a node to drop the sequence
	sequenceNode := &scpb.Sequence{SequenceID: seq.GetID()}
	if exists, _ := b.checkIfNodeExists(scpb.Target_DROP, sequenceNode); !exists {
		b.addNode(scpb.Target_DROP, sequenceNode)
	}
	if seq.GetSequenceOpts().SequenceOwner.OwnerTableID != descpb.InvalidID {
		sequenceOwnedBy := &scpb.SequenceOwnedBy{
			SequenceID:   seq.GetID(),
			OwnerTableID: seq.GetSequenceOpts().SequenceOwner.OwnerTableID}
		if exists, _ := b.checkIfNodeExists(scpb.Target_DROP, sequenceOwnedBy); !exists {
			b.addNode(scpb.Target_DROP, sequenceOwnedBy)
		}
	}
}

// dropSequence builds targets and transforms the provided schema change nodes
// accordingly, given an DROP SEQUENCE statement.
func (b *buildContext) dropSequence(ctx context.Context, n *tree.DropSequence) {
	// Find the sequence first.
	for _, name := range n.Names {
		_, table, err := resolver.ResolveExistingTableObject(ctx, b.Res, &name,
			tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			if pgerror.GetPGCode(err) == pgcode.UndefinedTable && n.IfExists {
				continue
			}
			panic(err)
		}
		if table == nil {
			panic(errors.AssertionFailedf("unable to resolve sequence %s",
				name.FQString()))
		}

		if table.Dropped() {
			return
		}
		b.dropSequenceDesc(ctx, table, n.DropBehavior)
	}
}
