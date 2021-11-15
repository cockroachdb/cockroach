// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildimpl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/scbuildctx"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/lib/pq/oid"
)

// DropTable implements DROP TABLE.
func DropTable(ctx context.Context, b scbuildctx.BuildCtx, n *tree.DropTable) {
	// Find the table first.
	for _, name := range n.Names {
		_, tbl := b.ResolveTable(ctx, name.ToUnresolvedObjectName(), scbuildctx.ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.DROP,
		})
		if tbl == nil {
			continue
		}
		dropTable(ctx, b, tbl, n.DropBehavior)
		b.IncrementSubWorkID()
	}
}

func dropTable(
	ctx context.Context,
	b scbuildctx.BuildCtx,
	tbl catalog.TableDescriptor,
	behavior tree.DropBehavior,
) {
	{
		c := b.BuildCtxWithNewSourceElementID()
		// Drop dependent views
		onErrPanic(tbl.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
			dependentDesc := c.MustReadTable(ctx, dep.ID)
			if behavior != tree.DropCascade {
				return pgerror.Newf(
					pgcode.DependentObjectsStillExist, "cannot drop table %q because view %q depends on it",
					tbl.GetName(), dependentDesc.GetName())
			}
			dropView(ctx, c, dependentDesc, behavior)
			return nil
		}))

		// Clean up foreign key references (both inbound and outbound).
		maybeCleanTableFKs(ctx, c, tbl, behavior)

		// Clean up sequence references and ownerships.
		maybeCleanTableSequenceRefs(ctx, c, tbl, behavior)

		// Clean up type back references
		removeTypeBackRefDeps(ctx, c, tbl)
	}
	b.EnqueueDrop(&scpb.Table{TableID: tbl.GetID()})
}

func maybeCleanTableSequenceRefs(
	ctx context.Context,
	b scbuildctx.BuildCtx,
	table catalog.TableDescriptor,
	behavior tree.DropBehavior,
) {
	// Setup nodes for dropping sequences
	// and cleaning up default expressions.
	for _, col := range table.PublicColumns() {
		// Loop over owned sequences
		for seqIdx := 0; seqIdx < col.NumOwnsSequences(); seqIdx++ {
			seqID := col.GetOwnsSequenceID(seqIdx)
			seq := b.MustReadTable(ctx, seqID)
			if behavior != tree.DropCascade {
				panic(pgerror.Newf(
					pgcode.DependentObjectsStillExist,
					"cannot drop table %s because other objects depend on it",
					seq.GetName(),
				))
			}
			dropSequence(ctx, b, seq, tree.DropCascade)
		}
		// Setup logic to clean up the default expression always.
		b.EnqueueDropIfNotExists(&scpb.DefaultExpression{
			DefaultExpr:     col.GetDefaultExpr(),
			TableID:         table.GetID(),
			UsesSequenceIDs: col.ColumnDesc().UsesSequenceIds,
			ColumnID:        col.GetID(),
		})
		// Get all available type references and create nodes
		// for dropping these type references.
		visitor := &tree.TypeCollectorVisitor{
			OIDs: make(map[oid.Oid]struct{}),
		}
		if col.HasDefault() && !col.ColumnDesc().HasNullDefault() {
			expr, err := parser.ParseExpr(col.GetDefaultExpr())
			onErrPanic(err)
			tree.WalkExpr(visitor, expr)
			for oid := range visitor.OIDs {
				typeID, err := typedesc.UserDefinedTypeOIDToID(oid)
				onErrPanic(err)
				b.EnqueueDropIfNotExists(&scpb.TypeReference{
					TypeID: typeID,
					DescID: table.GetID(),
				})
			}
		}

		// If there was a sequence dependency clean that up next.
		if col.NumUsesSequences() > 0 {
			// Drop the depends on within the sequence side.
			for seqOrd := 0; seqOrd < col.NumUsesSequences(); seqOrd++ {
				seqID := col.GetUsesSequenceID(seqOrd)
				b.EnqueueDropIfNotExists(&scpb.RelationDependedOnBy{
					TableID:      seqID,
					DependedOnBy: table.GetID(),
				})
			}
		}
	}
}

func maybeCleanTableFKs(
	ctx context.Context,
	b scbuildctx.BuildCtx,
	table catalog.TableDescriptor,
	behavior tree.DropBehavior,
) { // Loop through and update inbound and outbound
	// foreign key references.
	_ = table.ForeachInboundFK(func(fk *descpb.ForeignKeyConstraint) error {
		dependentTable := b.MustReadTable(ctx, fk.OriginTableID)
		if behavior != tree.DropCascade {
			panic(pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"%q is referenced by foreign key from table %q", fk.Name, dependentTable.GetName()))
		}
		onErrPanic(b.AuthorizationAccessor().CheckPrivilege(ctx, dependentTable, privilege.DROP))
		b.EnqueueDropIfNotExists(&scpb.OutboundForeignKey{
			OriginID:         fk.OriginTableID,
			OriginColumns:    fk.OriginColumnIDs,
			ReferenceID:      fk.ReferencedTableID,
			ReferenceColumns: fk.ReferencedColumnIDs,
			Name:             fk.Name,
		})
		b.EnqueueDropIfNotExists(&scpb.InboundForeignKey{
			OriginID:         fk.ReferencedTableID,
			OriginColumns:    fk.ReferencedColumnIDs,
			ReferenceID:      fk.OriginTableID,
			ReferenceColumns: fk.OriginColumnIDs,
			Name:             fk.Name,
		})
		return nil
	})

	_ = table.ForeachOutboundFK(func(fk *descpb.ForeignKeyConstraint) error {
		b.EnqueueDropIfNotExists(&scpb.OutboundForeignKey{
			OriginID:         fk.OriginTableID,
			OriginColumns:    fk.OriginColumnIDs,
			ReferenceID:      fk.ReferencedTableID,
			ReferenceColumns: fk.ReferencedColumnIDs,
			Name:             fk.Name,
		})
		b.EnqueueDropIfNotExists(&scpb.InboundForeignKey{
			OriginID:         fk.ReferencedTableID,
			OriginColumns:    fk.ReferencedColumnIDs,
			ReferenceID:      fk.OriginTableID,
			ReferenceColumns: fk.OriginColumnIDs,
			Name:             fk.Name,
		})
		return nil
	})
}
