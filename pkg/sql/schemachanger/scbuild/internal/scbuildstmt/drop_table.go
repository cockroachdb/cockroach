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
)

// DropTable implements DROP TABLE.
func DropTable(b BuildCtx, n *tree.DropTable) {
	type tblDropCtx struct {
		tbl      catalog.TableDescriptor
		buildCtx BuildCtx
	}
	// Find the table first.
	tables := make([]tblDropCtx, 0, len(n.Names))
	for _, name := range n.Names {
		_, tbl := b.ResolveTable(name.ToUnresolvedObjectName(), ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.DROP,
		})
		if tbl == nil {
			continue
		}
		// Only decompose the tables first into elements, next we will check for
		// dependent objects, in case they are all dropped *together*.
		newCtx := dropTableBasic(b, tbl)
		tables = append(tables, tblDropCtx{
			tbl:      tbl,
			buildCtx: newCtx,
		})
		b.IncrementSubWorkID()
	}
	// Validate if the dependent objects need to be dropped, if necessary
	// this will cascade.
	for _, tblCtx := range tables {
		dropTableDependents(tblCtx.buildCtx, tblCtx.tbl, n.DropBehavior)
	}
}

// dropTable drops a table and its dependencies, if the cascade behavior is not
// specified the appropriate error will be generated.
func dropTable(b BuildCtx, tbl catalog.TableDescriptor, behavior tree.DropBehavior) {
	dropTableDependents(dropTableBasic(b, tbl), tbl, behavior)
}

// dropTableBasic drops the table descriptor and does not validate or deal with
// any objects that may need to be dealt with when cascading. The BuildCtx for
// cascaded drops is returned.
func dropTableBasic(b BuildCtx, tbl catalog.TableDescriptor) BuildCtx {
	decomposeTableDescToElements(b, tbl, scpb.Status_ABSENT)
	return b.WithNewSourceElementID()
}

// dropTableDependents drops any dependent objects for the table if possible,
// if a cascade is not specified an appropriate error is returned.
func dropTableDependents(b BuildCtx, tbl catalog.TableDescriptor, behavior tree.DropBehavior) {
	{
		// Drop dependent views
		c := b.WithNewSourceElementID()
		onErrPanic(tbl.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
			dependentDesc := b.MustReadTable(dep.ID)
			if behavior != tree.DropCascade {
				name, err := b.CatalogReader().GetQualifiedTableNameByID(b.EvalCtx().Context, int64(tbl.GetID()), tree.ResolveRequireViewDesc)
				onErrPanic(err)
				depViewName, err := b.CatalogReader().GetQualifiedTableNameByID(b.EvalCtx().Context, int64(dep.ID), tree.ResolveRequireViewDesc)
				onErrPanic(err)

				return pgerror.Newf(
					pgcode.DependentObjectsStillExist, "cannot drop relation %q because view %q depends on it",
					name.Object(), depViewName.Object())
			}
			dropView(c, dependentDesc, behavior)
			return nil
		}))
		// Detect if foreign key back refs will end up preventing this drop behavior.
		scpb.ForEachForeignKeyBackReference(c, func(_, _ scpb.Status, fk *scpb.ForeignKeyBackReference) {
			dependentTable := c.MustReadTable(fk.ReferenceID)
			if fk.OriginID == tbl.GetID() &&
				!checkIfDescOrElementAreDropped(b, fk.ReferenceID) &&
				behavior != tree.DropCascade {
				panic(pgerror.Newf(
					pgcode.DependentObjectsStillExist,
					"%q is referenced by foreign key from table %q",
					tbl.GetName(),
					dependentTable.GetName()))
			}
			// Add a foreign key for clean up.
			b.EnqueueDropIfNotExists(&scpb.ForeignKey{
				Name:             fk.Name,
				OriginID:         fk.ReferenceID,
				OriginColumns:    fk.ReferenceColumns,
				ReferenceID:      fk.OriginID,
				ReferenceColumns: fk.OriginColumns,
			})
		})
		// Clean up any foreign keys next.
		scpb.ForEachForeignKey(c, func(_, _ scpb.Status, fk *scpb.ForeignKey) {
			// Add a back reference for clean up.
			b.EnqueueDropIfNotExists(&scpb.ForeignKeyBackReference{
				Name:             fk.Name,
				OriginID:         fk.ReferenceID,
				OriginColumns:    fk.ReferenceColumns,
				ReferenceID:      fk.OriginID,
				ReferenceColumns: fk.OriginColumns,
			})
		})
		// Detect any sequence ownerships and clean them up if only the
		// cascade option is specified, specifically if other objects rely
		// on the sequence.
		cleanSequenceOwnedBy := func(seq catalog.TableDescriptor) {
			dropSequence(b, seq, tree.DropCascade)
			if behavior == tree.DropCascade {
				return
			}
			scpb.ForEachRelationDependedOnBy(c, func(status scpb.Status,
				targetStatus scpb.Status,
				depBy *scpb.RelationDependedOnBy) {
				if depBy.TableID != seq.GetID() {
					return
				}
				if depBy.DependedOnBy == tbl.GetID() {
					return
				}
				panic(pgerror.Newf(
					pgcode.DependentObjectsStillExist,
					"cannot drop table a because other objects depend on it"))
			})
		}
		scpb.ForEachSequenceOwnedBy(c, func(_, _ scpb.Status, sequenceOwnedBy *scpb.SequenceOwnedBy) {
			if sequenceOwnedBy.OwnerTableID != tbl.GetID() {
				return
			}
			seq := c.MustReadTable(sequenceOwnedBy.SequenceID)
			cleanSequenceOwnedBy(seq)
		})
	}
}
