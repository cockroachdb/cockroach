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
	// Find the table first.
	for _, name := range n.Names {
		_, tbl := b.ResolveTable(name.ToUnresolvedObjectName(), ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.DROP,
		})
		if tbl == nil {
			continue
		}
		dropTable(b, tbl, n.DropBehavior)
		b.IncrementSubWorkID()
	}
}

func dropTable(b BuildCtx, tbl catalog.TableDescriptor, behavior tree.DropBehavior) {
	{
		decomposeTableDescToElements(b, tbl, scpb.Target_DROP)
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
					pgcode.DependentObjectsStillExist, "cannot drop table %q because view %q depends on it",
					name, depViewName)
			}
			dropView(c, dependentDesc, behavior)
			return nil
		}))
		// Detect if foreign keys will end up preventing this drop behavior.
		scpb.ForEachForeignKeyBackReference(c,
			func(_ scpb.Status,
				_ scpb.Target_Direction,
				fk *scpb.ForeignKeyBackReference) {
				dependentTable := c.MustReadTable(fk.ReferenceID)
				if fk.OriginID == tbl.GetID() {
					if behavior != tree.DropCascade {
						panic(pgerror.Newf(
							pgcode.DependentObjectsStillExist,
							"%q is referenced by foreign key from table %q", fk.Name, dependentTable.GetName()))
					}
				}
			})
		// Detect any sequence ownerships and prevent clean up if cascades
		// are disallowed.
		scpb.ForEachSequenceOwnedBy(c, func(_ scpb.Status,
			_ scpb.Target_Direction,
			sequenceOwnedBy *scpb.SequenceOwnedBy) {
			if sequenceOwnedBy.OwnerTableID != tbl.GetID() {
				return
			}
			sequence := c.MustReadTable(sequenceOwnedBy.SequenceID)
			if behavior != tree.DropCascade {
				panic(pgerror.Newf(
					pgcode.DependentObjectsStillExist,
					"cannot drop table %s because other objects depend on it",
					sequence.GetName(),
				))
			}
		})
	}
}
