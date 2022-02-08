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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// DropTable implements DROP TABLE.
func DropTable(b BuildCtx, n *tree.DropTable) {
	var toCheckBackrefs []catid.DescID
	droppedOwnedSequences := make(map[catid.DescID]catalog.DescriptorIDSet)
	for idx := range n.Names {
		name := &n.Names[idx]
		elts := b.ResolveTable(name.ToUnresolvedObjectName(), ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.DROP,
		})
		_, _, tbl := scpb.FindTable(elts)
		if tbl == nil {
			b.MarkNameAsNonExistent(name)
			continue
		}
		// Mutate the AST to have the fully resolved name from above, which will be
		// used for both event logging and errors.
		name.ObjectNamePrefix = b.NamePrefix(tbl)
		// We don't support dropping temporary tables.
		if tbl.IsTemporary {
			panic(scerrors.NotImplementedErrorf(n, "dropping a temporary table"))
		}
		// Only decompose the tables first into elements, next we will check for
		// dependent objects, in case they are all dropped *together*.
		if n.DropBehavior == tree.DropCascade {
			dropCascadeDescriptor(b, tbl.TableID)
		} else {
			// Handle special case of owned sequences
			var ownedIDs catalog.DescriptorIDSet
			scpb.ForEachSequenceOwner(
				b.QueryByID(tbl.TableID),
				func(_ scpb.Status, target scpb.TargetStatus, so *scpb.SequenceOwner) {
					if target == scpb.ToPublic {
						ownedIDs.Add(so.SequenceID)
					}
				},
			)
			if dropRestrictDescriptor(b, tbl.TableID) {
				toCheckBackrefs = append(toCheckBackrefs, tbl.TableID)
				ownedIDs.ForEach(func(ownedSequenceID descpb.ID) {
					dropRestrictDescriptor(b, ownedSequenceID)
				})
				droppedOwnedSequences[tbl.TableID] = ownedIDs
			}
		}
		b.IncrementSubWorkID()
		b.IncrementSchemaChangeDropCounter("table")
	}
	// Check if there are any back-references which would prevent a DROP RESTRICT.
	for _, tableID := range toCheckBackrefs {
		backrefs := undroppedBackrefs(b, tableID)
		hasUndroppedBackrefs := !backrefs.IsEmpty()
		droppedOwnedSequences[tableID].ForEach(func(seqID descpb.ID) {
			if !undroppedBackrefs(b, seqID).IsEmpty() {
				hasUndroppedBackrefs = true
			}
		})
		if !hasUndroppedBackrefs {
			continue
		}
		_, _, ns := scpb.FindNamespace(b.QueryByID(tableID))
		maybePanicOnDependentView(b, ns, backrefs)
		if _, _, fk := scpb.FindForeignKeyConstraint(backrefs); fk != nil {
			panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
				"%q is referenced by foreign key from table %q", ns.Name, simpleName(b, fk.TableID)))
		}
		panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
			"cannot drop table %s because other objects depend on it", ns.Name))
	}
}
