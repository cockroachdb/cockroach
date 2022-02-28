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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// DropSequence implements DROP SEQUENCE.
func DropSequence(b BuildCtx, n *tree.DropSequence) {
	var toCheckBackrefs []catid.DescID
	for idx := range n.Names {
		name := &n.Names[idx]
		elts := b.ResolveSequence(name.ToUnresolvedObjectName(), ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.DROP,
		})
		_, _, seq := scpb.FindSequence(elts)
		if seq == nil {
			b.MarkNameAsNonExistent(name)
			continue
		}
		// Mutate the AST to have the fully resolved name from above, which will be
		// used for both event logging and errors.
		name.ObjectNamePrefix = b.NamePrefix(seq)
		// We don't support dropping temporary tables.
		if seq.IsTemporary {
			panic(scerrors.NotImplementedErrorf(n, "dropping a temporary sequence"))
		}
		if n.DropBehavior == tree.DropCascade {
			dropCascadeDescriptor(b, seq.SequenceID)
		} else if dropRestrictDescriptor(b, seq.SequenceID) {
			// Drop sequence owner even for RESTRICT.
			scpb.ForEachSequenceOwner(
				undroppedBackrefs(b, seq.SequenceID),
				func(_ scpb.Status, _ scpb.TargetStatus, so *scpb.SequenceOwner) {
					dropElement(b, so)
				},
			)
			toCheckBackrefs = append(toCheckBackrefs, seq.SequenceID)
		}
		b.IncrementSubWorkID()
		b.IncrementSchemaChangeDropCounter("sequence")
	}
	// Check if there are any back-references which would prevent a DROP RESTRICT.
	for _, sequenceID := range toCheckBackrefs {
		backrefs := undroppedBackrefs(b, sequenceID)
		if backrefs.IsEmpty() {
			continue
		}
		_, _, ns := scpb.FindNamespace(b.QueryByID(sequenceID))
		panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
			"cannot drop sequence %s because other objects depend on it", ns.Name))
	}
}
