// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// RenameTable implements ALTER TABLE ... RENAME TO.
func RenameTable(b BuildCtx, n *tree.RenameTable) {
	// Only handle table renames, not views or sequences
	if n.IsView || n.IsSequence {
		panic(scerrors.NotImplementedError(n))
	}

	// 1. Resolve the existing table
	elts := b.ResolveTable(n.Name, ResolveParams{
		IsExistenceOptional: n.IfExists,
		RequiredPrivilege:   privilege.CREATE,
	})
	_, target, tbl := scpb.FindTable(elts)

	// 2. Handle IF EXISTS case
	if tbl == nil {
		if n.IfExists {
			return
		}
		panic(pgerror.Newf(pgcode.UndefinedTable, "relation %q does not exist", n.Name))
	}

	// Ensure the table is not being dropped
	if target != scpb.ToPublic {
		panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"table %q is being dropped, try again later", n.Name.Object()))
	}

	// 3. Get current namespace element
	_, _, currentNS := scpb.FindNamespace(elts)
	if currentNS == nil {
		panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "namespace element not found for table %q", n.Name))
	}

	// 4. Validate new name doesn't conflict
	// TODO: Implement name collision checking
	// b.CheckObjectNameCollision(n.NewName, currentNS.DatabaseID, currentNS.SchemaID)

	// 5. Mark old namespace as ABSENT
	b.Drop(currentNS)

	// 6. Add new namespace element targeting PUBLIC
	b.Add(&scpb.Namespace{
		DatabaseID:   currentNS.DatabaseID,
		SchemaID:     currentNS.SchemaID,
		DescriptorID: currentNS.DescriptorID,
		Name:         n.NewName.Object(),
	})
}