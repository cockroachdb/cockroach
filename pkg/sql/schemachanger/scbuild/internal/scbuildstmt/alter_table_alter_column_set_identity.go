// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func alterTableSetIdentity(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableSetIdentity,
) {
	alterColumnPreChecks(b, tn, tbl, t.Column)
	colElem := b.ResolveColumn(tbl.TableID, t.Column, ResolveParams{
		RequiredPrivilege: privilege.CREATE,
	}).FilterColumn().MustGetOneElement()
	columnID := colElem.ColumnID
	// Block alters on system columns.
	panicIfSystemColumn(colElem, t.Column)
	oldIdentityElem := retrieveColumnGeneratedAsIdentityElem(b, tbl.TableID, columnID)
	// Ensure that column is an identity column
	if oldIdentityElem == nil {
		panic(pgerror.Newf(
			pgcode.ObjectNotInPrerequisiteState,
			"column %q of relation %q is not an identity column", t.Column, tn.ObjectName))
	}
	// Create a copy of the old element and update the type if needed
	newIdentityElem := *oldIdentityElem
	switch t.GeneratedAsIdentityType {
	case tree.GeneratedAlways:
		newIdentityElem.Type = catpb.GeneratedAsIdentityType_GENERATED_ALWAYS
	case tree.GeneratedByDefault:
		newIdentityElem.Type = catpb.GeneratedAsIdentityType_GENERATED_BY_DEFAULT
	}
	// Skip operation if the new type is the same as the old one
	if newIdentityElem.Type == oldIdentityElem.Type {
		return
	}
	// Replace the old element with the new one
	b.Drop(oldIdentityElem)
	b.Add(&newIdentityElem)
}
