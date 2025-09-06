// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// RenameTable implements ALTER TABLE ... RENAME TO for the declarative schema changer.
func RenameTable(b BuildCtx, n *tree.RenameTable) {
	// Resolve the existing table.
	elts := b.ResolveTable(n.Name, ResolveParams{
		IsExistenceOptional: n.IfExists,
		RequiredPrivilege:   privilege.DROP,
	})

	// Handle IF EXISTS case where table doesn't exist.
	if elts == nil {
		// IF EXISTS was specified and table doesn't exist, so this is a no-op.
		return
	}

	// Find the table element.
	_, _, tbl := scpb.FindTable(elts)
	if tbl == nil {
		// This shouldn't happen if ResolveTable succeeded, but check for safety.
		if n.IfExists {
			return
		}
		panic(sqlerrors.NewUndefinedRelationError(n.Name))
	}

	// Extract the current table name.
	currentTableName := n.Name.ToTableName()
	dbElts, scElts := b.ResolveTargetObject(n.Name, privilege.CREATE /* this should be 0 */)
	_, _, scName := scpb.FindNamespace(scElts)
	_, _, dbname := scpb.FindNamespace(dbElts)
	currentTableName.SchemaName = tree.Name(scName.Name)
	currentTableName.CatalogName = tree.Name(dbname.Name)
	currentTableName.ExplicitCatalog = true
	currentTableName.ExplicitSchema = true

	// Need CREATE privilege on database.
	b.ResolveDatabase(currentTableName.CatalogName, ResolveParams{RequiredPrivilege: privilege.CREATE})

	// Find the current namespace element.
	_, _, currentNS := scpb.FindNamespace(elts)
	if currentNS == nil {
		panic(errors.AssertionFailedf("table %q resolved but namespace element not found", n.Name))
	}

	// Check for name conflicts with the new name.
	newTableName := n.NewName.ToTableName()
	checkTableNameConflicts(b, currentTableName, newTableName, currentNS)

	// Check if the new name is the same as the old name (no-op case).
	if currentNS.Name == string(newTableName.ObjectName) {
		return
	}
	// Drop the old namespace element and add the new one.
	// This pattern ensures proper rollback behavior (reverting to the old name).
	b.Drop(currentNS)
	b.Add(&scpb.Namespace{
		DatabaseID:   currentNS.DatabaseID,
		SchemaID:     currentNS.SchemaID,
		DescriptorID: currentNS.DescriptorID,
		Name:         string(newTableName.ObjectName),
	})
}

// validateTableRename performs validation checks before renaming a table.
func validateTableRename(b BuildCtx, currentName tree.TableName, newName tree.TableName) {
	if newName.ExplicitSchema {
		if currentName.SchemaName != newName.SchemaName {
			panic(errors.WithHint(
				pgerror.Newf(pgcode.InvalidName, "cannot change schema of table with RENAME"),
				"use ALTER TABLE ... SET SCHEMA instead",
			))
		}
	}
	// The legacy schema changer used to check the CREATE privilege on the
	// database in the new name too, so we preserve that here. It would make
	// more sense to not do that, since we validate that the new database name
	// is the same as the old database name. That change can be made in the
	// future.
	b.ResolveDatabase(newName.CatalogName, ResolveParams{RequiredPrivilege: privilege.CREATE})

	if newName.ExplicitCatalog {
		if currentName.CatalogName != newName.CatalogName {
			panic(pgerror.Newf(pgcode.FeatureNotSupported, "cannot change database of table using alter table rename to"))
		}
	}
}

// checkTableNameConflicts checks if the new table name conflicts with existing objects.
func checkTableNameConflicts(
	b BuildCtx, currentName, newName tree.TableName, currentNS *scpb.Namespace,
) {
	// Check if a relation with the new name already exists.
	ers := b.ResolveRelation(newName.ToUnresolvedObjectName(),
		ResolveParams{
			IsExistenceOptional: true,
			WithOffline:         true, // Check including offline objects.
			ResolveTypes:        true, // Check for collisions with type names.
		})

	if ers != nil && !ers.IsEmpty() {
		// Check if it's the same descriptor we're renaming.
		_, _, existingTable := scpb.FindTable(ers)
		if existingTable != nil && existingTable.TableID == currentNS.DescriptorID {
			return
		}

		// Validate that the table can be renamed.
		dbElts, scElts := b.ResolveTargetObject(newName.ToUnresolvedObjectName(), privilege.CREATE /* this should be 0 */)
		_, _, scName := scpb.FindNamespace(scElts)
		_, _, dbname := scpb.FindNamespace(dbElts)
		newName.SchemaName = tree.Name(scName.Name)
		newName.CatalogName = tree.Name(dbname.Name)
		newName.ExplicitCatalog = true
		newName.ExplicitSchema = true
		validateTableRename(b, currentName, newName)

		// Check if it's being dropped.
		_, existingTarget, existingNS := scpb.FindNamespace(ers)
		if existingNS != nil && existingTarget == scpb.ToAbsent {
			panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"relation %q being dropped, try again later", newName.String()))
		}

		// Object exists, throw duplicate name error with fully qualified name.
		panic(sqlerrors.NewRelationAlreadyExistsError(newName.String()))
	}
}
