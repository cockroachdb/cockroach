// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

// RenameTable implements ALTER TABLE ... RENAME TO for the declarative schema changer.
func RenameTable(b BuildCtx, n *tree.RenameTable) {
	// Determine what type of object we're resolving based on the statement.
	elts := b.ResolveRelation(n.Name, ResolveParams{
		IsExistenceOptional: n.IfExists,
		RequiredPrivilege:   privilege.DROP,
	})

	// IF EXISTS was specified and object doesn't exist, so this is a no-op.
	if elts == nil && n.IfExists {
		return
	}

	// Validate the object type matches what was requested.
	validateObjectType(elts, n)

	// Get the fully qualified object name.
	objectName := n.Name.ToTableName()
	dbElts, scElts := b.ResolveTargetObject(n.Name, privilege.CREATE /* this should be 0 */)
	_, _, scName := scpb.FindNamespace(scElts)
	_, _, dbname := scpb.FindNamespace(dbElts)
	objectName.SchemaName = tree.Name(scName.Name)
	objectName.CatalogName = tree.Name(dbname.Name)
	objectName.ExplicitCatalog = true
	objectName.ExplicitSchema = true

	// Get the descriptor ID for further processing.
	var targetDescriptorID catid.DescID
	var targetElement scpb.Element
	_, _, tbl := scpb.FindTable(elts)
	if tbl != nil {
		targetElement = tbl
		targetDescriptorID = tbl.TableID
	}
	if n.IsView || targetDescriptorID == 0 {
		_, _, view := scpb.FindView(elts)
		if view != nil {
			targetElement = view
			targetDescriptorID = view.ViewID
		}
	}
	if n.IsSequence || targetDescriptorID == 0 {
		_, _, seq := scpb.FindSequence(elts)
		if seq != nil {
			targetElement = seq
			targetDescriptorID = seq.SequenceID
		}
	}

	// Check for name-based dependencies that would prevent renaming.
	checkNameBasedDependencies(b, targetDescriptorID, targetElement, objectName)

	// Need CREATE privilege on database to match legacy schema changer behavior.
	b.ResolveDatabase(objectName.CatalogName, ResolveParams{RequiredPrivilege: privilege.CREATE})

	// Find the current namespace element.
	_, _, currentNS := scpb.FindNamespace(elts)
	if currentNS == nil {
		panic(errors.AssertionFailedf("table %q resolved but namespace element not found", n.Name))
	}

	// Check for name conflicts with the new fully qualified name. If the new name
	// is not qualified, assume it's the same database and schema as the old name.
	newName := n.NewName.ToTableName()
	if !newName.ExplicitCatalog && !newName.ExplicitSchema {
		newName.CatalogName = objectName.CatalogName
		newName.SchemaName = objectName.SchemaName
	} else {
		b.EvalCtx().ClientNoticeSender.BufferClientNotice(
			b,
			errors.WithHintf(
				pgnotice.Newf("renaming tables with a qualification is deprecated"),
				"use ALTER TABLE %s RENAME TO %s instead",
				n.Name.String(),
				newName.ObjectName,
			),
		)
		newDBElts, newScElts := b.ResolveTargetObject(newName.ToUnresolvedObjectName(), privilege.CREATE)
		_, _, newScName := scpb.FindNamespace(newScElts)
		_, _, newDBname := scpb.FindNamespace(newDBElts)
		newName.SchemaName = tree.Name(newScName.Name)
		newName.CatalogName = tree.Name(newDBname.Name)
	}
	newName.ExplicitCatalog = true
	newName.ExplicitSchema = true
	checkTableNameConflicts(b, objectName, newName, currentNS)

	// Check if the new name is the same as the old name (no-op case).
	if currentNS.Name == string(newName.ObjectName) {
		return
	}

	newNS := &scpb.Namespace{
		DatabaseID:   currentNS.DatabaseID,
		SchemaID:     currentNS.SchemaID,
		DescriptorID: currentNS.DescriptorID,
		Name:         string(newName.ObjectName),
	}

	// Drop the old namespace element and add the new one.
	b.Drop(currentNS)
	b.Add(newNS)

	// Log RenameTable event for audit logging.
	renameEvent := &eventpb.RenameTable{
		TableName:    objectName.FQString(),
		NewTableName: newName.FQString(),
	}
	b.LogEventForExistingPayload(newNS, renameEvent)
}

// validateTableRename performs validation checks before renaming a table.
func validateTableRename(b BuildCtx, currentName tree.TableName, newName tree.TableName) {
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

	if newName.ExplicitSchema {
		if currentName.SchemaName != newName.SchemaName {
			panic(errors.WithHint(
				pgerror.Newf(pgcode.InvalidName, "cannot change schema of table with RENAME"),
				"use ALTER TABLE ... SET SCHEMA instead",
			))
		}
	}
}

// checkTableNameConflicts checks if the new table name conflicts with existing
// objects.
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
		_, _, existingNS := scpb.FindNamespace(ers)
		if existingNS.DescriptorID == currentNS.DescriptorID {
			return
		}

		// Check if it's being dropped.
		_, existingTarget, existingNS := scpb.FindNamespace(ers)
		if existingNS != nil && existingTarget == scpb.ToAbsent {
			panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"relation %q being dropped, try again later", newName.String()))
		}

		// Object exists, throw duplicate name error with fully qualified name.
		if !ers.FilterCompositeType().IsEmpty() || !ers.FilterEnumType().IsEmpty() {
			panic(sqlerrors.NewTypeAlreadyExistsError(newName.String()))
		}
		panic(sqlerrors.NewRelationAlreadyExistsError(newName.String()))
	}

	validateTableRename(b, currentName, newName)
}

// validateObjectType validates that the resolved object type matches what was
// requested in the statement. Note that we allow ALTER TABLE to be used for
// views or sequences, just like in Postgres.
func validateObjectType(elts ElementResultSet, n *tree.RenameTable) {
	_, _, view := scpb.FindView(elts)
	_, _, seq := scpb.FindSequence(elts)

	objectName := n.Name.ToTableName().ObjectName

	if n.IsView && view == nil {
		// User asked for view but we found something else.
		panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a view", objectName))
	} else if n.IsSequence && seq == nil {
		// User asked for sequence but we found something else.
		panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a sequence", objectName))
	}

	if view != nil {
		// Validate view type (materialized vs non-materialized).
		if view.IsMaterialized && !n.IsMaterialized {
			panic(errors.WithHint(pgerror.Newf(pgcode.WrongObjectType, "%q is a materialized view", objectName),
				"use the corresponding MATERIALIZED VIEW command"))
		}
		if !view.IsMaterialized && n.IsMaterialized {
			panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a materialized view", objectName))
		}
	}
}

// checkNameBasedDependencies validates that no objects depend on this object
// via its name.
func checkNameBasedDependencies(
	b BuildCtx, descriptorID catid.DescID, element scpb.Element, objectName tree.TableName,
) {
	switch element.(type) {
	case *scpb.Sequence:
		// Sequences are always referenced by ID.
		return
	}
	backRefs := b.BackReferences(descriptorID)
	backRefs.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch backRefElem := e.(type) {
		case *scpb.View:
			// Views depend on other objects by name, so the rename needs to be
			// blocked.
			viewElts := b.QueryByID(backRefElem.ViewID)
			_, _, viewNS := scpb.FindNamespace(viewElts)
			panic(sqlerrors.NewDependentBlocksOpError("rename", "relation", objectName.String(), "view", viewNS.Name))
		case *scpb.FunctionName:
			funcElem := b.QueryByID(backRefElem.FunctionID).FilterFunction().MustGetOneElement()
			funcType := "function"
			if funcElem.IsProcedure {
				funcType = "procedure"
			}
			panic(sqlerrors.NewDependentBlocksOpError("rename", "relation", objectName.String(), funcType, backRefElem.Name))
		case *scpb.TriggerDeps:
			for _, usesRelation := range backRefElem.UsesRelations {
				if usesRelation.ID == descriptorID {
					dependentTableID := backRefElem.TableID
					dependentTriggerID := backRefElem.TriggerID
					dependentTableNS := b.QueryByID(dependentTableID).FilterNamespace().MustGetOneElement()
					dependentTriggerName := backRefs.FilterTriggerName().Filter(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.TriggerName) bool {
						return e.TriggerID == dependentTriggerID && e.TableID == dependentTableID
					}).MustGetOneElement()
					panic(sqlerrors.NewDependentObjectErrorf(
						"cannot rename relation %q because trigger %q on table %q depends on it",
						objectName.String(), dependentTriggerName.Name, dependentTableNS.Name,
					))
				}
			}
		}
	})
}
