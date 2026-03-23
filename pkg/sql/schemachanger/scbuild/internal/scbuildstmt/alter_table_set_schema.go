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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

// AlterTableSetSchema implements ALTER TABLE ... SET SCHEMA ... for the declarative schema changer.
// It sets the schema for a table, view, or sequence.
// Requires privileges: DROP on source table/view/sequence, CREATE on destination schema.
func AlterTableSetSchema(b BuildCtx, n *tree.AlterTableSetSchema) {
	// Resolve any type of object
	elts := b.ResolveRelation(n.Name, ResolveParams{
		IsExistenceOptional: n.IfExists,
		RequiredPrivilege:   privilege.DROP,
	})
	// IF EXISTS was specified, and the object doesn't exist, so this is a no-op.
	if elts == nil && n.IfExists {
		return
	}
	// Validate the object type matches what was requested.
	objName := n.Name.ToTableName().ObjectName
	validateObjectType(elts, objName, n.IsSequence, n.IsView, n.IsMaterialized)

	// get descId based on type to retrieve the namespace
	descID, element, isTemp := getRelationElement(elts)
	// Ensure that table is not temporary
	if isTemp {
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot move objects into or out of temporary schemas"))
	}
	// Get the fully qualified object name.
	currName := tree.MakeTableNameFromPrefix(b.NamePrefix(element), objName)
	newName := currName
	newName.SchemaName = n.Schema
	// Check for name-based dependencies
	checkNameBasedDependencies(b, descID, element, currName, "set schema on")

	// Get the schema ID directly from the namespace element
	currNamespace := mustRetrieveNamespaceElem(b, descID)
	currSchemaID := currNamespace.SchemaID
	newSchema := resolveSchemaByName(b, n.Schema, currNamespace.DatabaseID)
	newSchemaID := newSchema.SchemaID
	// Ensure that new schema is not temporary or virtual
	panicIfSchemaIsTemporaryOrVirtual(newSchema)
	// If new schema is the same as the curr schema, do a no-op
	if currSchemaID == newSchemaID {
		return
	}

	// Increment telemetry counter
	b.IncrementSchemaChangeAlterCounter(tree.GetTableType(n.IsSequence, n.IsView, n.IsMaterialized), n.TelemetryName())
	// Check for name conflicts
	checkTableNameConflicts(b, currName, newName, currNamespace)

	// drop the old namespace and add a new one
	newNamespace := *currNamespace
	newNamespace.SchemaID = newSchemaID
	b.Drop(currNamespace)
	b.Add(&newNamespace)

	// drop old schema child and add new one
	currSchemaChild := b.QueryByID(descID).FilterSchemaChild().MustGetOneElement()
	newSchemaChild := scpb.SchemaChild{
		ChildObjectID: descID,
		SchemaID:      newSchemaID,
	}
	b.Drop(currSchemaChild)
	b.Add(&newSchemaChild)

	// Log event for audit logging.
	kind := tree.GetTableType(n.IsSequence, n.IsView, n.IsMaterialized)
	setSchemaEvent := &eventpb.SetSchema{
		DescriptorName:    currName.FQString(),
		NewDescriptorName: newName.FQString(),
		DescriptorType:    kind,
	}
	b.LogEventForExistingPayload(&newNamespace, setSchemaEvent)
}

func resolveSchemaByName(b BuildCtx, schemaName tree.Name, databaseID catid.DescID) *scpb.Schema {
	dbElts := b.QueryByID(databaseID)
	dbNamespace := dbElts.FilterNamespace().MustGetOneElement()
	// Resolve the new schema to get its elements
	newSchemaPrefix := tree.ObjectNamePrefix{
		CatalogName:     tree.Name(dbNamespace.Name),
		SchemaName:      schemaName,
		ExplicitCatalog: true,
		ExplicitSchema:  true,
	}
	newSchema := b.ResolveSchema(newSchemaPrefix, ResolveParams{
		RequiredPrivilege: privilege.CREATE,
	}).FilterSchema().MustGetOneElement()
	return newSchema
}

func panicIfSchemaIsTemporaryOrVirtual(newSchema *scpb.Schema) {
	if newSchema.IsTemporary {
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot move objects into or out of temporary schemas"))
	}
	if newSchema.IsVirtual {
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot move objects into or out of virtual schemas"))
	}
}
