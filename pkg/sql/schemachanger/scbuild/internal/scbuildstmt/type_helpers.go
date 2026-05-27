// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

func resolveSchemaByName(b BuildCtx, schemaName tree.Name, databaseID catid.DescID) *scpb.Schema {
	dbElts := b.QueryByID(databaseID)
	dbNamespace := dbElts.FilterNamespace().MustGetOneElement()
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

func panicIfSchemaIsTemporaryOrVirtual(schemaName tree.Name) {
	name := string(schemaName)
	if _, ok := catconstants.VirtualSchemaNames[name]; ok {
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot move objects into or out of virtual schemas"))
	}
	if strings.HasPrefix(name, catconstants.PgTempSchemaName) {
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot move objects into or out of temporary schemas"))
	}
}

// setOwnerForTypeDesc implements OWNER TO for user-defined types that have an
// associated array type (enums, composites, domains).
func setOwnerForTypeDesc(
	b BuildCtx,
	tn *tree.TypeName,
	typeElem scpb.Element,
	typeID catid.DescID,
	arrayTypeID catid.DescID,
	owner tree.RoleSpec,
) {
	newOwner, err := decodeusername.FromRoleSpec(
		b.SessionData(), username.PurposeValidation, owner,
	)
	if err != nil {
		panic(err)
	}

	typeElts := b.QueryByID(typeID)
	oldOwner := typeElts.FilterOwner().MustGetOneElement()

	if newOwner.Normalized() == oldOwner.Owner {
		return
	}

	if !b.HasOwnership(typeElem) {
		panic(pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of type %s", tn.Object()))
	}

	if err := b.CheckRoleExists(b, newOwner); err != nil {
		panic(err)
	}
	if !b.CurrentUserHasAdminOrIsMemberOf(newOwner) {
		panic(pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be member of role %q", newOwner))
	}

	schemaChild := typeElts.FilterSchemaChild().MustGetOneElement()
	schemaElts := b.QueryByID(schemaChild.SchemaID)
	schema := schemaElts.FilterSchema().MustGetOneElement()
	if err := b.CheckPrivilegeForUser(schema, privilege.CREATE, newOwner); err != nil {
		panic(err)
	}

	b.Replace(&scpb.Owner{
		DescriptorID: typeID,
		Owner:        newOwner.Normalized(),
	})

	b.Replace(&scpb.Owner{
		DescriptorID: arrayTypeID,
		Owner:        newOwner.Normalized(),
	})

	arrayElts := b.QueryByID(arrayTypeID)
	arrayNs := arrayElts.FilterNamespace().MustGetOneElement()
	arrayTn := tree.MakeTypeNameWithPrefix(b.NamePrefix(typeElem), arrayNs.Name)

	b.LogEventForExistingPayload(&scpb.Owner{
		DescriptorID: typeID,
		Owner:        newOwner.Normalized(),
	}, &eventpb.AlterTypeOwner{
		TypeName: tn.FQString(),
		Owner:    newOwner.Normalized(),
	})
	b.LogEventForExistingPayload(&scpb.Owner{
		DescriptorID: arrayTypeID,
		Owner:        newOwner.Normalized(),
	}, &eventpb.AlterTypeOwner{
		TypeName: arrayTn.FQString(),
		Owner:    newOwner.Normalized(),
	})
}

// setSchemaForTypeDesc implements SET SCHEMA for user-defined types that have
// an associated array type (enums, composites, domains).
func setSchemaForTypeDesc(
	b BuildCtx,
	typeElem scpb.Element,
	typeID catid.DescID,
	arrayTypeID catid.DescID,
	newSchemaName tree.Name,
	descriptorType string,
) {
	currNamespace := mustRetrieveNamespaceElem(b, typeID)
	currSchemaID := currNamespace.SchemaID
	panicIfSchemaIsTemporaryOrVirtual(newSchemaName)
	newSchema := resolveSchemaByName(b, newSchemaName, currNamespace.DatabaseID)
	newSchemaID := newSchema.SchemaID

	if currSchemaID == newSchemaID {
		return
	}

	currName := tree.MakeTableNameFromPrefix(b.NamePrefix(typeElem), tree.Name(currNamespace.Name))
	newName := currName
	newName.SchemaName = newSchemaName

	checkTableNameConflicts(b, currName, newName, currNamespace)

	arrayNamespace := mustRetrieveNamespaceElem(b, arrayTypeID)
	arrayName := tree.MakeTableNameFromPrefix(
		b.NamePrefix(typeElem), tree.Name(arrayNamespace.Name),
	)
	newArrayName := arrayName
	newArrayName.SchemaName = newSchemaName
	checkTableNameConflicts(b, arrayName, newArrayName, arrayNamespace)

	newNS, _ := moveDescriptorToSchema(b, typeID, currNamespace, newSchemaID)
	moveDescriptorToSchema(b, arrayTypeID, arrayNamespace, newSchemaID)

	b.LogEventForExistingPayload(newNS, &eventpb.SetSchema{
		DescriptorName:    currName.FQString(),
		NewDescriptorName: newName.FQString(),
		DescriptorType:    descriptorType,
	})
}

// moveDescriptorToSchema drops the old Namespace and SchemaChild elements for
// the given descriptor and adds new ones with the updated schema ID. It returns
// the new Namespace and SchemaChild elements.
func moveDescriptorToSchema(
	b BuildCtx, descID catid.DescID, currNamespace *scpb.Namespace, newSchemaID catid.DescID,
) (*scpb.Namespace, *scpb.SchemaChild) {
	newNamespace := *currNamespace
	newNamespace.SchemaID = newSchemaID
	b.Drop(currNamespace)
	b.Add(&newNamespace)

	currSchemaChild := b.QueryByID(descID).FilterSchemaChild().MustGetOneElement()
	newSchemaChild := &scpb.SchemaChild{
		ChildObjectID: descID,
		SchemaID:      newSchemaID,
	}
	b.Drop(currSchemaChild)
	b.Add(newSchemaChild)

	return &newNamespace, newSchemaChild
}
