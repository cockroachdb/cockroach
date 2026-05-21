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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
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
	b BuildCtx, tn *tree.TypeName, typeID catid.DescID, arrayTypeID catid.DescID, owner tree.RoleSpec,
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

	if !b.HasOwnership(oldOwner) {
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
	arrayTn := tree.MakeTypeNameWithPrefix(b.NamePrefix(arrayNs), arrayNs.Name)

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

	currName := tree.MakeTableNameFromPrefix(b.NamePrefix(currNamespace), tree.Name(currNamespace.Name))
	newName := currName
	newName.SchemaName = newSchemaName

	checkTableNameConflicts(b, currName, newName, currNamespace)

	arrayNamespace := mustRetrieveNamespaceElem(b, arrayTypeID)
	arrayName := tree.MakeTableNameFromPrefix(
		b.NamePrefix(arrayNamespace), tree.Name(arrayNamespace.Name),
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

// renameForTypeDesc implements RENAME TO for user-defined types that have an
// associated array type (enums, composites, domains).
func renameForTypeDesc(
	b BuildCtx, tn *tree.TypeName, typeID catid.DescID, arrayTypeID catid.DescID, newName string,
) {
	typeElts := b.QueryByID(typeID)
	currentNS := typeElts.FilterNamespace().NotToAbsent().MustGetOneElement()

	// Unlike table rename (which no-ops on self-rename), Postgres treats type
	// rename to the same name as an error.
	if currentNS.Name == newName {
		typeName := tree.MakeTableNameFromPrefix(
			b.NamePrefix(currentNS), tree.Name(newName),
		)
		panic(sqlerrors.NewTypeAlreadyExistsError(typeName.String()))
	}

	checkTypeNameConflicts(b, newName, currentNS)

	newNS := &scpb.Namespace{
		DatabaseID:   currentNS.DatabaseID,
		SchemaID:     currentNS.SchemaID,
		DescriptorID: currentNS.DescriptorID,
		Name:         newName,
	}
	b.Drop(currentNS)
	b.Add(newNS)

	// Rename the implicit array type.
	arrayElts := b.QueryByID(arrayTypeID)
	arrayNS := arrayElts.FilterNamespace().NotToAbsent().MustGetOneElement()

	newArrayName := findFreeNameInSchema(b, b.NamePrefix(arrayNS), "_"+newName)
	newArrayNS := &scpb.Namespace{
		DatabaseID:   arrayNS.DatabaseID,
		SchemaID:     arrayNS.SchemaID,
		DescriptorID: arrayNS.DescriptorID,
		Name:         newArrayName,
	}
	b.Drop(arrayNS)
	b.Add(newArrayNS)

	b.LogEventForExistingPayload(newNS, &eventpb.RenameType{
		TypeName:    tn.FQString(),
		NewTypeName: newName,
	})
}

// checkTypeNameConflicts checks if the new type name conflicts with an
// existing object in the same schema.
func checkTypeNameConflicts(b BuildCtx, newName string, currentNS *scpb.Namespace) {
	tn := tree.MakeTableNameFromPrefix(
		b.NamePrefix(currentNS),
		tree.Name(newName),
	)
	ers := b.ResolveRelation(tn.ToUnresolvedObjectName(),
		ResolveParams{
			IsExistenceOptional: true,
			WithOffline:         true,
			ResolveTypes:        true,
		})
	if ers == nil || ers.IsEmpty() {
		return
	}

	existingNS := ers.FilterNamespace().NotToAbsent().MustGetZeroOrOneElement()
	if existingNS == nil {
		if !ers.FilterNamespace().ToAbsent().IsEmpty() {
			panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"object %q being dropped, try again later", newName))
		}
		return
	}

	if !ers.FilterCompositeType().IsEmpty() || !ers.FilterEnumType().IsEmpty() || !ers.FilterDomainType().IsEmpty() {
		panic(sqlerrors.NewTypeAlreadyExistsError(tn.String()))
	}
	panic(sqlerrors.NewRelationAlreadyExistsError(tn.String()))
}

// findFreeNameInSchema prepends underscores to candidateName until it doesn't
// collide with any existing object in the given schema.
func findFreeNameInSchema(b BuildCtx, prefix tree.ObjectNamePrefix, candidateName string) string {
	for {
		tn := tree.MakeTableNameFromPrefix(prefix, tree.Name(candidateName))
		ers := b.ResolveRelation(tn.ToUnresolvedObjectName(),
			ResolveParams{
				IsExistenceOptional: true,
				WithOffline:         true,
				ResolveTypes:        true,
			})
		if ers == nil || ers.IsEmpty() {
			return candidateName
		}
		candidateName = "_" + candidateName
	}
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
