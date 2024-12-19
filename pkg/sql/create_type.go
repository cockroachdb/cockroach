// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type createTypeNode struct {
	zeroInputPlanNode
	n        *tree.CreateType
	typeName *tree.TypeName
	dbDesc   catalog.DatabaseDescriptor
}

// EnumType is the type of an enum.
type EnumType int

const (
	// EnumTypeUserDefined is a user defined enum.
	EnumTypeUserDefined = iota
	// EnumTypeMultiRegion is a multi-region related enum.
	EnumTypeMultiRegion
)

// Use to satisfy the linter.
var _ planNode = &createTypeNode{n: nil}

func (p *planner) CreateType(ctx context.Context, n *tree.CreateType) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"CREATE TYPE",
	); err != nil {
		return nil, err
	}

	// Resolve the desired new type name.
	typeName, db, err := resolveNewTypeName(ctx, p, n.TypeName)
	if err != nil {
		return nil, err
	}
	n.TypeName.SetAnnotation(&p.semaCtx.Annotations, typeName)
	return &createTypeNode{
		n:        n,
		typeName: typeName,
		dbDesc:   db,
	}, nil
}

func (n *createTypeNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("type"))
	// Check if a type with the same name exists already.
	g := params.p.Descriptors().ByName(params.p.Txn()).MaybeGet()
	_, typ, err := descs.PrefixAndType(params.ctx, g, n.typeName)
	if err != nil {
		return err
	}

	// If we found a descriptor and have IfNotExists = true, then buffer a notice
	// and exit without doing anything. Ideally, we would do this below by
	// inspecting the type of error returned by getCreateTypeParams, but it
	// doesn't return enough information for us to do so. For comparison, we
	// handle this case in CREATE TABLE IF NOT EXISTS by checking the return code
	// (pgcode.DuplicateRelation) of getCreateTableParams. However, there isn't
	// a pgcode for duplicate types, only the more general pgcode.DuplicateObject.
	if typ != nil && n.n.IfNotExists {
		params.p.BufferClientNotice(
			params.ctx,
			pgnotice.Newf("type %q already exists, skipping", n.typeName),
		)
		return nil
	}

	return params.p.createUserDefinedType(params, n)
}

func resolveNewTypeName(
	ctx context.Context, p *planner, name *tree.UnresolvedObjectName,
) (*tree.TypeName, catalog.DatabaseDescriptor, error) {
	// Resolve the target schema and database.
	db, _, prefix, err := p.ResolveTargetObject(ctx, name)
	if err != nil {
		return nil, nil, err
	}

	if err := p.CheckPrivilege(ctx, db, privilege.CREATE); err != nil {
		return nil, nil, err
	}

	// Disallow type creation in the system database.
	if db.GetID() == keys.SystemDatabaseID {
		return nil, nil, errors.New("cannot create a type in the system database")
	}

	typename := tree.NewUnqualifiedTypeName(name.Object())
	typename.ObjectNamePrefix = prefix
	return typename, db, nil
}

// getCreateTypeParams performs some initial validation on the input new
// TypeName and returns the ID of the parent schema.
func getCreateTypeParams(
	ctx context.Context, p *planner, name *tree.TypeName, db catalog.DatabaseDescriptor,
) (schema catalog.SchemaDescriptor, err error) {
	// Check we are not creating a type which conflicts with an alias available
	// as a built-in type in CockroachDB but an extension type on the public
	// schema for PostgreSQL.
	if name.Schema() == catconstants.PublicSchemaName {
		if _, ok := types.PublicSchemaAliases[name.Object()]; ok {
			return nil, sqlerrors.NewTypeAlreadyExistsError(name.String())
		}
	}
	// Get the ID of the schema the type is being created in.
	dbID := db.GetID()
	schema, err = p.getNonTemporarySchemaForCreate(ctx, db, name.Schema())
	if err != nil {
		return nil, err
	}

	// Check permissions on the schema.
	if err := p.canCreateOnSchema(
		ctx, schema.GetID(), dbID, p.User(), skipCheckPublicSchema); err != nil {
		return nil, err
	}

	if schema.SchemaKind() == catalog.SchemaUserDefined {
		sqltelemetry.IncrementUserDefinedSchemaCounter(sqltelemetry.UserDefinedSchemaUsedByObject)
	}

	err = descs.CheckObjectNameCollision(
		ctx,
		p.Descriptors(),
		p.txn,
		db.GetID(),
		schema.GetID(),
		name,
	)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

// Postgres starts off trying to create the type as _<typename>. It then
// continues adding "_" to the front of the name until it doesn't find
// a collision. findFreeArrayTypeName performs this logic to find a free name
// for the array type based off of a type with the input name.
func findFreeArrayTypeName(
	ctx context.Context,
	txn *kv.Txn,
	col *descs.Collection,
	parentID, schemaID descpb.ID,
	name string,
) (string, error) {
	arrayName := "_" + name
	for {
		// See if there is a collision with the current name.
		objectID, err := col.LookupObjectID(ctx, txn, parentID, schemaID, arrayName)
		if err != nil {
			return "", err
		}
		// If we found an empty spot, then break out.
		if objectID == descpb.InvalidID {
			break
		}
		// Otherwise, append another "_" to the front of the name.
		arrayName = "_" + arrayName
	}
	return arrayName, nil
}

// CreateUserDefinedArrayTypeDesc creates a type descriptor for the array of the
// given user-defined type.
func CreateUserDefinedArrayTypeDesc(
	typDesc *typedesc.Mutable,
	db catalog.DatabaseDescriptor,
	schemaID descpb.ID,
	id descpb.ID,
	arrayTypeName string,
) (*typedesc.Mutable, error) {
	// Create the element type for the array. Note that it must know about the
	// ID of the array type in order for the array type to correctly created.
	var elemTyp *types.T
	switch t := typDesc.Kind; t {
	case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
		elemTyp = types.MakeEnum(catid.TypeIDToOID(typDesc.GetID()), catid.TypeIDToOID(id))
	case descpb.TypeDescriptor_COMPOSITE:
		for _, e := range typDesc.Composite.Elements {
			if e.ElementType.UserDefined() {
				return nil, unimplemented.NewWithIssue(91779,
					"composite types that reference user-defined types not yet supported")
			}
			if e.ElementType.TypeMeta.ImplicitRecordType {
				return nil, unimplemented.NewWithIssue(70099,
					"cannot use table record type as part of composite type")
			}
		}
		contents := make([]*types.T, len(typDesc.Composite.Elements))
		labels := make([]string, len(typDesc.Composite.Elements))
		for i, e := range typDesc.Composite.Elements {
			contents[i] = e.ElementType
			labels[i] = e.ElementLabel
		}
		elemTyp = types.NewCompositeType(catid.TypeIDToOID(typDesc.GetID()), catid.TypeIDToOID(id), contents, labels)
	default:
		return nil, errors.AssertionFailedf("cannot make array type for kind %s", t.String())
	}

	// Construct the descriptor for the array type.
	return typedesc.NewBuilder(&descpb.TypeDescriptor{
		Name:           arrayTypeName,
		ID:             id,
		ParentID:       db.GetID(),
		ParentSchemaID: schemaID,
		Kind:           descpb.TypeDescriptor_ALIAS,
		Alias:          types.MakeArray(elemTyp),
		Version:        1,
		Privileges:     typDesc.Privileges,
	}).BuildCreatedMutableType(), nil
}

// createArrayType performs the implicit array type creation logic of Postgres.
// When a type is created in Postgres, Postgres will implicitly create an array
// type of that user defined type. This array type tracks changes to the
// original type, and is dropped when the original type is dropped.
// createArrayType creates the implicit array type for the input TypeDescriptor
// and returns the ID of the created type.
func (p *planner) createArrayType(
	ctx context.Context,
	evalCtx *eval.Context,
	typ *tree.TypeName,
	typDesc *typedesc.Mutable,
	db catalog.DatabaseDescriptor,
	schemaID descpb.ID,
) (descpb.ID, error) {
	arrayTypeName, err := findFreeArrayTypeName(
		ctx,
		p.txn,
		p.Descriptors(),
		db.GetID(),
		schemaID,
		typ.Type(),
	)
	if err != nil {
		return 0, err
	}

	// Generate the stable ID for the array type.
	id, err := evalCtx.DescIDGenerator.GenerateUniqueDescID(ctx)
	if err != nil {
		return 0, err
	}

	arrayTypDesc, err := CreateUserDefinedArrayTypeDesc(
		typDesc,
		db,
		schemaID,
		id,
		arrayTypeName,
	)
	if err != nil {
		return 0, err
	}
	jobStr := fmt.Sprintf("implicit array type creation for %s", typ)
	if err := p.createDescriptor(ctx, arrayTypDesc, jobStr); err != nil {
		return 0, err
	}
	return id, nil
}

func (p *planner) createUserDefinedType(params runParams, n *createTypeNode) error {
	// Generate a stable ID for the new type.
	id, err := params.EvalContext().DescIDGenerator.GenerateUniqueDescID(params.ctx)
	if err != nil {
		return err
	}
	switch n.n.Variety {
	case tree.Enum:
		return params.p.createEnumWithID(
			params.ctx, params.EvalContext(), id, n.n.EnumLabels, n.dbDesc, n.typeName, EnumTypeUserDefined,
		)
	case tree.Composite:
		return params.p.createCompositeWithID(
			params, id, n.n.CompositeTypeList, n.dbDesc, n.typeName,
		)
	}
	return unimplemented.NewWithIssue(25123, "CREATE TYPE")
}

// CreateEnumTypeDesc creates a new enum type descriptor.
func CreateEnumTypeDesc(
	sd *sessiondata.SessionData,
	id descpb.ID,
	enumLabels tree.EnumValueList,
	dbDesc catalog.DatabaseDescriptor,
	schema catalog.SchemaDescriptor,
	typeName *tree.TypeName,
	enumType EnumType,
) (*typedesc.Mutable, error) {
	// Ensure there are no duplicates in the input enum values.
	seenVals := make(map[tree.EnumValue]struct{})
	for _, value := range enumLabels {
		_, ok := seenVals[value]
		if ok {
			return nil, pgerror.Newf(pgcode.InvalidObjectDefinition,
				"enum definition contains duplicate value %q", value)
		}
		seenVals[value] = struct{}{}
	}

	members := make([]descpb.TypeDescriptor_EnumMember, len(enumLabels))
	physReps := enum.GenerateNEvenlySpacedBytes(len(enumLabels))
	for i := range enumLabels {
		members[i] = descpb.TypeDescriptor_EnumMember{
			LogicalRepresentation:  string(enumLabels[i]),
			PhysicalRepresentation: physReps[i],
			Capability:             descpb.TypeDescriptor_EnumMember_ALL,
		}
	}

	privs, err := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		dbDesc.GetDefaultPrivilegeDescriptor(),
		schema.GetDefaultPrivilegeDescriptor(),
		dbDesc.GetID(),
		sd.User(),
		privilege.Types,
	)
	if err != nil {
		return nil, err
	}

	enumKind := descpb.TypeDescriptor_ENUM
	var regionConfig *descpb.TypeDescriptor_RegionConfig
	if enumType == EnumTypeMultiRegion {
		enumKind = descpb.TypeDescriptor_MULTIREGION_ENUM
		primaryRegion, err := dbDesc.PrimaryRegionName()
		if err != nil {
			return nil, err
		}
		regionConfig = &descpb.TypeDescriptor_RegionConfig{
			PrimaryRegion: primaryRegion,
		}
	}

	// TODO (rohany): OID's are computed using an offset of
	//  oidext.CockroachPredefinedOIDMax from the descriptor ID. Once we have
	//  a free list of descriptor ID's (#48438), we should allocate an ID from
	//  there if id + oidext.CockroachPredefinedOIDMax overflows past the
	//  maximum uint32 value.
	return typedesc.NewBuilder(&descpb.TypeDescriptor{
		Name:           typeName.Type(),
		ID:             id,
		ParentID:       dbDesc.GetID(),
		ParentSchemaID: schema.GetID(),
		Kind:           enumKind,
		EnumMembers:    members,
		Version:        1,
		Privileges:     privs,
		RegionConfig:   regionConfig,
	}).BuildCreatedMutableType(), nil
}

// createCompositeTypeDesc creates a new composite type descriptor.
func createCompositeTypeDesc(
	params runParams,
	id descpb.ID,
	compositeTypeList []tree.CompositeTypeElem,
	dbDesc catalog.DatabaseDescriptor,
	schema catalog.SchemaDescriptor,
	typeName *tree.TypeName,
) (*typedesc.Mutable, error) {
	// Ensure there are no duplicates in the input enum values.
	seenLabels := make(map[tree.Name]struct{})
	elts := make([]descpb.TypeDescriptor_Composite_CompositeElement, len(compositeTypeList))
	for i, value := range compositeTypeList {
		_, ok := seenLabels[value.Label]
		if ok {
			return nil, pgerror.Newf(pgcode.InvalidObjectDefinition,
				"composite type definition contains duplicate label %q", value)
		}
		elts[i].ElementLabel = string(value.Label)
		typ, err := tree.ResolveType(params.ctx, value.Type, params.p.semaCtx.TypeResolver)
		if err != nil {
			return nil, err
		}
		if typ.Identical(types.Trigger) {
			return nil, tree.CannotAcceptTriggerErr
		}
		if err = tree.CheckUnsupportedType(params.ctx, &params.p.semaCtx, typ); err != nil {
			return nil, err
		}
		if typ.UserDefined() {
			return nil, unimplemented.NewWithIssue(91779,
				"composite types that reference user-defined types not yet supported")
		}
		if typ.TypeMeta.ImplicitRecordType {
			return nil, unimplemented.NewWithIssue(70099,
				"cannot use table record type as part of composite type")
		}
		elts[i].ElementType = typ
		seenLabels[value.Label] = struct{}{}
	}

	privs, err := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		dbDesc.GetDefaultPrivilegeDescriptor(),
		schema.GetDefaultPrivilegeDescriptor(),
		dbDesc.GetID(),
		params.SessionData().User(),
		privilege.Types,
	)
	if err != nil {
		return nil, err
	}

	return typedesc.NewBuilder(&descpb.TypeDescriptor{
		Name:           typeName.Type(),
		ID:             id,
		ParentID:       dbDesc.GetID(),
		ParentSchemaID: schema.GetID(),
		Kind:           descpb.TypeDescriptor_COMPOSITE,
		Composite: &descpb.TypeDescriptor_Composite{
			Elements: elts,
		},
		Version:    1,
		Privileges: privs,
	}).BuildCreatedMutableType(), nil
}

func (p *planner) createEnumWithID(
	ctx context.Context,
	evalCtx *eval.Context,
	id descpb.ID,
	enumLabels tree.EnumValueList,
	dbDesc catalog.DatabaseDescriptor,
	typeName *tree.TypeName,
	enumType EnumType,
) error {
	sqltelemetry.IncrementEnumCounter(sqltelemetry.EnumCreate)

	// Generate a key in the namespace table and a new id for this type.
	schema, err := getCreateTypeParams(ctx, p, typeName, dbDesc)
	if err != nil {
		return err
	}

	typeDesc, err := CreateEnumTypeDesc(p.SessionData(), id, enumLabels, dbDesc, schema, typeName, enumType)
	if err != nil {
		return err
	}

	return p.finishCreateType(ctx, evalCtx, typeName, typeDesc, dbDesc, schema)
}

func (p *planner) createCompositeWithID(
	params runParams,
	id descpb.ID,
	compositeTypeList []tree.CompositeTypeElem,
	dbDesc catalog.DatabaseDescriptor,
	typeName *tree.TypeName,
) error {
	// Generate a key in the namespace table and a new id for this type.
	schema, err := getCreateTypeParams(params.ctx, p, typeName, dbDesc)
	if err != nil {
		return err
	}

	typeDesc, err := createCompositeTypeDesc(params, id, compositeTypeList, dbDesc, schema, typeName)
	if err != nil {
		return err
	}

	if err := p.finishCreateType(params.ctx, params.EvalContext(), typeName, typeDesc, dbDesc, schema); err != nil {
		return err
	}
	// Install back references to types used by this type.
	if err := p.addBackRefsFromAllTypesInType(params.ctx, typeDesc); err != nil {
		return err
	}
	return nil
}

func (p *planner) finishCreateType(
	ctx context.Context,
	evalCtx *eval.Context,
	typeName *tree.TypeName,
	typeDesc *typedesc.Mutable,
	dbDesc catalog.DatabaseDescriptor,
	schema catalog.SchemaDescriptor,
) error {
	// Create the implicit array type for this type before finishing the type.
	arrayTypeID, err := p.createArrayType(ctx, evalCtx, typeName, typeDesc, dbDesc, schema.GetID())
	if err != nil {
		return err
	}

	// Update the typeDesc with the created array type ID.
	typeDesc.ArrayTypeID = arrayTypeID

	// Now create the type after the implicit array type as been created.
	if err := p.createDescriptor(ctx, typeDesc, typeName.String()); err != nil {
		return err
	}

	// Log the event.
	return p.logEvent(
		ctx,
		typeDesc.GetID(),
		&eventpb.CreateType{
			TypeName: typeName.FQString(),
		})
}

func (n *createTypeNode) Next(params runParams) (bool, error) { return false, nil }
func (n *createTypeNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *createTypeNode) Close(ctx context.Context)           {}
func (n *createTypeNode) ReadingOwnWrites()                   {}
