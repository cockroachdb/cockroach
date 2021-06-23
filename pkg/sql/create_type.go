// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type createTypeNode struct {
	n        *tree.CreateType
	typeName *tree.TypeName
	dbDesc   catalog.DatabaseDescriptor
}

type enumType int

const (
	enumTypeUserDefined = iota
	enumTypeMultiRegion
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
	typeName, db, err := resolveNewTypeName(p.RunParams(ctx), n.TypeName)
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
	// Check if a type with the same name exists already.
	flags := tree.ObjectLookupFlags{CommonLookupFlags: tree.CommonLookupFlags{
		Required:    false,
		AvoidCached: true,
	}}
	found, _, err := params.p.Descriptors().GetImmutableTypeByName(params.ctx, params.p.Txn(), n.typeName, flags)
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
	if found && n.n.IfNotExists {
		params.p.BufferClientNotice(
			params.ctx,
			pgnotice.Newf("type %q already exists, skipping", n.typeName),
		)
		return nil
	}

	switch n.n.Variety {
	case tree.Enum:
		return params.p.createUserDefinedEnum(params, n)
	default:
		return unimplemented.NewWithIssue(25123, "CREATE TYPE")
	}
}

func resolveNewTypeName(
	params runParams, name *tree.UnresolvedObjectName,
) (*tree.TypeName, catalog.DatabaseDescriptor, error) {
	// Resolve the target schema and database.
	db, _, prefix, err := params.p.ResolveTargetObject(params.ctx, name)
	if err != nil {
		return nil, nil, err
	}

	if err := params.p.CheckPrivilege(params.ctx, db, privilege.CREATE); err != nil {
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
	params runParams, name *tree.TypeName, db catalog.DatabaseDescriptor,
) (schema catalog.SchemaDescriptor, err error) {
	// Check we are not creating a type which conflicts with an alias available
	// as a built-in type in CockroachDB but an extension type on the public
	// schema for PostgreSQL.
	if name.Schema() == tree.PublicSchema {
		if _, ok := types.PublicSchemaAliases[name.Object()]; ok {
			return nil, sqlerrors.NewTypeAlreadyExistsError(name.String())
		}
	}
	// Get the ID of the schema the type is being created in.
	dbID := db.GetID()
	schema, err = params.p.getNonTemporarySchemaForCreate(params.ctx, db, name.Schema())
	if err != nil {
		return nil, err
	}

	// Check permissions on the schema.
	if err := params.p.canCreateOnSchema(
		params.ctx, schema.GetID(), dbID, params.p.User(), skipCheckPublicSchema); err != nil {
		return nil, err
	}

	if schema.SchemaKind() == catalog.SchemaUserDefined {
		sqltelemetry.IncrementUserDefinedSchemaCounter(sqltelemetry.UserDefinedSchemaUsedByObject)
	}

	err = catalogkv.CheckObjectCollision(
		params.ctx,
		params.p.txn,
		params.ExecCfg().Codec,
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
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, parentID, schemaID descpb.ID, name string,
) (string, error) {
	arrayName := "_" + name
	for {
		// See if there is a collision with the current name.
		exists, _, err := catalogkv.LookupObjectID(
			ctx,
			txn,
			codec,
			parentID,
			schemaID,
			arrayName,
		)
		if err != nil {
			return "", err
		}
		// If we found an empty spot, then break out.
		if !exists {
			break
		}
		// Otherwise, append another "_" to the front of the name.
		arrayName = "_" + arrayName
	}
	return arrayName, nil
}

// createArrayType performs the implicit array type creation logic of Postgres.
// When a type is created in Postgres, Postgres will implicitly create an array
// type of that user defined type. This array type tracks changes to the
// original type, and is dropped when the original type is dropped.
// createArrayType creates the implicit array type for the input TypeDescriptor
// and returns the ID of the created type.
func (p *planner) createArrayType(
	params runParams,
	typ *tree.TypeName,
	typDesc *typedesc.Mutable,
	db catalog.DatabaseDescriptor,
	schemaID descpb.ID,
) (descpb.ID, error) {
	arrayTypeName, err := findFreeArrayTypeName(
		params.ctx,
		params.p.txn,
		params.ExecCfg().Codec,
		db.GetID(),
		schemaID,
		typ.Type(),
	)
	if err != nil {
		return 0, err
	}
	arrayTypeKey := catalogkeys.MakeObjectNameKey(params.ExecCfg().Codec, db.GetID(), schemaID, arrayTypeName)

	// Generate the stable ID for the array type.
	id, err := catalogkv.GenerateUniqueDescID(params.ctx, params.ExecCfg().DB, params.ExecCfg().Codec)
	if err != nil {
		return 0, err
	}

	// Create the element type for the array. Note that it must know about the
	// ID of the array type in order for the array type to correctly created.
	var elemTyp *types.T
	switch t := typDesc.Kind; t {
	case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
		elemTyp = types.MakeEnum(typedesc.TypeIDToOID(typDesc.GetID()), typedesc.TypeIDToOID(id))
	default:
		return 0, errors.AssertionFailedf("cannot make array type for kind %s", t.String())
	}

	// Construct the descriptor for the array type.
	// TODO(ajwerner): This is getting fixed up in a later commit to deal with
	// meta, just hold on.
	arrayTypDesc := typedesc.NewBuilder(&descpb.TypeDescriptor{
		Name:           arrayTypeName,
		ID:             id,
		ParentID:       db.GetID(),
		ParentSchemaID: schemaID,
		Kind:           descpb.TypeDescriptor_ALIAS,
		Alias:          types.MakeArray(elemTyp),
		Version:        1,
		Privileges:     typDesc.Privileges,
	}).BuildCreatedMutable()

	jobStr := fmt.Sprintf("implicit array type creation for %s", typ)
	if err := p.createDescriptorWithID(
		params.ctx,
		arrayTypeKey,
		id,
		arrayTypDesc,
		params.EvalContext().Settings,
		jobStr,
	); err != nil {
		return 0, err
	}
	return id, nil
}

func (p *planner) createUserDefinedEnum(params runParams, n *createTypeNode) error {
	// Generate a stable ID for the new type.
	id, err := catalogkv.GenerateUniqueDescID(
		params.ctx, params.ExecCfg().DB, params.ExecCfg().Codec,
	)
	if err != nil {
		return err
	}
	return params.p.createEnumWithID(
		params, id, n.n.EnumLabels, n.dbDesc, n.typeName, enumTypeUserDefined,
	)
}

func (p *planner) createEnumWithID(
	params runParams,
	id descpb.ID,
	enumLabels tree.EnumValueList,
	dbDesc catalog.DatabaseDescriptor,
	typeName *tree.TypeName,
	enumType enumType,
) error {

	sqltelemetry.IncrementEnumCounter(sqltelemetry.EnumCreate)

	// Ensure there are no duplicates in the input enum values.
	seenVals := make(map[tree.EnumValue]struct{})
	for _, value := range enumLabels {
		_, ok := seenVals[value]
		if ok {
			return pgerror.Newf(pgcode.InvalidObjectDefinition,
				"enum definition contains duplicate value %q", value)
		}
		seenVals[value] = struct{}{}
	}

	// Generate a key in the namespace table and a new id for this type.
	schema, err := getCreateTypeParams(params, typeName, dbDesc)
	if err != nil {
		return err
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

	// Database privileges and Type privileges do not overlap so there is nothing
	// to inherit.
	// However having USAGE on a parent schema of the type
	// gives USAGE privilege to the type.
	privs := descpb.NewDefaultPrivilegeDescriptor(params.p.User())

	inheritUsagePrivilegeFromSchema(schema, privs)
	privs.Grant(params.p.User(), privilege.List{privilege.ALL})

	enumKind := descpb.TypeDescriptor_ENUM
	var regionConfig *descpb.TypeDescriptor_RegionConfig
	if enumType == enumTypeMultiRegion {
		enumKind = descpb.TypeDescriptor_MULTIREGION_ENUM
		primaryRegion, err := dbDesc.PrimaryRegionName()
		if err != nil {
			return err
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
	typeDesc := typedesc.NewBuilder(&descpb.TypeDescriptor{
		Name:           typeName.Type(),
		ID:             id,
		ParentID:       dbDesc.GetID(),
		ParentSchemaID: schema.GetID(),
		Kind:           enumKind,
		EnumMembers:    members,
		Version:        1,
		Privileges:     privs,
		RegionConfig:   regionConfig,
	}).BuildCreatedMutableType()

	// Create the implicit array type for this type before finishing the type.
	arrayTypeID, err := p.createArrayType(params, typeName, typeDesc, dbDesc, schema.GetID())
	if err != nil {
		return err
	}

	// Update the typeDesc with the created array type ID.
	typeDesc.ArrayTypeID = arrayTypeID

	// Now create the type after the implicit array type as been created.
	if err := p.createDescriptorWithID(
		params.ctx,
		catalogkeys.MakeObjectNameKey(params.ExecCfg().Codec, dbDesc.GetID(), schema.GetID(), typeName.Type()),
		id,
		typeDesc,
		params.EvalContext().Settings,
		typeName.String(),
	); err != nil {
		return err
	}

	// Log the event.
	return p.logEvent(params.ctx,
		typeDesc.GetID(),
		&eventpb.CreateType{
			TypeName: typeName.FQString(),
		})
}

func (n *createTypeNode) Next(params runParams) (bool, error) { return false, nil }
func (n *createTypeNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *createTypeNode) Close(ctx context.Context)           {}
func (n *createTypeNode) ReadingOwnWrites()                   {}

// TODO(richardjcai): Instead of inheriting the privilege when creating the
// descriptor, we can check the parent of type for usage privilege as well,
// this seems to be how Postgres does it.
// Add a test for this when we support granting privileges to schemas #50879.
func inheritUsagePrivilegeFromSchema(
	schema catalog.SchemaDescriptor, privs *descpb.PrivilegeDescriptor,
) {

	switch kind := schema.SchemaKind(); kind {
	case catalog.SchemaPublic:
		// If the type is in the public schema, the public role has USAGE on it.
		privs.Grant(security.PublicRoleName(), privilege.List{privilege.USAGE})
	case catalog.SchemaTemporary, catalog.SchemaVirtual:
		// No types should be created in a temporary schema or a virtual schema.
		panic(errors.AssertionFailedf(
			"type being created in schema kind %d with id %d",
			kind, schema.GetID()))
	case catalog.SchemaUserDefined:
		schemaPrivs := schema.GetPrivileges()

		// Look for all users that have USAGE on the schema and add it to the
		// privilege descriptor.
		for _, u := range schemaPrivs.Users {
			if u.Privileges&privilege.USAGE.Mask() == 1 {
				privs.Grant(u.User(), privilege.List{privilege.USAGE})
			}
		}
	default:
		panic(errors.AssertionFailedf("unknown schema kind %d", kind))
	}
}
