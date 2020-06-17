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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

type createTypeNode struct {
	n *tree.CreateType
}

// Use to satisfy the linter.
var _ planNode = &createTypeNode{n: nil}

func (p *planner) CreateType(ctx context.Context, n *tree.CreateType) (planNode, error) {
	return &createTypeNode{n: n}, nil
}

func (n *createTypeNode) startExec(params runParams) error {
	switch n.n.Variety {
	case tree.Enum:
		return params.p.createEnum(params, n.n)
	default:
		return unimplemented.NewWithIssue(25123, "CREATE TYPE")
	}
}

func resolveNewTypeName(
	params runParams, name *tree.UnresolvedObjectName,
) (*tree.TypeName, *sqlbase.ImmutableDatabaseDescriptor, error) {
	// Resolve the target schema and database.
	db, prefix, err := params.p.ResolveUncachedDatabase(params.ctx, name)
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

	typename := tree.NewUnqualifiedTypeName(tree.Name(name.Object()))
	typename.ObjectNamePrefix = prefix
	return typename, db, nil
}

// getCreateTypeParams performs some initial validation on the input new
// TypeName and returns the key for the new type descriptor, the ID of the
// new type, the parent database and parent schema id.
func getCreateTypeParams(
	params runParams, name *tree.TypeName, db *sqlbase.ImmutableDatabaseDescriptor,
) (sqlbase.DescriptorKey, sqlbase.ID, error) {
	// TODO (rohany): This should be named object key.
	typeKey := sqlbase.MakePublicTableNameKey(params.ctx, params.ExecCfg().Settings, db.GetID(), name.Type())
	// As of now, we can only create types in the public schema.
	schemaID := sqlbase.ID(keys.PublicSchemaID)
	exists, collided, err := sqlbase.LookupObjectID(
		params.ctx, params.p.txn, params.ExecCfg().Codec, db.GetID(), schemaID, name.Type())
	if err == nil && exists {
		// Try and see what kind of object we collided with.
		desc, err := catalogkv.GetDescriptorByID(params.ctx, params.p.txn, params.ExecCfg().Codec, collided)
		if err != nil {
			return nil, 0, err
		}
		return nil, 0, sqlbase.MakeObjectAlreadyExistsError(desc.DescriptorProto(), name.String())
	}
	if err != nil {
		return nil, 0, err
	}
	id, err := catalogkv.GenerateUniqueDescID(params.ctx, params.ExecCfg().DB, params.ExecCfg().Codec)
	if err != nil {
		return nil, 0, err
	}
	return typeKey, id, nil
}

// createArrayType performs the implicit array type creation logic of Postgres.
// When a type is created in Postgres, Postgres will implicitly create an array
// type of that user defined type. This array type tracks changes to the
// original type, and is dropped when the original type is dropped.
// createArrayType creates the implicit array type for the input TypeDescriptor
// and returns the ID of the created type.
func (p *planner) createArrayType(
	params runParams,
	n *tree.CreateType,
	typ *tree.TypeName,
	typDesc *sqlbase.MutableTypeDescriptor,
	db *sqlbase.ImmutableDatabaseDescriptor,
) (sqlbase.ID, error) {
	// Postgres starts off trying to create the type as _<typename>. It then
	// continues adding "_" to the front of the name until it doesn't find
	// a collision.
	schemaID := sqlbase.ID(keys.PublicSchemaID)
	arrayTypeName := "_" + typ.Type()
	var arrayTypeKey sqlbase.DescriptorKey
	for {
		// See if there is a collision with the current name.
		exists, _, err := sqlbase.LookupObjectID(
			params.ctx,
			params.p.txn,
			params.ExecCfg().Codec,
			db.GetID(),
			schemaID,
			arrayTypeName,
		)
		if err != nil {
			return 0, err
		}
		// If we found an empty spot, then create the namespace key for this entry.
		if !exists {
			arrayTypeKey = sqlbase.MakePublicTableNameKey(
				params.ctx,
				params.ExecCfg().Settings,
				db.GetID(),
				arrayTypeName,
			)
			break
		}
		// Otherwise, append another "_" to the front of the name.
		arrayTypeName = "_" + arrayTypeName
	}

	// Generate the stable ID for the array type.
	id, err := catalogkv.GenerateUniqueDescID(params.ctx, params.ExecCfg().DB, params.ExecCfg().Codec)
	if err != nil {
		return 0, err
	}

	// Create the element type for the array. Note that it must know about the
	// ID of the array type in order for the array type to correctly created.
	var elemTyp *types.T
	switch t := typDesc.Kind; t {
	case sqlbase.TypeDescriptor_ENUM:
		elemTyp = types.MakeEnum(uint32(typDesc.GetID()), uint32(id))
	default:
		return 0, errors.AssertionFailedf("cannot make array type for kind %s", t.String())
	}

	// Construct the descriptor for the array type.
	// TODO(ajwerner): This is getting fixed up in a later commit to deal with
	// meta, just hold on.
	arrayTypDesc := sqlbase.NewMutableCreatedTypeDescriptor(sqlbase.TypeDescriptor{
		Name:           arrayTypeName,
		ID:             id,
		ParentID:       db.GetID(),
		ParentSchemaID: keys.PublicSchemaID,
		Kind:           sqlbase.TypeDescriptor_ALIAS,
		Alias:          types.MakeArray(elemTyp),
	})

	jobStr := fmt.Sprintf("implicit array type creation for %s", tree.AsStringWithFQNames(n, params.Ann()))
	if err := p.createDescriptorWithID(
		params.ctx,
		arrayTypeKey.Key(params.ExecCfg().Codec),
		id,
		arrayTypDesc,
		params.EvalContext().Settings,
		jobStr,
	); err != nil {
		return 0, err
	}
	return id, nil
}

func (p *planner) createEnum(params runParams, n *tree.CreateType) error {
	// Make sure that all nodes in the cluster are able to recognize ENUM types.
	if !p.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.VersionEnums) {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"not all nodes are the correct version for ENUM type creation")
	}

	// Check that usage of ENUM types is enabled.
	if !p.EvalContext().SessionData.EnumsEnabled {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"session variable experimental_enable_enums is set to false, cannot create an enum")
	}

	// Ensure there are no duplicates in the input enum values.
	seenVals := make(map[string]struct{})
	for _, value := range n.EnumLabels {
		_, ok := seenVals[value]
		if ok {
			return pgerror.Newf(pgcode.InvalidObjectDefinition,
				"enum definition contains duplicate value %q", value)
		}
		seenVals[value] = struct{}{}
	}

	// Resolve the desired new type name.
	typeName, db, err := resolveNewTypeName(params, n.TypeName)
	if err != nil {
		return err
	}
	n.TypeName.SetAnnotation(&p.semaCtx.Annotations, typeName)

	// Generate a key in the namespace table and a new id for this type.
	typeKey, id, err := getCreateTypeParams(params, typeName, db)
	if err != nil {
		return err
	}

	members := make([]sqlbase.TypeDescriptor_EnumMember, len(n.EnumLabels))
	physReps := enum.GenerateNEvenlySpacedBytes(len(n.EnumLabels))
	for i := range n.EnumLabels {
		members[i] = sqlbase.TypeDescriptor_EnumMember{
			LogicalRepresentation:  n.EnumLabels[i],
			PhysicalRepresentation: physReps[i],
		}
	}

	// TODO (rohany): OID's are computed using an offset of
	//  oidext.CockroachPredefinedOIDMax from the descriptor ID. Once we have
	//  a free list of descriptor ID's (#48438), we should allocate an ID from
	//  there if id + oidext.CockroachPredefinedOIDMax overflows past the
	//  maximum uint32 value.
	typeDesc := sqlbase.NewMutableCreatedTypeDescriptor(sqlbase.TypeDescriptor{
		Name:           typeName.Type(),
		ID:             id,
		ParentID:       db.GetID(),
		ParentSchemaID: keys.PublicSchemaID,
		Kind:           sqlbase.TypeDescriptor_ENUM,
		EnumMembers:    members,
	})

	// Create the implicit array type for this type before finishing the type.
	arrayTypeID, err := p.createArrayType(params, n, typeName, typeDesc, db)
	if err != nil {
		return err
	}

	// Update the typeDesc with the created array type ID.
	typeDesc.ArrayTypeID = arrayTypeID

	// Now create the type after the implicit array type as been created.
	return p.createDescriptorWithID(
		params.ctx,
		typeKey.Key(params.ExecCfg().Codec),
		id,
		typeDesc,
		params.EvalContext().Settings,
		tree.AsStringWithFQNames(n, params.Ann()),
	)
}

func (n *createTypeNode) Next(params runParams) (bool, error) { return false, nil }
func (n *createTypeNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *createTypeNode) Close(ctx context.Context)           {}
func (n *createTypeNode) ReadingOwnWrites()                   {}
