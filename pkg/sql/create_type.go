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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
) (*tree.TypeName, *DatabaseDescriptor, error) {
	// Resolve the target schema and database.
	db, prefix, err := params.p.ResolveUncachedDatabase(params.ctx, name)
	if err != nil {
		return nil, nil, err
	}

	if err := params.p.CheckPrivilege(params.ctx, db, privilege.CREATE); err != nil {
		return nil, nil, err
	}

	// Disallow type creation in the system database.
	if db.ID == keys.SystemDatabaseID {
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
	params runParams, name *tree.TypeName, db *DatabaseDescriptor,
) (sqlbase.DescriptorKey, sqlbase.ID, error) {
	// TODO (rohany): This should be named object key.
	typeKey := sqlbase.MakePublicTableNameKey(params.ctx, params.ExecCfg().Settings, db.ID, name.Type())
	// As of now, we can only create types in the public schema.
	schemaID := sqlbase.ID(keys.PublicSchemaID)
	exists, collided, err := sqlbase.LookupObjectID(
		params.ctx, params.p.txn, params.ExecCfg().Codec, db.ID, schemaID, name.Type())
	if err == nil && exists {
		// Try and see what kind of object we collided with.
		desc, err := catalogkv.GetDescriptorByID(params.ctx, params.p.txn, params.ExecCfg().Codec, collided)
		if err != nil {
			return nil, 0, err
		}
		return nil, 0, makeObjectAlreadyExistsError(desc, name.String())
	}
	if err != nil {
		return nil, 0, err
	}
	id, err := GenerateUniqueDescID(params.ctx, params.ExecCfg().DB, params.ExecCfg().Codec)
	if err != nil {
		return nil, 0, err
	}
	return typeKey, id, nil
}

func (p *planner) createEnum(params runParams, n *tree.CreateType) error {
	// Make sure that all nodes in the cluster are able to recognize ENUM types.
	if !p.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.VersionEnums) {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"not all nodes are the correct version for ENUM type creation")
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

	// TODO (rohany): We need to generate an oid for this type!
	typeDesc := &sqlbase.TypeDescriptor{
		ParentID:       db.ID,
		ParentSchemaID: keys.PublicSchemaID,
		Name:           typeName.Type(),
		ID:             id,
		Kind:           sqlbase.TypeDescriptor_ENUM,
		EnumMembers:    members,
	}

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
