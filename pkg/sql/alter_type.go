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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

type alterTypeNode struct {
	n    *tree.AlterType
	desc *sqlbase.MutableTypeDescriptor
}

// alterTypeNode implements planNode. We set n here to satisfy the linter.
var _ planNode = &alterTypeNode{n: nil}

func (p *planner) AlterType(ctx context.Context, n *tree.AlterType) (planNode, error) {
	// Resolve the type.
	desc, err := p.ResolveMutableTypeDescriptor(ctx, n.Type, true /* required */)
	if err != nil {
		return nil, err
	}

	if err := p.canModifyType(ctx, desc); err != nil {
		return nil, err
	}

	// The implicit array types are not modifiable.
	if desc.Kind == descpb.TypeDescriptor_ALIAS {
		return nil, pgerror.Newf(
			pgcode.WrongObjectType,
			"%q is an implicit array type and cannot be modified",
			tree.AsStringWithFQNames(n.Type, &p.semaCtx.Annotations),
		)
	}
	// TODO (rohany): Check permissions here once we track them.
	return &alterTypeNode{
		n:    n,
		desc: desc,
	}, nil
}

func (n *alterTypeNode) startExec(params runParams) error {
	var err error
	switch t := n.n.Cmd.(type) {
	case *tree.AlterTypeAddValue:
		err = params.p.addEnumValue(params.ctx, n, t)
	case *tree.AlterTypeRenameValue:
		err = params.p.renameTypeValue(params.ctx, n, t.OldVal, t.NewVal)
	case *tree.AlterTypeRename:
		err = params.p.renameType(params.ctx, n, t.NewName)
	case *tree.AlterTypeSetSchema:
		err = params.p.setTypeSchema(params.ctx, n, t.Schema)
	case *tree.AlterTypeOwner:
		err = params.p.alterTypeOwner(params.ctx, n, t.Owner)
	default:
		err = errors.AssertionFailedf("unknown alter type cmd %s", t)
	}
	if err != nil {
		return err
	}
	return n.desc.Validate(params.ctx, params.p.txn, params.ExecCfg().Codec)
}

func (p *planner) addEnumValue(
	ctx context.Context, n *alterTypeNode, node *tree.AlterTypeAddValue,
) error {
	if err := n.desc.AddEnumValue(node); err != nil {
		return err
	}
	return p.writeTypeSchemaChange(
		ctx,
		n.desc,
		tree.AsStringWithFQNames(n.n, p.Ann()),
	)
}

func (p *planner) renameType(ctx context.Context, n *alterTypeNode, newName string) error {
	// See if there is a name collision with the new name.
	exists, id, err := catalogkv.LookupObjectID(
		ctx,
		p.txn,
		p.ExecCfg().Codec,
		n.desc.ParentID,
		n.desc.ParentSchemaID,
		newName,
	)
	if err == nil && exists {
		// Try and see what kind of object we collided with.
		desc, err := catalogkv.GetAnyDescriptorByID(ctx, p.txn, p.ExecCfg().Codec, id, catalogkv.Immutable)
		if err != nil {
			return sqlbase.WrapErrorWhileConstructingObjectAlreadyExistsErr(err)
		}
		return sqlbase.MakeObjectAlreadyExistsError(desc.DescriptorProto(), newName)
	} else if err != nil {
		return err
	}

	// Rename the base descriptor.
	if err := p.performRenameTypeDesc(
		ctx,
		n.desc,
		newName,
		tree.AsStringWithFQNames(n.n, p.Ann()),
	); err != nil {
		return err
	}

	// Now rename the array type.
	newArrayName, err := findFreeArrayTypeName(
		ctx,
		p.txn,
		p.ExecCfg().Codec,
		n.desc.ParentID,
		n.desc.ParentSchemaID,
		newName,
	)
	if err != nil {
		return err
	}
	arrayDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, n.desc.ArrayTypeID)
	if err != nil {
		return err
	}
	if err := p.performRenameTypeDesc(
		ctx,
		arrayDesc,
		newArrayName,
		tree.AsStringWithFQNames(n.n, p.Ann()),
	); err != nil {
		return err
	}
	return nil
}

func (p *planner) performRenameTypeDesc(
	ctx context.Context, desc *sqlbase.MutableTypeDescriptor, newName string, jobDesc string,
) error {
	// Record the rename details in the descriptor for draining.
	name := descpb.NameInfo{
		ParentID:       desc.ParentID,
		ParentSchemaID: desc.ParentSchemaID,
		Name:           desc.Name,
	}
	desc.AddDrainingName(name)

	// Set the descriptor up with the new name.
	desc.Name = newName
	if err := p.writeTypeSchemaChange(ctx, desc, jobDesc); err != nil {
		return err
	}
	// Construct the new namespace key.
	key := catalogkv.MakeObjectNameKey(
		ctx,
		p.ExecCfg().Settings,
		desc.ParentID,
		desc.ParentSchemaID,
		newName,
	)

	return p.writeNameKey(ctx, key, desc.ID)
}

func (p *planner) renameTypeValue(
	ctx context.Context, n *alterTypeNode, oldVal string, newVal string,
) error {
	enumMemberIndex := -1

	// Do one pass to verify that the oldVal exists and there isn't already
	// a member that is named newVal.
	for i := range n.desc.EnumMembers {
		member := n.desc.EnumMembers[i]
		if member.LogicalRepresentation == oldVal {
			enumMemberIndex = i
		} else if member.LogicalRepresentation == newVal {
			return pgerror.Newf(pgcode.DuplicateObject,
				"enum label %s already exists", newVal)
		}
	}

	// An enum member with the name oldVal was not found.
	if enumMemberIndex == -1 {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"%s is not an existing enum label", oldVal)
	}

	n.desc.EnumMembers[enumMemberIndex].LogicalRepresentation = newVal

	return p.writeTypeSchemaChange(
		ctx,
		n.desc,
		tree.AsStringWithFQNames(n.n, p.Ann()),
	)
}

func (p *planner) setTypeSchema(ctx context.Context, n *alterTypeNode, schema string) error {
	typeDesc := n.desc
	schemaID := typeDesc.GetParentSchemaID()
	databaseID := typeDesc.GetParentID()

	desiredSchemaID, err := p.prepareSetSchema(ctx, typeDesc, schema)
	if err != nil {
		return err
	}

	// If the schema being changed to is the same as the current schema for the
	// type, do a no-op.
	if desiredSchemaID == schemaID {
		return nil
	}

	name := descpb.NameInfo{
		ParentID:       databaseID,
		ParentSchemaID: schemaID,
		Name:           typeDesc.Name,
	}
	typeDesc.AddDrainingName(name)

	// Set the tableDesc's new schema id to the desired schema's id.
	typeDesc.SetParentSchemaID(desiredSchemaID)

	if err := p.writeTypeSchemaChange(
		ctx, typeDesc, tree.AsStringWithFQNames(n.n, p.Ann()),
	); err != nil {
		return err
	}

	newKey := catalogkv.MakeObjectNameKey(ctx, p.ExecCfg().Settings,
		databaseID, desiredSchemaID, typeDesc.Name)

	return p.writeNameKey(ctx, newKey, typeDesc.ID)
}

func (p *planner) alterTypeOwner(ctx context.Context, n *alterTypeNode, newOwner string) error {
	typeDesc := n.desc
	privs := typeDesc.GetPrivileges()

	// If the owner we want to set to is the current owner, do a no-op.
	if newOwner == privs.Owner {
		return nil
	}

	// Make sure the newOwner exists.
	roleExists, err := p.RoleExists(ctx, newOwner)
	if err != nil {
		return err
	}
	if !roleExists {
		return pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", newOwner)
	}

	// The user explicitly has to be owner to change the owner.
	if ok := IsOwner(typeDesc, p.User()); !ok {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of type %s", tree.Name(typeDesc.GetName()))
	}

	// Requirements from PG:
	// To alter the owner, you must also be a direct or indirect member of the new owning role,
	// and that role must have CREATE privilege on the type's schema.
	memberOf, err := p.MemberOfWithAdminOption(ctx, privs.Owner)
	if err != nil {
		return err
	}
	if _, ok := memberOf[newOwner]; !ok {
		return pgerror.Newf(
			pgcode.InsufficientPrivilege, "must be member of role %s", newOwner)
	}

	// Ensure the new owner has CREATE privilege on the type's schema.
	// We do not have to check for the public schema.
	if typeDesc.ParentSchemaID != keys.RootNamespaceID && typeDesc.ParentSchemaID != keys.PublicSchemaID {
		schemaDesc, err := catalogkv.MustGetSchemaDescByID(
			ctx, p.Txn(), p.ExecCfg().Codec, typeDesc.ParentSchemaID,
		)
		if err != nil {
			return err
		}

		if err := p.CheckPrivilegeForUser(ctx, schemaDesc, privilege.CREATE, newOwner); err != nil {
			return err
		}
	}

	privs.SetOwner(newOwner)

	if err := p.writeTypeSchemaChange(
		ctx, typeDesc, tree.AsStringWithFQNames(n.n, p.Ann()),
	); err != nil {
		return err
	}

	return nil
}

func (n *alterTypeNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterTypeNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterTypeNode) Close(ctx context.Context)           {}
func (n *alterTypeNode) ReadingOwnWrites()                   {}

func (p *planner) canModifyType(ctx context.Context, desc *MutableTypeDescriptor) error {
	hasOwnership, err := p.HasOwnership(ctx, desc)
	if err != nil {
		return err
	}

	if !hasOwnership {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of type %s", tree.Name(desc.GetName()))
	}

	return nil
}
