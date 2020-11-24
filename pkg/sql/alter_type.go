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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

type alterTypeNode struct {
	n    *tree.AlterType
	desc *typedesc.Mutable
}

// alterTypeNode implements planNode. We set n here to satisfy the linter.
var _ planNode = &alterTypeNode{n: nil}

func (p *planner) AlterType(ctx context.Context, n *tree.AlterType) (planNode, error) {
	// Resolve the type.
	desc, err := p.ResolveMutableTypeDescriptor(ctx, n.Type, true /* required */)
	if err != nil {
		return nil, err
	}

	// The user needs ownership privilege to alter the type.
	if err := p.canModifyType(ctx, desc); err != nil {
		return nil, err
	}

	switch desc.Kind {
	case descpb.TypeDescriptor_ALIAS:
		// The implicit array types are not modifiable.
		return nil, pgerror.Newf(
			pgcode.WrongObjectType,
			"%q is an implicit array type and cannot be modified",
			tree.AsStringWithFQNames(n.Type, &p.semaCtx.Annotations),
		)
	case descpb.TypeDescriptor_ENUM:
		sqltelemetry.IncrementEnumCounter(sqltelemetry.EnumAlter)
	}

	return &alterTypeNode{
		n:    n,
		desc: desc,
	}, nil
}

func (n *alterTypeNode) startExec(params runParams) error {
	telemetry.Inc(n.n.Cmd.TelemetryCounter())
	var err error
	switch t := n.n.Cmd.(type) {
	case *tree.AlterTypeAddValue:
		err = params.p.addEnumValue(params.ctx, n, t)
	case *tree.AlterTypeRenameValue:
		err = params.p.renameTypeValue(params.ctx, n, string(t.OldVal), string(t.NewVal))
	case *tree.AlterTypeRename:
		err = params.p.renameType(params.ctx, n, string(t.NewName))
	case *tree.AlterTypeSetSchema:
		err = params.p.setTypeSchema(params.ctx, n, string(t.Schema))
	case *tree.AlterTypeOwner:
		err = params.p.alterTypeOwner(params.ctx, n, t.Owner)
	default:
		err = errors.AssertionFailedf("unknown alter type cmd %s", t)
	}
	if err != nil {
		return err
	}

	// Validate the type descriptor after the changes.
	dg := catalogkv.NewOneLevelUncachedDescGetter(params.p.txn, params.ExecCfg().Codec)
	if err := n.desc.Validate(params.ctx, dg); err != nil {
		return err
	}

	// Write a log event.
	return MakeEventLogger(params.p.ExecCfg()).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogAlterType,
		int32(n.desc.ID),
		int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
		struct {
			TypeName  string
			Statement string
			User      string
		}{
			n.desc.Name,
			tree.AsStringWithFQNames(n.n, params.Ann()),
			params.p.User().Normalized(),
		},
	)
}

func (p *planner) addEnumValue(
	ctx context.Context, n *alterTypeNode, node *tree.AlterTypeAddValue,
) error {
	if n.desc.Kind != descpb.TypeDescriptor_ENUM {
		return pgerror.Newf(pgcode.WrongObjectType, "%q is not an enum", n.desc.Name)
	}
	// See if the value already exists in the enum or not.
	for _, member := range n.desc.EnumMembers {
		if member.LogicalRepresentation == string(node.NewVal) {
			if node.IfNotExists {
				p.BufferClientNotice(
					ctx,
					pgnotice.Newf("enum label %q already exists, skipping", node.NewVal),
				)
				return nil
			}
			return pgerror.Newf(pgcode.DuplicateObject, "enum label %q already exists", node.NewVal)
		}
	}

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
			return sqlerrors.WrapErrorWhileConstructingObjectAlreadyExistsErr(err)
		}
		return sqlerrors.MakeObjectAlreadyExistsError(desc.DescriptorProto(), newName)
	} else if err != nil {
		return err
	}

	// Rename the base descriptor.
	if err := p.performRenameTypeDesc(
		ctx,
		n.desc,
		newName,
		n.desc.ParentSchemaID,
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
		arrayDesc.ParentSchemaID,
		tree.AsStringWithFQNames(n.n, p.Ann()),
	); err != nil {
		return err
	}
	return nil
}

// performRenameTypeDesc renames and/or sets the schema of a type descriptor.
// newName and newSchemaID may be the same as the current name and schemaid.
func (p *planner) performRenameTypeDesc(
	ctx context.Context,
	desc *typedesc.Mutable,
	newName string,
	newSchemaID descpb.ID,
	jobDesc string,
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
	// Set the descriptor to the new schema ID.
	desc.SetParentSchemaID(newSchemaID)
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

	desiredSchemaID, err := p.prepareSetSchema(ctx, typeDesc, schema)
	if err != nil {
		return err
	}

	// If the schema being changed to is the same as the current schema for the
	// type, do a no-op.
	if desiredSchemaID == schemaID {
		return nil
	}

	err = p.performRenameTypeDesc(
		ctx, typeDesc, typeDesc.Name, desiredSchemaID, tree.AsStringWithFQNames(n.n, p.Ann()),
	)

	if err != nil {
		return err
	}

	arrayDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, n.desc.ArrayTypeID)
	if err != nil {
		return err
	}

	return p.performRenameTypeDesc(
		ctx, arrayDesc, arrayDesc.Name, desiredSchemaID, tree.AsStringWithFQNames(n.n, p.Ann()),
	)
}

func (p *planner) alterTypeOwner(
	ctx context.Context, n *alterTypeNode, newOwner security.SQLUsername,
) error {
	typeDesc := n.desc

	privs := typeDesc.GetPrivileges()

	// If the owner we want to set to is the current owner, do a no-op.
	if newOwner == privs.Owner() {
		return nil
	}

	arrayDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, typeDesc.ArrayTypeID)
	if err != nil {
		return err
	}

	if err := p.checkCanAlterTypeAndSetNewOwner(ctx, typeDesc, arrayDesc, newOwner); err != nil {
		return err
	}

	if err := p.writeTypeSchemaChange(
		ctx, typeDesc, tree.AsStringWithFQNames(n.n, p.Ann()),
	); err != nil {
		return err
	}

	return p.writeTypeSchemaChange(
		ctx, arrayDesc, tree.AsStringWithFQNames(n.n, p.Ann()),
	)
}

// checkCanAlterTypeAndSetNewOwner handles privilege checking and setting new owner.
// Called in ALTER TYPE and REASSIGN OWNED BY.
func (p *planner) checkCanAlterTypeAndSetNewOwner(
	ctx context.Context,
	typeDesc *typedesc.Mutable,
	arrayTypeDesc *typedesc.Mutable,
	newOwner security.SQLUsername,
) error {
	if err := p.checkCanAlterToNewOwner(ctx, typeDesc, newOwner); err != nil {
		return err
	}

	// Ensure the new owner has CREATE privilege on the type's schema.
	if err := p.canCreateOnSchema(
		ctx, typeDesc.GetParentSchemaID(), typeDesc.ParentID, newOwner, checkPublicSchema); err != nil {
		return err
	}

	privs := typeDesc.GetPrivileges()
	privs.SetOwner(newOwner)

	// Also have to change the owner of the implicit array type.
	arrayTypeDesc.Privileges.SetOwner(newOwner)

	return nil
}

func (n *alterTypeNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterTypeNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterTypeNode) Close(ctx context.Context)           {}
func (n *alterTypeNode) ReadingOwnWrites()                   {}

func (p *planner) canModifyType(ctx context.Context, desc *typedesc.Mutable) error {
	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}
	if hasAdmin {
		return nil
	}

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
