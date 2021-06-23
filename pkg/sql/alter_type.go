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

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type alterTypeNode struct {
	n      *tree.AlterType
	prefix catalog.ResolvedObjectPrefix
	desc   *typedesc.Mutable
}

// alterTypeNode implements planNode. We set n here to satisfy the linter.
var _ planNode = &alterTypeNode{n: nil}

func (p *planner) AlterType(ctx context.Context, n *tree.AlterType) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER TYPE",
	); err != nil {
		return nil, err
	}

	// Resolve the type.
	prefix, desc, err := p.ResolveMutableTypeDescriptor(ctx, n.Type, true /* required */)
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
	case descpb.TypeDescriptor_MULTIREGION_ENUM:
		// Multi-region enums can't be directly modified.
		return nil, errors.WithHint(
			pgerror.Newf(
				pgcode.WrongObjectType,
				"%q is a multi-region enum and can't be modified using the alter type command",
				tree.AsStringWithFQNames(n.Type, &p.semaCtx.Annotations)),
			"try adding/removing the region using ALTER DATABASE")
	case descpb.TypeDescriptor_ENUM:
		sqltelemetry.IncrementEnumCounter(sqltelemetry.EnumAlter)
	}

	return &alterTypeNode{
		n:      n,
		prefix: prefix,
		desc:   desc,
	}, nil
}

func (n *alterTypeNode) startExec(params runParams) error {
	telemetry.Inc(n.n.Cmd.TelemetryCounter())

	typeName := tree.AsStringWithFQNames(n.n.Type, params.p.Ann())
	eventLogDone := false
	var err error
	switch t := n.n.Cmd.(type) {
	case *tree.AlterTypeAddValue:
		err = params.p.addEnumValue(params.ctx, n.desc, t, tree.AsStringWithFQNames(n.n, params.p.Ann()))
	case *tree.AlterTypeRenameValue:
		err = params.p.renameTypeValue(params.ctx, n, string(t.OldVal), string(t.NewVal))
	case *tree.AlterTypeRename:
		if err = params.p.renameType(params.ctx, n, string(t.NewName)); err != nil {
			return err
		}
		err = params.p.logEvent(params.ctx, n.desc.ID, &eventpb.RenameType{
			TypeName:    typeName,
			NewTypeName: string(t.NewName),
		})
		eventLogDone = true
	case *tree.AlterTypeSetSchema:
		// TODO(knz): this is missing dedicated logging,
		// See https://github.com/cockroachdb/cockroach/issues/57741
		err = params.p.setTypeSchema(params.ctx, n, string(t.Schema))
	case *tree.AlterTypeOwner:
		if err = params.p.alterTypeOwner(params.ctx, n, t.Owner); err != nil {
			return err
		}
		eventLogDone = true // done inside alterTypeOwner().
	case *tree.AlterTypeDropValue:
		if !params.p.SessionData().DropEnumValueEnabled {
			return pgerror.WithCandidateCode(
				errors.WithHint(
					errors.WithIssueLink(
						errors.New("ALTER TYPE ... DROP VALUE ... is only supported as an alpha feature "+
							"since view, default, or computed expressions will stop working if they reference the "+
							"ENUM value"),
						errors.IssueLink{IssueURL: build.MakeIssueURL(61594)}),
					"you can enable alter type drop value by running "+
						"`SET enable_drop_enum_value = true`"),
				pgcode.FeatureNotSupported)
		}
		err = params.p.dropEnumValue(params.ctx, n.desc, t.Val)
	default:
		err = errors.AssertionFailedf("unknown alter type cmd %s", t)
	}
	if err != nil {
		return err
	}

	if !eventLogDone {
		// Write a log event.
		if err := params.p.logEvent(params.ctx,
			n.desc.ID,
			&eventpb.AlterType{
				TypeName: typeName,
			}); err != nil {
			return err
		}
	}
	return nil
}

func findEnumMemberByName(
	desc *typedesc.Mutable, val tree.EnumValue,
) (bool, *descpb.TypeDescriptor_EnumMember) {
	for _, member := range desc.EnumMembers {
		if member.LogicalRepresentation == string(val) {
			return true, &member
		}
	}
	return false, nil
}

func (p *planner) addEnumValue(
	ctx context.Context, desc *typedesc.Mutable, node *tree.AlterTypeAddValue, jobDesc string,
) error {
	if desc.Kind != descpb.TypeDescriptor_ENUM &&
		desc.Kind != descpb.TypeDescriptor_MULTIREGION_ENUM {
		return pgerror.Newf(pgcode.WrongObjectType, "%q is not an enum", desc.Name)
	}
	// See if the value already exists in the enum or not.
	found, member := findEnumMemberByName(desc, node.NewVal)
	if found {
		if enumMemberIsRemoving(member) {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"enum value %q is being dropped, try again later", node.NewVal)
		}
		if node.IfNotExists {
			p.BufferClientNotice(
				ctx,
				pgnotice.Newf("enum value %q already exists, skipping", node.NewVal),
			)
			return nil
		}
		return pgerror.Newf(pgcode.DuplicateObject, "enum value %q already exists", node.NewVal)
	}

	if err := desc.AddEnumValue(node); err != nil {
		return err
	}
	return p.writeTypeSchemaChange(ctx, desc, jobDesc)
}

func (p *planner) dropEnumValue(
	ctx context.Context, desc *typedesc.Mutable, val tree.EnumValue,
) error {
	if desc.Kind != descpb.TypeDescriptor_ENUM &&
		desc.Kind != descpb.TypeDescriptor_MULTIREGION_ENUM {
		return pgerror.Newf(pgcode.WrongObjectType, "%q is not an enum", desc.Name)
	}

	found, member := findEnumMemberByName(desc, val)
	if !found {
		return pgerror.Newf(pgcode.UndefinedObject, "enum value %q does not exist", val)
	}
	// Do not allow drops if the enum value isn't public yet.
	if enumMemberIsRemoving(member) {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"enum value %q is already being dropped", val)
	}
	if enumMemberIsAdding(member) {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"enum value %q is being added, try again later", val)
	}

	desc.DropEnumValue(val)
	return p.writeTypeSchemaChange(ctx, desc, desc.Name)
}

func (p *planner) renameType(ctx context.Context, n *alterTypeNode, newName string) error {
	err := catalogkv.CheckObjectCollision(
		ctx,
		p.txn,
		p.ExecCfg().Codec,
		n.desc.ParentID,
		n.desc.ParentSchemaID,
		tree.NewUnqualifiedTypeName(newName),
	)
	if err != nil {
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
	// Write the new namespace key.
	return p.writeNameKey(ctx, desc, desc.ID)
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
				"enum value %s already exists", newVal)
		}
	}

	// An enum member with the name oldVal was not found.
	if enumMemberIndex == -1 {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"%s is not an existing enum value", oldVal)
	}

	if enumMemberIsRemoving(&n.desc.EnumMembers[enumMemberIndex]) {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"enum value %q is being dropped", oldVal)
	}
	if enumMemberIsAdding(&n.desc.EnumMembers[enumMemberIndex]) {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"enum value %q is being added, try again later", oldVal)

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

	oldName, err := p.getQualifiedTypeName(ctx, typeDesc)
	if err != nil {
		return err
	}

	desiredSchemaID, err := p.prepareSetSchema(ctx, n.prefix.Database, typeDesc, schema)
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

	if err := p.performRenameTypeDesc(
		ctx, arrayDesc, arrayDesc.Name, desiredSchemaID, tree.AsStringWithFQNames(n.n, p.Ann()),
	); err != nil {
		return err
	}

	newName, err := p.getQualifiedTypeName(ctx, typeDesc)
	if err != nil {
		return err
	}

	return p.logEvent(ctx,
		desiredSchemaID,
		&eventpb.SetSchema{
			CommonEventDetails:    eventpb.CommonEventDetails{},
			CommonSQLEventDetails: eventpb.CommonSQLEventDetails{},
			DescriptorName:        oldName.FQString(),
			NewDescriptorName:     newName.FQString(),
			DescriptorType:        "type",
		},
	)
}

func (p *planner) alterTypeOwner(
	ctx context.Context, n *alterTypeNode, newOwner security.SQLUsername,
) error {
	typeDesc := n.desc
	oldOwner := typeDesc.GetPrivileges().Owner()

	arrayDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, typeDesc.ArrayTypeID)
	if err != nil {
		return err
	}

	if err := p.checkCanAlterTypeAndSetNewOwner(ctx, typeDesc, arrayDesc, newOwner); err != nil {
		return err
	}

	// If the owner we want to set to is the current owner, do a no-op.
	if newOwner == oldOwner {
		return nil
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

	if err := p.logEvent(ctx,
		typeDesc.GetID(),
		&eventpb.AlterTypeOwner{
			// TODO(knz): This name is insufficiently qualified.
			// See: https://github.com/cockroachdb/cockroach/issues/57734
			TypeName: typeDesc.GetName(),
			Owner:    newOwner.Normalized(),
		}); err != nil {
		return err
	}
	return p.logEvent(ctx,
		arrayTypeDesc.GetID(),
		&eventpb.AlterTypeOwner{
			// TODO(knz): This name is insufficiently qualified.
			// See: https://github.com/cockroachdb/cockroach/issues/57734
			TypeName: arrayTypeDesc.GetName(),
			Owner:    newOwner.Normalized(),
		})
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
