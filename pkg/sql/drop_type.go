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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type dropTypeNode struct {
	n      *tree.DropType
	toDrop map[descpb.ID]*typedesc.Mutable
}

// Use to satisfy the linter.
var _ planNode = &dropTypeNode{n: nil}

func (p *planner) DropType(ctx context.Context, n *tree.DropType) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"DROP TYPE",
	); err != nil {
		return nil, err
	}

	node := &dropTypeNode{
		n:      n,
		toDrop: make(map[descpb.ID]*typedesc.Mutable),
	}
	if n.DropBehavior == tree.DropCascade {
		return nil, unimplemented.NewWithIssue(51480, "DROP TYPE CASCADE is not yet supported")
	}
	for _, name := range n.Names {
		// Resolve the desired type descriptor.
		_, typeDesc, err := p.ResolveMutableTypeDescriptor(ctx, name, !n.IfExists)
		if err != nil {
			return nil, err
		}
		if typeDesc == nil {
			continue
		}
		// If we've already seen this type, then skip it.
		if _, ok := node.toDrop[typeDesc.ID]; ok {
			continue
		}
		switch typeDesc.Kind {
		case descpb.TypeDescriptor_ALIAS:
			// The implicit array types are not directly droppable.
			return nil, pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"%q is an implicit array type and cannot be modified",
				name,
			)
		case descpb.TypeDescriptor_MULTIREGION_ENUM:
			// Multi-region enums are not directly droppable.
			return nil, errors.WithHintf(
				pgerror.Newf(
					pgcode.DependentObjectsStillExist,
					"%q is a multi-region enum and cannot be modified directly",
					name,
				),
				"try ALTER DATABASE DROP REGION %s", name)
		case descpb.TypeDescriptor_ENUM:
			sqltelemetry.IncrementEnumCounter(sqltelemetry.EnumDrop)
		}

		// Check if we can drop the type.
		if err := p.canDropTypeDesc(ctx, typeDesc, n.DropBehavior); err != nil {
			return nil, err
		}

		// Get the array type that needs to be dropped as well.
		mutArrayDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, typeDesc.ArrayTypeID)
		if err != nil {
			return nil, err
		}
		// Ensure that we can drop the array type as well.
		if err := p.canDropTypeDesc(ctx, mutArrayDesc, n.DropBehavior); err != nil {
			return nil, err
		}
		// Record these descriptors for deletion.
		node.toDrop[typeDesc.ID] = typeDesc
		node.toDrop[mutArrayDesc.ID] = mutArrayDesc
	}
	return node, nil
}

func (p *planner) canDropTypeDesc(
	ctx context.Context, desc *typedesc.Mutable, behavior tree.DropBehavior,
) error {
	if err := p.canModifyType(ctx, desc); err != nil {
		return err
	}
	if len(desc.ReferencingDescriptorIDs) > 0 && behavior != tree.DropCascade {
		dependentNames, err := p.getFullyQualifiedTableNamesFromIDs(ctx, desc.ReferencingDescriptorIDs)
		if err != nil {
			return errors.Wrapf(err, "type %q has dependent objects", desc.Name)
		}
		return pgerror.Newf(
			pgcode.DependentObjectsStillExist,
			"cannot drop type %q because other objects (%v) still depend on it",
			desc.Name,
			dependentNames,
		)
	}
	return nil
}

func (n *dropTypeNode) startExec(params runParams) error {
	for _, typeDesc := range n.toDrop {
		typeFQName, err := getTypeNameFromTypeDescriptor(
			oneAtATimeSchemaResolver{params.ctx, params.p},
			typeDesc,
		)
		if err != nil {
			return err
		}
		err = params.p.dropTypeImpl(params.ctx, typeDesc, "dropping type "+typeFQName.FQString(), true /* queueJob */)
		if err != nil {
			return err
		}
		event := &eventpb.DropType{
			TypeName: typeFQName.FQString(),
		}
		// Log a Drop Type event.
		if err := params.p.logEvent(params.ctx, typeDesc.ID, event); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) addTypeBackReference(
	ctx context.Context, typeID, ref descpb.ID, jobDesc string,
) error {
	mutDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, typeID)
	if err != nil {
		return err
	}

	// Check if this user has USAGE privilege on the type. This function if an
	// object has a dependency on a type, the user must have USAGE privilege on
	// the type to create a dependency.
	if err := p.CheckPrivilege(ctx, mutDesc, privilege.USAGE); err != nil {
		return err
	}

	mutDesc.AddReferencingDescriptorID(ref)
	return p.writeTypeSchemaChange(ctx, mutDesc, jobDesc)
}

func (p *planner) removeTypeBackReferences(
	ctx context.Context, typeIDs []descpb.ID, ref descpb.ID, jobDesc string,
) error {
	for _, typeID := range typeIDs {
		mutDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, typeID)
		if err != nil {
			return err
		}
		mutDesc.RemoveReferencingDescriptorID(ref)
		if err := p.writeTypeSchemaChange(ctx, mutDesc, jobDesc); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) addBackRefsFromAllTypesInTable(
	ctx context.Context, desc *tabledesc.Mutable,
) error {
	_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(
		ctx, p.txn, desc.GetParentID(), tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return err
	}
	typeIDs, err := desc.GetAllReferencedTypeIDs(dbDesc, func(id descpb.ID) (catalog.TypeDescriptor, error) {
		mutDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, id)
		if err != nil {
			return nil, err
		}
		return mutDesc, nil
	})
	if err != nil {
		return err
	}
	for _, id := range typeIDs {
		jobDesc := fmt.Sprintf("updating type back reference %d for table %d", id, desc.ID)
		if err := p.addTypeBackReference(ctx, id, desc.ID, jobDesc); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) removeBackRefsFromAllTypesInTable(
	ctx context.Context, desc *tabledesc.Mutable,
) error {
	_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(
		ctx, p.txn, desc.GetParentID(), tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return err
	}
	typeIDs, err := desc.GetAllReferencedTypeIDs(dbDesc, func(id descpb.ID) (catalog.TypeDescriptor, error) {
		mutDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, id)
		if err != nil {
			return nil, err
		}
		return mutDesc, nil
	})
	if err != nil {
		return err
	}
	jobDesc := fmt.Sprintf("updating type back references %v for table %d", typeIDs, desc.ID)
	return p.removeTypeBackReferences(ctx, typeIDs, desc.ID, jobDesc)
}

// dropTypeImpl does the work of dropping a type and everything that depends on it.
func (p *planner) dropTypeImpl(
	ctx context.Context, typeDesc *typedesc.Mutable, jobDesc string, queueJob bool,
) error {
	if typeDesc.Dropped() {
		return errors.Errorf("type %q is already being dropped", typeDesc.Name)
	}

	// Add a draining name.
	typeDesc.DrainingNames = append(typeDesc.DrainingNames, descpb.NameInfo{
		ParentID:       typeDesc.ParentID,
		ParentSchemaID: typeDesc.ParentSchemaID,
		Name:           typeDesc.Name,
	})

	// Actually mark the type as dropped.
	typeDesc.State = descpb.DescriptorState_DROP
	if queueJob {
		return p.writeTypeSchemaChange(ctx, typeDesc, jobDesc)
	}
	return p.writeTypeDesc(ctx, typeDesc)
}

func (n *dropTypeNode) Next(params runParams) (bool, error) { return false, nil }
func (n *dropTypeNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *dropTypeNode) Close(ctx context.Context)           {}
func (n *dropTypeNode) ReadingOwnWrites()                   {}
