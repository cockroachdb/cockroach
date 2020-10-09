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
	"github.com/cockroachdb/errors"
)

type dropTypeNode struct {
	n  *tree.DropType
	td map[descpb.ID]*typedesc.Mutable
}

// Use to satisfy the linter.
var _ planNode = &dropTypeNode{n: nil}

func (p *planner) DropType(ctx context.Context, n *tree.DropType) (planNode, error) {
	node := &dropTypeNode{
		n:  n,
		td: make(map[descpb.ID]*typedesc.Mutable),
	}
	if n.DropBehavior == tree.DropCascade {
		return nil, unimplemented.NewWithIssue(51480, "DROP TYPE CASCADE is not yet supported")
	}
	for _, name := range n.Names {
		// Resolve the desired type descriptor.
		typeDesc, err := p.ResolveMutableTypeDescriptor(ctx, name, !n.IfExists)
		if err != nil {
			return nil, err
		}
		if typeDesc == nil {
			continue
		}
		// If we've already seen this type, then skip it.
		if _, ok := node.td[typeDesc.ID]; ok {
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
		node.td[typeDesc.ID] = typeDesc
		node.td[mutArrayDesc.ID] = mutArrayDesc
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
		var dependentNames []string
		for _, id := range desc.ReferencingDescriptorIDs {
			desc, err := p.Descriptors().GetMutableTableVersionByID(ctx, id, p.txn)
			if err != nil {
				return errors.Wrapf(err, "type has dependent objects")
			}
			fqName, err := p.getQualifiedTableName(ctx, desc)
			if err != nil {
				return errors.Wrapf(err, "type %q has dependent objects", desc.Name)
			}
			dependentNames = append(dependentNames, fqName.FQString())
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
	for _, typ := range n.td {
		if err := params.p.dropTypeImpl(params.ctx, typ, tree.AsStringWithFQNames(n.n, params.Ann()), true /* queueJob */); err != nil {
			return err
		}
		// Log a Drop Type event.
		if err := MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
			params.ctx,
			params.p.txn,
			EventLogDropType,
			int32(typ.ID),
			int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
			struct {
				TypeName  string
				Statement string
				User      string
			}{typ.Name, tree.AsStringWithFQNames(n.n, params.Ann()), params.p.User().Normalized()},
		); err != nil {
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

func (p *planner) removeTypeBackReference(
	ctx context.Context, typeID, ref descpb.ID, jobDesc string,
) error {
	mutDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, typeID)
	if err != nil {
		return err
	}
	mutDesc.RemoveReferencingDescriptorID(ref)
	return p.writeTypeSchemaChange(ctx, mutDesc, jobDesc)
}

func (p *planner) addBackRefsFromAllTypesInTable(
	ctx context.Context, desc *tabledesc.Mutable,
) error {
	typeIDs, err := desc.GetAllReferencedTypeIDs(func(id descpb.ID) (catalog.TypeDescriptor, error) {
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
	typeIDs, err := desc.GetAllReferencedTypeIDs(func(id descpb.ID) (catalog.TypeDescriptor, error) {
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
		if err := p.removeTypeBackReference(ctx, id, desc.ID, jobDesc); err != nil {
			return err
		}
	}
	return nil
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
