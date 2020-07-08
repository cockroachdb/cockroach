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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

type dropTypeNode struct {
	n  *tree.DropType
	td map[sqlbase.ID]*sqlbase.MutableTypeDescriptor
}

// Use to satisfy the linter.
var _ planNode = &dropTypeNode{n: nil}

func (p *planner) DropType(ctx context.Context, n *tree.DropType) (planNode, error) {
	node := &dropTypeNode{
		n:  n,
		td: make(map[sqlbase.ID]*sqlbase.MutableTypeDescriptor),
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
		// The implicit array types are not directly droppable.
		if typeDesc.Kind == sqlbase.TypeDescriptor_ALIAS {
			return nil, pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"%q is an implicit array type and cannot be modified",
				name,
			)
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
	ctx context.Context, desc *sqlbase.MutableTypeDescriptor, behavior tree.DropBehavior,
) error {
	// TODO (rohany): Add privilege checks here when we have them.
	if len(desc.ReferencingDescriptorIDs) > 0 && behavior != tree.DropCascade {
		var dependentNames []string
		for _, id := range desc.ReferencingDescriptorIDs {
			desc, err := p.Descriptors().GetMutableTableVersionByID(ctx, id, p.txn)
			if err != nil {
				return errors.Wrapf(err, "type has dependent objects")
			}
			fqName, err := p.getQualifiedTableName(ctx, desc.TableDesc())
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
		if err := params.p.dropTypeImpl(params.ctx, typ, tree.AsStringWithFQNames(n.n, params.Ann())); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) addTypeBackReference(
	ctx context.Context, typeID, ref sqlbase.ID, jobDesc string,
) error {
	mutDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, typeID)
	if err != nil {
		return err
	}
	mutDesc.AddReferencingDescriptorID(ref)
	return p.writeTypeChange(ctx, mutDesc, jobDesc)
}

func (p *planner) removeTypeBackReference(
	ctx context.Context, typeID, ref sqlbase.ID, jobDesc string,
) error {
	mutDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, typeID)
	if err != nil {
		return err
	}
	mutDesc.RemoveReferencingDescriptorID(ref)
	return p.writeTypeChange(ctx, mutDesc, jobDesc)
}

func (p *planner) addBackRefsFromAllTypesInTable(
	ctx context.Context, desc *sqlbase.MutableTableDescriptor,
) error {
	typeIDs, err := desc.GetAllReferencedTypeIDs(func(id sqlbase.ID) (*sqlbase.TypeDescriptor, error) {
		mutDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, id)
		if err != nil {
			return nil, err
		}
		return mutDesc.TypeDesc(), nil
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
	ctx context.Context, desc *sqlbase.MutableTableDescriptor,
) error {
	typeIDs, err := desc.GetAllReferencedTypeIDs(func(id sqlbase.ID) (*sqlbase.TypeDescriptor, error) {
		mutDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, id)
		if err != nil {
			return nil, err
		}
		return mutDesc.TypeDesc(), nil
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
	ctx context.Context, typeDesc *sqlbase.MutableTypeDescriptor, jobDesc string,
) error {
	if typeDesc.Dropped() {
		return errors.Errorf("type %q is already being dropped", typeDesc.Name)
	}

	// Add a draining name.
	typeDesc.DrainingNames = append(typeDesc.DrainingNames, sqlbase.NameInfo{
		ParentID:       typeDesc.ParentID,
		ParentSchemaID: typeDesc.ParentSchemaID,
		Name:           typeDesc.Name,
	})

	// Actually mark the type as dropped.
	typeDesc.State = sqlbase.TypeDescriptor_DROP
	return p.writeTypeChange(ctx, typeDesc, jobDesc)
}

func (n *dropTypeNode) Next(params runParams) (bool, error) { return false, nil }
func (n *dropTypeNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *dropTypeNode) Close(ctx context.Context)           {}
func (n *dropTypeNode) ReadingOwnWrites()                   {}
