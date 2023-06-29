// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

func (i *immediateVisitor) RemoveSchemaParent(
	ctx context.Context, op scop.RemoveSchemaParent,
) error {
	db, err := i.checkOutDatabase(ctx, op.Parent.ParentDatabaseID)
	if err != nil {
		return err
	}
	for name, info := range db.Schemas {
		if info.ID == op.Parent.SchemaID {
			delete(db.Schemas, name)
			break
		}
	}
	return nil
}

func (i *immediateVisitor) AddSchemaParent(ctx context.Context, op scop.AddSchemaParent) error {
	db, err := i.checkOutDatabase(ctx, op.Parent.ParentDatabaseID)
	if err != nil {
		return err
	}
	sc, err := i.checkOutSchema(ctx, op.Parent.SchemaID)
	if err != nil {
		return err
	}
	sc.ParentID = op.Parent.ParentDatabaseID

	if sc.Name == "" {
		return errors.AssertionFailedf("schema name is empty")
	}
	if _, ok := db.Schemas[sc.Name]; ok {
		return errors.AssertionFailedf("schema %v already exists in database %v", sc.Name, db.Name)
	}
	db.Schemas[sc.Name] = descpb.DatabaseDescriptor_SchemaInfo{ID: sc.ID}
	return nil
}

func (i *immediateVisitor) RemoveOwnerBackReferenceInSequence(
	ctx context.Context, op scop.RemoveOwnerBackReferenceInSequence,
) error {
	seq, err := i.checkOutTable(ctx, op.SequenceID)
	if err != nil || seq.Dropped() {
		return err
	}
	seq.GetSequenceOpts().SequenceOwner.Reset()
	return nil
}

func (i *immediateVisitor) AddOwnerBackReferenceInSequence(
	ctx context.Context, op scop.AddOwnerBackReferenceInSequence,
) error {
	seq, err := i.checkOutTable(ctx, op.SequenceID)
	if err != nil || seq.Dropped() {
		return err
	}
	opts := seq.GetSequenceOpts()
	opts.SequenceOwner.OwnerColumnID = op.ColumnID
	opts.SequenceOwner.OwnerTableID = op.TableID
	return nil
}

func (i *immediateVisitor) AddSequenceOwner(ctx context.Context, op scop.AddSequenceOwner) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	col, err := catalog.MustFindColumnByID(tbl, op.ColumnID)
	if err != nil {
		return err
	}
	ids := catalog.MakeDescriptorIDSet(col.ColumnDesc().OwnsSequenceIds...)
	ids.Add(op.OwnedSequenceID)
	col.ColumnDesc().OwnsSequenceIds = ids.Ordered()
	return nil
}

func (i *immediateVisitor) RemoveSequenceOwner(
	ctx context.Context, op scop.RemoveSequenceOwner,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	col, err := catalog.MustFindColumnByID(tbl, op.ColumnID)
	if err != nil {
		return err
	}
	ids := catalog.MakeDescriptorIDSet(col.ColumnDesc().OwnsSequenceIds...)
	ids.Remove(op.OwnedSequenceID)
	col.ColumnDesc().OwnsSequenceIds = ids.Ordered()
	return nil
}

func (i *immediateVisitor) RemoveForeignKeyBackReference(
	ctx context.Context, op scop.RemoveForeignKeyBackReference,
) error {
	in, err := i.checkOutTable(ctx, op.ReferencedTableID)
	if err != nil {
		// Exit early if the foreign key back-reference holder is getting dropped.
		return err
	}
	// Retrieve foreign key name in origin table to identify it in the referenced
	// table.
	var name string
	{
		out, err := i.getDescriptor(ctx, op.OriginTableID)
		if err != nil {
			return err
		}
		tbl, err := catalog.AsTableDescriptor(out)
		if err != nil {
			return err
		}
		for _, fk := range tbl.OutboundForeignKeys() {
			if fk.GetConstraintID() == op.OriginConstraintID {
				name = fk.GetName()
				break
			}
		}
		if name == "" {
			return errors.AssertionFailedf("foreign key with ID %d not found in origin table %q (%d)",
				op.OriginConstraintID, out.GetName(), out.GetID())
		}
	}
	// Attempt to remove back reference.
	// Note how we
	//  1. only check to remove from `in.InboundFKs` but not from `in.Mutations`:
	//  this is because we only add the back-reference in `in` when we publish
	//  the adding FK in `out`, so it's impossible for a back-reference to exist
	//  on the mutation slice.
	//  2. only attempt to remove (i.e. we do not panic when it's not found):
	//  this is because if we roll back before the adding FK is published in `out`,
	//  such a back-reference won't exist in `in` yet.
	for idx, fk := range in.InboundFKs {
		if fk.OriginTableID == op.OriginTableID && fk.Name == name {
			in.InboundFKs = append(in.InboundFKs[:idx], in.InboundFKs[idx+1:]...)
			if len(in.InboundFKs) == 0 {
				in.InboundFKs = nil
			}
			break
		}
	}
	return nil
}

func (i *immediateVisitor) UpdateTableBackReferencesInTypes(
	ctx context.Context, op scop.UpdateTableBackReferencesInTypes,
) error {
	var forwardRefs catalog.DescriptorIDSet
	if desc, err := i.getDescriptor(ctx, op.BackReferencedTableID); err != nil {
		return err
	} else if !desc.Dropped() {
		tbl, err := catalog.AsTableDescriptor(desc)
		if err != nil {
			return err
		}
		parent, err := i.getDescriptor(ctx, desc.GetParentID())
		if err != nil {
			return err
		}
		db, err := catalog.AsDatabaseDescriptor(parent)
		if err != nil {
			return err
		}
		ids, _, err := tbl.GetAllReferencedTypeIDs(db, func(id descpb.ID) (catalog.TypeDescriptor, error) {
			d, err := i.getDescriptor(ctx, id)
			if err != nil {
				return nil, err
			}
			return catalog.AsTypeDescriptor(d)
		})
		if err != nil {
			return err
		}
		for _, id := range ids {
			forwardRefs.Add(id)
		}
	}
	return updateBackReferencesInTypes(ctx, i, op.TypeIDs, op.BackReferencedTableID, forwardRefs)
}

func (i *immediateVisitor) RemoveBackReferenceInTypes(
	ctx context.Context, op scop.RemoveBackReferenceInTypes,
) error {
	return updateBackReferencesInTypes(ctx, i, op.TypeIDs, op.BackReferencedDescriptorID, catalog.DescriptorIDSet{})
}

// updateBackReferencesInTypes updates back references to `backReferencedDescID`
// in types represented by `typeIDs`, given the expected forward references from
// the object represented by `backReferencedDescID`.
func updateBackReferencesInTypes(
	ctx context.Context,
	m *immediateVisitor,
	typeIDs []catid.DescID,
	backReferencedDescID catid.DescID,
	forwardRefs catalog.DescriptorIDSet,
) error {
	for _, typeID := range typeIDs {
		typ, err := m.checkOutType(ctx, typeID)
		if err != nil {
			return err
		} else if typ.Dropped() {
			// Skip updating back-references in dropped type descriptors.
			continue
		}
		backRefs := catalog.MakeDescriptorIDSet(typ.ReferencingDescriptorIDs...)
		if forwardRefs.Contains(typeID) {
			if backRefs.Contains(backReferencedDescID) {
				continue
			}
			backRefs.Add(backReferencedDescID)
		} else {
			if !backRefs.Contains(backReferencedDescID) {
				continue
			}
			backRefs.Remove(backReferencedDescID)
		}
		typ.ReferencingDescriptorIDs = backRefs.Ordered()
	}
	return nil
}

func (i *immediateVisitor) UpdateTypeBackReferencesInTypes(
	ctx context.Context, op scop.UpdateTypeBackReferencesInTypes,
) error {
	var forwardRefs catalog.DescriptorIDSet
	if desc, err := i.getDescriptor(ctx, op.BackReferencedTypeID); err != nil {
		return err
	} else {
		typ, err := catalog.AsTypeDescriptor(desc)
		if err != nil {
			return err
		}
		forwardRefs = typ.GetIDClosure()
	}
	return updateBackReferencesInTypes(ctx, i, op.TypeIDs, op.BackReferencedTypeID, forwardRefs)
}

func (i *immediateVisitor) UpdateTableBackReferencesInSequences(
	ctx context.Context, op scop.UpdateTableBackReferencesInSequences,
) error {
	var forwardRefs catalog.DescriptorIDSet
	if desc, err := i.getDescriptor(ctx, op.BackReferencedTableID); err != nil {
		return err
	} else if !desc.Dropped() {
		tbl, err := catalog.AsTableDescriptor(desc)
		if err != nil {
			return err
		}
		if op.BackReferencedColumnID != 0 {
			col, err := catalog.MustFindColumnByID(tbl, op.BackReferencedColumnID)
			if err != nil {
				return err
			}
			for i, n := 0, col.NumUsesSequences(); i < n; i++ {
				forwardRefs.Add(col.GetUsesSequenceID(i))
			}
			for i, n := 0, col.NumOwnsSequences(); i < n; i++ {
				forwardRefs.Add(col.GetOwnsSequenceID(i))
			}
		} else {
			for _, c := range tbl.CheckConstraints() {
				ids, err := sequenceIDsInExpr(c.GetExpr())
				if err != nil {
					return err
				}
				ids.ForEach(forwardRefs.Add)
			}
		}
	}
	for _, seqID := range op.SequenceIDs {
		if err := updateBackReferencesInSequences(
			ctx, i, seqID, op.BackReferencedTableID, op.BackReferencedColumnID, forwardRefs,
		); err != nil {
			return err
		}
	}
	return nil
}

func (i *immediateVisitor) AddTableConstraintBackReferencesInFunctions(
	ctx context.Context, op scop.AddTableConstraintBackReferencesInFunctions,
) error {
	for _, fnID := range op.FunctionIDs {
		fnDesc, err := i.checkOutFunction(ctx, fnID)
		if err != nil {
			return err
		}
		if err := fnDesc.AddConstraintReference(op.BackReferencedTableID, op.BackReferencedConstraintID); err != nil {
			return err
		}
	}
	return nil
}

func (i *immediateVisitor) RemoveTableConstraintBackReferencesFromFunctions(
	ctx context.Context, op scop.RemoveTableConstraintBackReferencesFromFunctions,
) error {
	for _, fnID := range op.FunctionIDs {
		fnDesc, err := i.checkOutFunction(ctx, fnID)
		if err != nil {
			return err
		}
		fnDesc.RemoveConstraintReference(op.BackReferencedTableID, op.BackReferencedConstraintID)
	}
	return nil
}

func (i *immediateVisitor) RemoveTableColumnBackReferencesInFunctions(
	ctx context.Context, op scop.RemoveTableColumnBackReferencesInFunctions,
) error {
	tblDesc, err := i.checkOutTable(ctx, op.BackReferencedTableID)
	if err != nil {
		return err
	}
	var fnIDsInUse catalog.DescriptorIDSet
	if !tblDesc.Dropped() {
		// If table is dropped then there is no functions in use.
		fnIDsInUse, err = tblDesc.GetAllReferencedFunctionIDsInColumnExprs(op.BackReferencedColumnID)
		if err != nil {
			return err
		}
	}
	for _, id := range op.FunctionIDs {
		if fnIDsInUse.Contains(id) {
			continue
		}
		fnDesc, err := i.checkOutFunction(ctx, id)
		if err != nil {
			return err
		}
		fnDesc.RemoveColumnReference(op.BackReferencedTableID, op.BackReferencedColumnID)
	}
	return nil
}

// Look through `seqID`'s dependedOnBy slice, find the back-reference to `tblID`,
// and update it to either
//   - upsert `colID` to ColumnIDs field of that back-reference, if `forwardRefs` contains `seqID`; or
//   - remove `colID` from ColumnIDs field of that back-reference, if `forwardRefs` does not contain `seqID`.
func updateBackReferencesInSequences(
	ctx context.Context,
	m *immediateVisitor,
	seqID, tblID descpb.ID,
	colID descpb.ColumnID,
	forwardRefs catalog.DescriptorIDSet,
) error {
	seq, err := m.checkOutTable(ctx, seqID)
	if err != nil || seq.Dropped() {
		// Skip updating back-references in dropped sequence descriptors.
		return err
	}
	var current, updated catalog.TableColSet
	_ = seq.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
		if dep.ID == tblID {
			current = catalog.MakeTableColSet(dep.ColumnIDs...)
			return iterutil.StopIteration()
		}
		return nil
	})
	if forwardRefs.Contains(seqID) {
		if current.Contains(colID) {
			return nil
		}
		updated.UnionWith(current)
		if colID != 0 {
			updated.Add(colID)
		}
	} else {
		if !current.Contains(colID) && colID != 0 {
			return nil
		}
		current.ForEach(func(id descpb.ColumnID) {
			if id != colID {
				updated.Add(id)
			}
		})
	}
	seq.UpdateColumnsDependedOnBy(tblID, updated)
	return nil
}

func (i *immediateVisitor) RemoveBackReferencesInRelations(
	ctx context.Context, op scop.RemoveBackReferencesInRelations,
) error {
	for _, relationID := range op.RelationIDs {
		if err := removeViewBackReferencesInRelation(ctx, i, relationID, op.BackReferencedID); err != nil {
			return err
		}
	}
	return nil
}

func removeViewBackReferencesInRelation(
	ctx context.Context, m *immediateVisitor, relationID, backReferencedID descpb.ID,
) error {
	tbl, err := m.checkOutTable(ctx, relationID)
	if err != nil || tbl.Dropped() {
		// Skip updating back-references in dropped table or view descriptors.
		return err
	}
	var newBackRefs []descpb.TableDescriptor_Reference
	for _, by := range tbl.DependedOnBy {
		if by.ID != backReferencedID {
			newBackRefs = append(newBackRefs, by)
		}
	}
	tbl.DependedOnBy = newBackRefs
	return nil
}

func (i *immediateVisitor) RemoveObjectParent(
	ctx context.Context, op scop.RemoveObjectParent,
) error {
	obj, err := i.checkOutDescriptor(ctx, op.ObjectID)
	if err != nil {
		return err
	}
	switch obj.DescriptorType() {
	case catalog.Function:
		sc, err := i.checkOutSchema(ctx, op.ParentSchemaID)
		if err != nil {
			return err
		}
		sc.RemoveFunction(obj.GetName(), obj.GetID())
	}
	return nil
}

func (i *immediateVisitor) UpdateFunctionTypeReferences(
	ctx context.Context, op scop.UpdateFunctionTypeReferences,
) error {
	fn, err := i.checkOutFunction(ctx, op.FunctionID)
	if err != nil {
		return err
	}

	newForwardRefs := catalog.MakeDescriptorIDSet(op.TypeIDs...)
	currentForwardRefs := catalog.MakeDescriptorIDSet(fn.DependsOnTypes...)
	typeIDsToUpdate := newForwardRefs.Union(currentForwardRefs)
	if err := updateBackReferencesInTypes(ctx, i, typeIDsToUpdate.Ordered(), op.FunctionID, newForwardRefs); err != nil {
		return err
	}

	fn.DependsOnTypes = op.TypeIDs
	return nil
}

func (i *immediateVisitor) UpdateFunctionRelationReferences(
	ctx context.Context, op scop.UpdateFunctionRelationReferences,
) error {
	fn, err := i.checkOutFunction(ctx, op.FunctionID)
	if err != nil {
		return err
	}
	relIDs := catalog.DescriptorIDSet{}
	relIDToReferences := make(map[descpb.ID][]descpb.TableDescriptor_Reference)

	for _, ref := range op.TableReferences {
		relIDs.Add(ref.TableID)
		dep := descpb.TableDescriptor_Reference{
			ID:        op.FunctionID,
			IndexID:   ref.IndexID,
			ColumnIDs: ref.ColumnIDs,
		}
		relIDToReferences[ref.TableID] = append(relIDToReferences[ref.TableID], dep)
	}

	for _, ref := range op.ViewReferences {
		relIDs.Add(ref.ViewID)
		dep := descpb.TableDescriptor_Reference{
			ID:        op.FunctionID,
			ColumnIDs: ref.ColumnIDs,
		}
		relIDToReferences[ref.ViewID] = append(relIDToReferences[ref.ViewID], dep)
	}

	for _, seqID := range op.SequenceIDs {
		relIDs.Add(seqID)
		dep := descpb.TableDescriptor_Reference{
			ID:   op.FunctionID,
			ByID: true,
		}
		relIDToReferences[seqID] = append(relIDToReferences[seqID], dep)
	}

	for relID, refs := range relIDToReferences {
		if err := updateBackReferencesInRelation(ctx, i, relID, op.FunctionID, refs); err != nil {
			return err
		}
	}

	fn.DependsOn = relIDs.Ordered()
	return nil
}

func updateBackReferencesInRelation(
	ctx context.Context,
	i *immediateVisitor,
	relID descpb.ID,
	backReferencedID descpb.ID,
	backRefs []descpb.TableDescriptor_Reference,
) error {
	rel, err := i.checkOutTable(ctx, relID)
	if err != nil {
		return err
	}
	newRefs := rel.DependedOnBy[:0]
	for _, ref := range rel.DependedOnBy {
		if ref.ID != backReferencedID {
			newRefs = append(newRefs, ref)
		}
	}

	newRefs = append(newRefs, backRefs...)
	rel.DependedOnBy = newRefs
	return nil
}

func (i *immediateVisitor) SetObjectParentID(ctx context.Context, op scop.SetObjectParentID) error {
	sc, err := i.checkOutSchema(ctx, op.ObjParent.SchemaID)
	if err != nil {
		return err
	}

	obj, err := i.checkOutDescriptor(ctx, op.ObjParent.ChildObjectID)
	if err != nil {
		return err
	}
	switch t := obj.(type) {
	case *funcdesc.Mutable:
		if t.ParentSchemaID != descpb.InvalidID {
			sc.RemoveFunction(t.GetName(), t.GetID())
		}
		t.ParentID = sc.GetParentID()
		t.ParentSchemaID = sc.GetID()

		ol := descpb.SchemaDescriptor_FunctionSignature{
			ID:         obj.GetID(),
			ArgTypes:   make([]*types.T, len(t.GetParams())),
			ReturnType: t.GetReturnType().Type,
			ReturnSet:  t.GetReturnType().ReturnSet,
		}
		for i := range t.Params {
			ol.ArgTypes[i] = t.Params[i].Type
		}
		sc.AddFunction(obj.GetName(), ol)
	}
	return nil
}
