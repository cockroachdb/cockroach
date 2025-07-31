// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	if db.Schemas == nil {
		db.Schemas = make(map[string]descpb.DatabaseDescriptor_SchemaInfo)
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
			for _, p := range tbl.GetPolicies() {
				for _, pexpr := range []string{p.WithCheckExpr, p.UsingExpr} {
					if pexpr != "" {
						ids, err := sequenceIDsInExpr(pexpr)
						if err != nil {
							return err
						}
						ids.ForEach(forwardRefs.Add)
					}
				}
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

func (i *immediateVisitor) AddTableColumnBackReferencesInFunctions(
	ctx context.Context, op scop.AddTableColumnBackReferencesInFunctions,
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
		// If the fnIDSInUse are functions that we are not "adding" back in, do nothing.
		if !fnIDsInUse.Contains(id) {
			continue
		}
		fnDesc, err := i.checkOutFunction(ctx, id)
		if err != nil {
			return err
		}
		if err = fnDesc.AddColumnReference(op.BackReferencedTableID, op.BackReferencedColumnID); err != nil {
			return err
		}
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

func (i *immediateVisitor) AddTriggerBackReferencesInRoutines(
	ctx context.Context, op scop.AddTriggerBackReferencesInRoutines,
) error {
	for _, id := range op.RoutineIDs {
		fnDesc, err := i.checkOutFunction(ctx, id)
		if err != nil {
			return err
		}
		if err := fnDesc.AddTriggerReference(op.BackReferencedTableID, op.BackReferencedTriggerID); err != nil {
			return err
		}
	}
	return nil
}

func (i *immediateVisitor) RemoveTriggerBackReferencesInRoutines(
	ctx context.Context, op scop.RemoveTriggerBackReferencesInRoutines,
) error {
	for _, id := range op.RoutineIDs {
		fnDesc, err := i.checkOutFunction(ctx, id)
		if err != nil {
			return err
		}
		fnDesc.RemoveTriggerReference(op.BackReferencedTableID, op.BackReferencedTriggerID)
	}
	return nil
}

func (i *immediateVisitor) AddPolicyBackReferenceInFunctions(
	ctx context.Context, op scop.AddPolicyBackReferenceInFunctions,
) error {
	for _, id := range op.FunctionIDs {
		fnDesc, err := i.checkOutFunction(ctx, id)
		if err != nil {
			return err
		}
		if err := fnDesc.AddPolicyReference(op.BackReferencedTableID, op.BackReferencedPolicyID); err != nil {
			return err
		}
	}
	return nil
}

func (i *immediateVisitor) RemovePolicyBackReferenceInFunctions(
	ctx context.Context, op scop.RemovePolicyBackReferenceInFunctions,
) error {
	for _, id := range op.FunctionIDs {
		fnDesc, err := i.checkOutFunction(ctx, id)
		if err != nil {
			return err
		}
		fnDesc.RemovePolicyReference(op.BackReferencedTableID, op.BackReferencedPolicyID)
	}
	return nil
}

// updateBackReferencesInSequences will maintain the back-references in the
// sequence to a given table (and optional column).
//
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
	var hasExistingFwdRef bool
	_ = seq.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
		if dep.ID == tblID {
			hasExistingFwdRef = true
			current = catalog.MakeTableColSet(dep.ColumnIDs...)
			return iterutil.StopIteration()
		}
		return nil
	})
	if forwardRefs.Contains(seqID) {
		// The sequence should maintain a back reference to the table. Check if the
		// current reference is sufficient. We can skip updating if the forward reference
		// already points to the correct column (if specified) or, if no column is given,
		// the table itself has an existing reference.
		if current.Contains(colID) || (colID == 0 && hasExistingFwdRef) {
			return nil
		}
		updated.UnionWith(current)
		if colID != 0 {
			updated.Add(colID)
		}
	} else {
		// The sequence should no longer reference the table—either for the specified
		// column (if provided) or for the entire table if no column is given. Check
		// if an update is needed.
		if (colID != 0 && !current.Contains(colID)) || (colID == 0 && !hasExistingFwdRef) {
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
		if err := removeBackReferencesInRelation(ctx, i, relationID, op.BackReferencedID); err != nil {
			return err
		}
	}
	return nil
}

func (i *immediateVisitor) RemoveBackReferenceInFunctions(
	ctx context.Context, op scop.RemoveBackReferenceInFunctions,
) error {
	for _, f := range op.FunctionIDs {
		backRefFunc, err := i.checkOutFunction(ctx, f)
		if err != nil {
			return err
		}
		for i, dep := range backRefFunc.DependedOnBy {
			if dep.ID == op.BackReferencedDescriptorID {
				backRefFunc.DependedOnBy = append(backRefFunc.DependedOnBy[:i], backRefFunc.DependedOnBy[i+1:]...)
				break
			}
		}
	}
	return nil
}

func removeBackReferencesInRelation(
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
	functionIDs := catalog.DescriptorIDSet{}

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

	for _, functionRef := range op.FunctionReferences {
		backRefFunc, err := i.checkOutFunction(ctx, functionRef)
		if err != nil {
			return err
		}
		if err := backRefFunc.AddFunctionReference(op.FunctionID); err != nil {
			return err
		}
		functionIDs.Add(functionRef)
	}
	fn.DependsOnFunctions = functionIDs.Ordered()

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

func (i *immediateVisitor) UpdateTableBackReferencesInRelations(
	ctx context.Context, op scop.UpdateTableBackReferencesInRelations,
) error {
	backRefTbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	forwardRefs := backRefTbl.GetAllReferencedTableIDs()
	for _, relID := range op.RelationIDs {
		referenced, err := i.checkOutTable(ctx, relID)
		if err != nil {
			return err
		}
		newBackRefIsDupe := false
		newBackRef := descpb.TableDescriptor_Reference{ID: op.TableID, ByID: referenced.IsSequence()}
		removeBackRefs := !forwardRefs.Contains(referenced.GetID())
		newDependedOnBy := referenced.DependedOnBy[:0]
		for _, backRef := range referenced.DependedOnBy {
			if removeBackRefs && backRef.ID == op.TableID {
				continue
			}
			newBackRefIsDupe = newBackRefIsDupe || backRef.Equal(newBackRef)
			newDependedOnBy = append(newDependedOnBy, backRef)
		}
		if !removeBackRefs && !newBackRefIsDupe {
			newDependedOnBy = append(newDependedOnBy, newBackRef)
		}
		referenced.DependedOnBy = newDependedOnBy
	}
	return nil
}

func (i *immediateVisitor) SetObjectParentID(ctx context.Context, op scop.SetObjectParentID) error {
	obj, err := i.checkOutDescriptor(ctx, op.ObjParent.ChildObjectID)
	if err != nil {
		return err
	}
	switch t := obj.(type) {
	case *funcdesc.Mutable:
		sc, err := i.checkOutSchema(ctx, op.ObjParent.SchemaID)
		if err != nil {
			return err
		}
		if t.ParentSchemaID != descpb.InvalidID {
			sc.RemoveFunction(t.GetName(), t.GetID())
		}
		t.ParentID = sc.GetParentID()
		t.ParentSchemaID = sc.GetID()

		ol := descpb.SchemaDescriptor_FunctionSignature{
			ID:          obj.GetID(),
			ArgTypes:    make([]*types.T, 0, len(t.GetParams())),
			ReturnType:  t.GetReturnType().Type,
			ReturnSet:   t.GetReturnType().ReturnSet,
			IsProcedure: t.IsProcedure(),
		}
		for pIdx, p := range t.Params {
			class := funcdesc.ToTreeRoutineParamClass(p.Class)
			if tree.IsInParamClass(class) {
				ol.ArgTypes = append(ol.ArgTypes, p.Type)
			}
			if class == tree.RoutineParamOut {
				ol.OutParamOrdinals = append(ol.OutParamOrdinals, int32(pIdx))
				ol.OutParamTypes = append(ol.OutParamTypes, p.Type)
			}
			if p.DefaultExpr != nil {
				ol.DefaultExprs = append(ol.DefaultExprs, *p.DefaultExpr)
			}
		}
		sc.AddFunction(obj.GetName(), ol)
	}
	return nil
}
