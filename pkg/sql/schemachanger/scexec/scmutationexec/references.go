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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
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
		}
	}
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

func (i *immediateVisitor) RemoveSequenceOwner(
	ctx context.Context, op scop.RemoveSequenceOwner,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	col, err := catalog.MustFindColumnByID(tbl, op.ColumnID)
	if err != nil || col == nil {
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
	if err != nil || in.Dropped() {
		// Exit early if the foreign key back-reference holder is getting dropped.
		return err
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
		if fk.OriginTableID == op.OriginTableID && fk.ConstraintID == op.OriginConstraintID {
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
	} else if !desc.Dropped() {
		typ, err := catalog.AsTypeDescriptor(desc)
		if err != nil {
			return err
		}
		forwardRefs = typ.GetIDClosure()
	}
	return updateBackReferencesInTypes(ctx, i, op.TypeIDs, op.BackReferencedTypeID, forwardRefs)
}

func (i *immediateVisitor) UpdateBackReferencesInSequences(
	ctx context.Context, op scop.UpdateBackReferencesInSequences,
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
