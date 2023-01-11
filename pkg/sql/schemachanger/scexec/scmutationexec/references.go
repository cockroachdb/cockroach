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
	"github.com/cockroachdb/errors"
)

func (m *visitor) RemoveSchemaParent(ctx context.Context, op scop.RemoveSchemaParent) error {
	db, err := m.checkOutDatabase(ctx, op.Parent.ParentDatabaseID)
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

func (m *visitor) RemoveOwnerBackReferenceInSequence(
	ctx context.Context, op scop.RemoveOwnerBackReferenceInSequence,
) error {
	seq, err := m.checkOutTable(ctx, op.SequenceID)
	if err != nil || seq.Dropped() {
		return err
	}
	seq.GetSequenceOpts().SequenceOwner.Reset()
	return nil
}

func (m *visitor) RemoveSequenceOwner(ctx context.Context, op scop.RemoveSequenceOwner) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
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

func (m *visitor) RemoveForeignKeyBackReference(
	ctx context.Context, op scop.RemoveForeignKeyBackReference,
) error {
	in, err := m.checkOutTable(ctx, op.ReferencedTableID)
	if err != nil || in.Dropped() {
		// Exit early if the foreign key back-reference holder is getting dropped.
		return err
	}
	// Retrieve foreign key name in origin table to identify it in the referenced
	// table.
	var name string
	{
		out, err := m.s.GetDescriptor(ctx, op.OriginTableID)
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
	for i, fk := range in.InboundFKs {
		if fk.OriginTableID == op.OriginTableID && fk.Name == name {
			in.InboundFKs = append(in.InboundFKs[:i], in.InboundFKs[i+1:]...)
			if len(in.InboundFKs) == 0 {
				in.InboundFKs = nil
			}
			break
		}
	}
	return nil
}

func (m *visitor) UpdateTableBackReferencesInTypes(
	ctx context.Context, op scop.UpdateTableBackReferencesInTypes,
) error {
	var forwardRefs catalog.DescriptorIDSet
	if desc, err := m.s.GetDescriptor(ctx, op.BackReferencedTableID); err != nil {
		return err
	} else if !desc.Dropped() {
		tbl, err := catalog.AsTableDescriptor(desc)
		if err != nil {
			return err
		}
		parent, err := m.s.GetDescriptor(ctx, desc.GetParentID())
		if err != nil {
			return err
		}
		db, err := catalog.AsDatabaseDescriptor(parent)
		if err != nil {
			return err
		}
		ids, _, err := tbl.GetAllReferencedTypeIDs(db, func(id descpb.ID) (catalog.TypeDescriptor, error) {
			d, err := m.s.GetDescriptor(ctx, id)
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
	return updateBackReferencesInTypes(ctx, m, op.TypeIDs, op.BackReferencedTableID, forwardRefs)
}

func (m *visitor) RemoveBackReferenceInTypes(
	ctx context.Context, op scop.RemoveBackReferenceInTypes,
) error {
	return updateBackReferencesInTypes(ctx, m, op.TypeIDs, op.BackReferencedDescriptorID, catalog.DescriptorIDSet{})
}

func updateBackReferencesInTypes(
	ctx context.Context,
	m *visitor,
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

func (m *visitor) UpdateTypeBackReferencesInTypes(
	ctx context.Context, op scop.UpdateTypeBackReferencesInTypes,
) error {
	var forwardRefs catalog.DescriptorIDSet
	if desc, err := m.s.GetDescriptor(ctx, op.BackReferencedTypeID); err != nil {
		return err
	} else if !desc.Dropped() {
		typ, err := catalog.AsTypeDescriptor(desc)
		if err != nil {
			return err
		}
		forwardRefs = typ.GetIDClosure()
	}
	return updateBackReferencesInTypes(ctx, m, op.TypeIDs, op.BackReferencedTypeID, forwardRefs)
}

func (m *visitor) UpdateBackReferencesInSequences(
	ctx context.Context, op scop.UpdateBackReferencesInSequences,
) error {
	var forwardRefs catalog.DescriptorIDSet
	if desc, err := m.s.GetDescriptor(ctx, op.BackReferencedTableID); err != nil {
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
			ctx, m, seqID, op.BackReferencedTableID, op.BackReferencedColumnID, forwardRefs,
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
	m *visitor,
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
		if !current.Contains(colID) {
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

func (m *visitor) RemoveViewBackReferencesInRelations(
	ctx context.Context, op scop.RemoveViewBackReferencesInRelations,
) error {
	for _, relationID := range op.RelationIDs {
		if err := removeViewBackReferencesInRelation(ctx, m, relationID, op.BackReferencedViewID); err != nil {
			return err
		}
	}
	return nil
}

func removeViewBackReferencesInRelation(
	ctx context.Context, m *visitor, relationID, viewID descpb.ID,
) error {
	tbl, err := m.checkOutTable(ctx, relationID)
	if err != nil || tbl.Dropped() {
		// Skip updating back-references in dropped table or view descriptors.
		return err
	}
	var newBackRefs []descpb.TableDescriptor_Reference
	for _, by := range tbl.DependedOnBy {
		if by.ID != viewID {
			newBackRefs = append(newBackRefs, by)
		}
	}
	tbl.DependedOnBy = newBackRefs
	return nil
}
