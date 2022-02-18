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
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

func (m *visitor) RemoveSchemaParent(ctx context.Context, op scop.RemoveSchemaParent) error {
	refID := op.Parent.ParentDatabaseID
	if desc, err := MustReadImmutableDescriptor(ctx, m.cr, refID); err != nil || desc.Dropped() {
		return err
	}
	db, err := m.checkOutDatabase(ctx, refID)
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

func (m *visitor) RemoveSequenceOwner(ctx context.Context, op scop.RemoveSequenceOwner) error {
	refID := op.Owner.SequenceID
	if desc, err := MustReadImmutableDescriptor(ctx, m.cr, refID); err != nil || desc.Dropped() {
		return err
	}
	seq, err := m.checkOutTable(ctx, refID)
	if err != nil {
		return err
	}
	if !seq.Dropped() {
		seq.GetSequenceOpts().SequenceOwner.OwnerTableID = descpb.InvalidID
		seq.GetSequenceOpts().SequenceOwner.OwnerColumnID = 0
	}
	tbl, err := m.checkOutTable(ctx, op.Owner.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	col, err := tbl.FindColumnWithID(op.Owner.ColumnID)
	if err != nil {
		return err
	}
	ids := catalog.MakeDescriptorIDSet(col.ColumnDesc().OwnsSequenceIds...)
	ids.Remove(seq.GetID())
	col.ColumnDesc().OwnsSequenceIds = ids.Ordered()
	return nil
}

func (m *visitor) RemoveCheckConstraint(ctx context.Context, op scop.RemoveCheckConstraint) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	var found bool
	for i, c := range tbl.Checks {
		if c.ConstraintID == op.ConstraintID {
			tbl.Checks = append(tbl.Checks[:i], tbl.Checks[i+1:]...)
			found = true
			break
		}
	}
	for i, m := range tbl.Mutations {
		if c := m.GetConstraint(); c != nil &&
			c.ConstraintType != descpb.ConstraintToUpdate_CHECK &&
			c.Check.ConstraintID == op.ConstraintID {
			tbl.Mutations = append(tbl.Mutations[:i], tbl.Mutations[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return errors.AssertionFailedf("failed to find check constraint %d in table %q (%d)",
			op.ConstraintID, tbl.GetName(), tbl.GetID())
	}
	return nil
}

func (m *visitor) RemoveForeignKeyConstraintAndBackReference(
	ctx context.Context, op scop.RemoveForeignKeyConstraintAndBackReference,
) error {
	var name string
	// Remove forward reference, and retrieve name to make it possible to remove
	// the back reference afterwards.
	{
		out, err := m.checkOutTable(ctx, op.TableID)
		if err != nil {
			return err
		}
		for i, fk := range out.OutboundFKs {
			if fk.ConstraintID == op.ConstraintID {
				name = fk.Name
				out.OutboundFKs = append(out.OutboundFKs[:i], out.OutboundFKs[i+1:]...)
				break
			}
		}
		for i, m := range out.Mutations {
			if c := m.GetConstraint(); c != nil &&
				c.ConstraintType != descpb.ConstraintToUpdate_FOREIGN_KEY &&
				c.ForeignKey.ConstraintID == op.ConstraintID {
				name = c.Name
				out.Mutations = append(out.Mutations[:i], out.Mutations[i+1:]...)
				break
			}
		}
		if name == "" {
			return errors.AssertionFailedf("failed to find foreign constraint %d in table %q (%d)",
				op.ConstraintID, out.GetName(), out.GetID())
		}
	}
	// Remove back reference.
	if desc, err := MustReadImmutableDescriptor(ctx, m.cr, op.ReferencedTableID); err != nil || desc.Dropped() {
		return err
	}
	in, err := m.checkOutTable(ctx, op.ReferencedTableID)
	if err != nil {
		return err
	}
	for i, fk := range in.InboundFKs {
		if fk.OriginTableID == op.TableID && fk.Name == name {
			in.InboundFKs = append(in.InboundFKs[:i], in.InboundFKs[i+1:]...)
			break
		}
	}
	for i, m := range in.Mutations {
		if c := m.GetConstraint(); c != nil &&
			c.ConstraintType != descpb.ConstraintToUpdate_FOREIGN_KEY &&
			c.ForeignKey.OriginTableID == op.TableID &&
			c.Name == name {
			in.Mutations = append(in.Mutations[:i], in.Mutations[i+1:]...)
			break
		}
	}
	return nil
}

func (m *visitor) UpdateBackReferencesInTypes(
	ctx context.Context, op scop.UpdateBackReferencesInTypes,
) error {
	if op.TypeIDs.Empty() {
		return nil
	}
	var forwardRefs catalog.DescriptorIDSet
	if desc, err := MustReadImmutableDescriptor(ctx, m.cr, op.BackReferencedDescID); err != nil {
		return err
	} else if !desc.Dropped() {
		desc, err = m.s.CheckOutDescriptor(ctx, desc.GetID())
		if err != nil {
			return err
		}
		if !desc.Dropped() {
			forwardRefs, err = desc.GetReferencedDescIDs()
			if err != nil {
				return err
			}
		}
	}
	for _, typeID := range op.TypeIDs.Ordered() {
		if desc, err := MustReadImmutableDescriptor(ctx, m.cr, typeID); err != nil {
			return err
		} else if desc.Dropped() {
			continue
		}
		typ, err := m.checkOutType(ctx, typeID)
		if err != nil {
			return err
		}
		if typ.Dropped() {
			continue
		}
		backRefs := catalog.MakeDescriptorIDSet(typ.ReferencingDescriptorIDs...)
		if forwardRefs.Contains(typeID) {
			if backRefs.Contains(op.BackReferencedDescID) {
				continue
			}
			backRefs.Add(op.BackReferencedDescID)
		} else {
			if !backRefs.Contains(op.BackReferencedDescID) {
				continue
			}
			backRefs.Remove(op.BackReferencedDescID)
		}
		typ.ReferencingDescriptorIDs = backRefs.Ordered()
	}
	return nil
}

func (m *visitor) UpdateBackReferencesInSequences(
	ctx context.Context, op scop.UpdateBackReferencesInSequences,
) error {
	if op.SequenceIDs.Empty() {
		return nil
	}
	var forwardRefs catalog.DescriptorIDSet
	if desc, err := MustReadImmutableDescriptor(ctx, m.cr, op.BackReferencedTableID); err != nil {
		return err
	} else if !desc.Dropped() {
		tbl, err := m.checkOutTable(ctx, op.BackReferencedTableID)
		if err != nil {
			return err
		}
		if !tbl.Dropped() {
			if op.BackReferencedColumnID != 0 {
				col, err := tbl.FindColumnWithID(op.BackReferencedColumnID)
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
				for _, c := range tbl.AllActiveAndInactiveChecks() {
					ids, err := sequenceIDsInExpr(c.Expr)
					if err != nil {
						return err
					}
					ids.ForEach(forwardRefs.Add)
				}
			}
		}
	}
	for _, seqID := range op.SequenceIDs.Ordered() {
		if err := updateBackReferencesInSequences(ctx, m, seqID, op.BackReferencedTableID, op.BackReferencedColumnID, forwardRefs); err != nil {
			return err
		}
	}
	return nil
}

func updateBackReferencesInSequences(
	ctx context.Context,
	m *visitor,
	seqID, tblID descpb.ID,
	colID descpb.ColumnID,
	forwardRefs catalog.DescriptorIDSet,
) error {
	if desc, err := MustReadImmutableDescriptor(ctx, m.cr, seqID); err != nil || desc.Dropped() {
		return err
	}
	seq, err := m.checkOutTable(ctx, seqID)
	if err != nil || seq.Dropped() {
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
		updated.Add(colID)
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
	for _, relationID := range op.RelationIDs.Ordered() {
		if err := removeViewBackReferencesInRelation(ctx, m, relationID, op.BackReferencedViewID); err != nil {
			return err
		}
	}
	return nil
}

func removeViewBackReferencesInRelation(
	ctx context.Context, m *visitor, relationID, viewID descpb.ID,
) error {
	if desc, err := MustReadImmutableDescriptor(ctx, m.cr, relationID); err != nil || desc.Dropped() {
		return err
	}
	tbl, err := m.checkOutTable(ctx, relationID)
	if err != nil || tbl.Dropped() {
		return err
	}
	tbl.UpdateColumnsDependedOnBy(viewID, catalog.TableColSet{})
	return nil
}
