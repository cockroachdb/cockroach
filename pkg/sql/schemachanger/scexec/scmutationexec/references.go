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
	"github.com/cockroachdb/errors"
)

func (m *visitor) DropForeignKeyRef(ctx context.Context, op scop.DropForeignKeyRef) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	fks := tbl.TableDesc().OutboundFKs
	if !op.Outbound {
		fks = tbl.TableDesc().InboundFKs
	}
	newFks := make([]descpb.ForeignKeyConstraint, 0, len(fks)-1)
	for _, fk := range fks {
		if op.Outbound && (fk.OriginTableID != op.TableID ||
			op.Name != fk.Name) {
			newFks = append(newFks, fk)
		} else if !op.Outbound && (fk.ReferencedTableID != op.TableID ||
			op.Name != fk.Name) {
			newFks = append(newFks, fk)
		}
	}
	if op.Outbound {
		tbl.TableDesc().OutboundFKs = newFks
	} else {
		tbl.TableDesc().InboundFKs = newFks
	}
	return nil
}

func (m *visitor) AddCheckConstraint(ctx context.Context, op scop.AddCheckConstraint) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	ck := &descpb.TableDescriptor_CheckConstraint{
		Expr:      op.Expr,
		Name:      op.Name,
		ColumnIDs: op.ColumnIDs,
		Hidden:    op.Hidden,
	}
	if op.Unvalidated {
		ck.Validity = descpb.ConstraintValidity_Unvalidated
	} else {
		ck.Validity = descpb.ConstraintValidity_Validating
	}
	tbl.Checks = append(tbl.Checks, ck)
	return nil
}

func (m *visitor) AddTypeBackRef(ctx context.Context, op scop.AddTypeBackRef) error {
	typ, err := m.checkOutType(ctx, op.TypeID)
	if err != nil {
		return err
	}
	typ.AddReferencingDescriptorID(op.DescID)
	// Sanity: Validate that a back reference exists by now.
	desc, err := MustReadImmutableDescriptor(ctx, m.cr, op.DescID)
	if err != nil {
		return err
	}
	refDescIDs, err := desc.GetReferencedDescIDs()
	if err != nil {
		return err
	}
	if !refDescIDs.Contains(op.TypeID) {
		return errors.AssertionFailedf("Back reference for type %d is missing inside descriptor %d.",
			op.TypeID, op.DescID)
	}
	return nil
}

func (m *visitor) RemoveRelationDependedOnBy(
	ctx context.Context, op scop.RemoveRelationDependedOnBy,
) error {
	// Clean up dependencies for the relationship.
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	depDesc, err := m.checkOutTable(ctx, op.DependedOnBy)
	if err != nil {
		return err
	}
	// DependedOnBy can contain multiple entries per-dependency, so
	// this isn't a single delete operation.
	newDependedOnBy := make([]descpb.TableDescriptor_Reference, 0, len(tbl.DependedOnBy))
	for _, dependsOnBy := range tbl.DependedOnBy {
		if dependsOnBy.ID != op.DependedOnBy {
			newDependedOnBy = append(newDependedOnBy, dependsOnBy)
		}
	}
	tbl.DependedOnBy = newDependedOnBy
	// Intentionally set empty slices to nil, so that for our data driven tests
	// these fields are omitted in the output.
	if len(tbl.DependedOnBy) == 0 {
		tbl.DependedOnBy = nil
	}

	for depIdx, dependsOn := range depDesc.DependsOn {
		if dependsOn == op.TableID {
			depDesc.DependsOn = append(depDesc.DependsOn[:depIdx], depDesc.DependsOn[depIdx+1:]...)
			break
		}
	}
	// Intentionally set empty slices to nil, so that for our data driven tests
	// these fields are omitted in the output.
	if len(depDesc.DependsOn) == 0 {
		depDesc.DependsOn = nil
	}
	return nil
}

func (m *visitor) RemoveSequenceOwnedBy(ctx context.Context, op scop.RemoveSequenceOwnedBy) error {
	tbl, err := m.checkOutTable(ctx, op.SequenceID)
	if err != nil {
		return err
	}
	// Clean up the ownership inside the owning table first.
	sequenceOwner := &tbl.GetSequenceOpts().SequenceOwner
	ownedByTbl, err := m.checkOutTable(ctx, sequenceOwner.OwnerTableID)
	if err != nil {
		return err
	}
	col, err := ownedByTbl.FindColumnWithID(sequenceOwner.OwnerColumnID)
	if err != nil {
		return err
	}
	if found := removeOwnedByFromColumn(col.ColumnDesc(), op.SequenceID); !found {
		return errors.AssertionFailedf("unable to find sequence (%d) owned by"+
			" inside table (%d) and column (%d)",
			op.SequenceID,
			sequenceOwner.OwnerTableID,
			sequenceOwner.OwnerColumnID)
	}
	// Next, clean the ownership on the sequence.
	tbl.GetSequenceOpts().SequenceOwner.OwnerTableID = descpb.InvalidID
	tbl.GetSequenceOpts().SequenceOwner.OwnerColumnID = 0
	return nil
}

func removeOwnedByFromColumn(col *descpb.ColumnDescriptor, seqID descpb.ID) (found bool) {
	for idx := range col.OwnsSequenceIds {
		if col.OwnsSequenceIds[idx] == seqID {
			col.OwnsSequenceIds = append(
				col.OwnsSequenceIds[:idx],
				col.OwnsSequenceIds[idx+1:]...)
			return true
		}
	}
	return false
}

func (m *visitor) RemoveTypeBackRef(ctx context.Context, op scop.RemoveTypeBackRef) error {
	typ, err := m.checkOutType(ctx, op.TypeID)
	if err != nil {
		return err
	}
	typ.RemoveReferencingDescriptorID(op.DescID)
	return nil
}

func (m *visitor) RemoveColumnSequenceReferences(
	ctx context.Context, op scop.RemoveColumnSequenceReferences,
) error {
	seqIDs := catalog.MakeDescriptorIDSet(op.SequenceIDs...)
	if seqIDs.Empty() {
		return nil
	}

	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	newRefs := make([]descpb.TableDescriptor_Reference, 0, len(tbl.DependedOnBy))
	for _, ref := range tbl.DependedOnBy {
		if seqIDs.Contains(ref.ID) {
			for j, colID := range ref.ColumnIDs {
				if colID == op.ColumnID {
					ref.ColumnIDs = append(ref.ColumnIDs[:j], ref.ColumnIDs[j+1:]...)
					break
				}
			}
		}
		newRefs = append(newRefs, ref)
	}
	if len(newRefs) == 0 {
		newRefs = nil
	}
	tbl.DependedOnBy = newRefs
	return nil
}

func (m *visitor) DeleteDatabaseSchemaEntry(
	ctx context.Context, op scop.DeleteDatabaseSchemaEntry,
) error {
	db, err := m.checkOutDatabase(ctx, op.DatabaseID)
	if err != nil {
		return err
	}
	sc, err := MustReadImmutableDescriptor(ctx, m.cr, op.SchemaID)
	if err != nil {
		return err
	}
	delete(db.Schemas, sc.GetName())
	return nil
}
