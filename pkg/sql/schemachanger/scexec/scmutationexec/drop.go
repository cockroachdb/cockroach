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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func (m *visitor) CreateGcJobForTable(ctx context.Context, op scop.CreateGcJobForTable) error {
	desc, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	m.s.AddNewGCJobForTable(desc)
	return nil
}

func (m *visitor) CreateGcJobForDatabase(
	ctx context.Context, op scop.CreateGcJobForDatabase,
) error {
	desc, err := m.checkOutDatabase(ctx, op.DatabaseID)
	if err != nil {
		return err
	}
	m.s.AddNewGCJobForDatabase(desc)
	return nil
}

func (m *visitor) CreateGcJobForIndex(ctx context.Context, op scop.CreateGcJobForIndex) error {
	desc, err := m.s.GetDescriptor(ctx, op.TableID)
	if err != nil {
		return err
	}
	tbl, err := catalog.AsTableDescriptor(desc)
	if err != nil {
		return err
	}
	idx, err := tbl.FindIndexWithID(op.IndexID)
	if err != nil {
		return errors.AssertionFailedf("table %q (%d): could not find index %d", tbl.GetName(), tbl.GetID(), op.IndexID)
	}
	m.s.AddNewGCJobForIndex(tbl, idx)
	return nil
}

func (m *visitor) MarkDescriptorAsDropped(
	ctx context.Context, op scop.MarkDescriptorAsDropped,
) error {
	// Before we can mutate the descriptor, get rid of any synthetic descriptor.
	m.sd.RemoveSyntheticDescriptor(op.DescID)
	desc, err := m.s.CheckOutDescriptor(ctx, op.DescID)
	if err != nil {
		return err
	}
	desc.SetDropped()
	// After marking a table as dropped we will populate the drop time.
	if tableDesc, ok := desc.(*tabledesc.Mutable); ok && tableDesc.IsTable() {
		tableDesc.DropTime = timeutil.Now().UnixNano()
	}
	return nil
}

func (m *visitor) MarkDescriptorAsDroppedSynthetically(
	ctx context.Context, op scop.MarkDescriptorAsDroppedSynthetically,
) error {
	if co := m.s.MaybeCheckedOutDescriptor(op.DescID); co != nil {
		return errors.AssertionFailedf("cannot mark already checked-out descriptor %q (%d) as synthetic",
			co.GetName(), co.GetID())
	}
	desc, err := m.s.GetDescriptor(ctx, op.DescID)
	if err != nil {
		return err
	}
	mut := desc.NewBuilder().BuildCreatedMutable()
	mut.SetDropped()
	m.sd.AddSyntheticDescriptor(mut)
	return nil
}

func (m *visitor) DrainDescriptorName(_ context.Context, op scop.DrainDescriptorName) error {
	nameDetails := descpb.NameInfo{
		ParentID:       op.Namespace.DatabaseID,
		ParentSchemaID: op.Namespace.SchemaID,
		Name:           op.Namespace.Name,
	}
	m.s.AddDrainedName(op.Namespace.DescriptorID, nameDetails)
	return nil
}

func (m *visitor) DeleteDescriptor(_ context.Context, op scop.DeleteDescriptor) error {
	m.s.DeleteDescriptor(op.DescriptorID)
	return nil
}

func (m *visitor) RemoveTableComment(_ context.Context, op scop.RemoveTableComment) error {
	m.s.DeleteComment(op.TableID, 0, keys.TableCommentType)
	return nil
}

func (m *visitor) RemoveDatabaseComment(_ context.Context, op scop.RemoveDatabaseComment) error {
	m.s.DeleteComment(op.DatabaseID, 0, keys.DatabaseCommentType)
	return nil
}

func (m *visitor) RemoveSchemaComment(_ context.Context, op scop.RemoveSchemaComment) error {
	m.s.DeleteComment(op.SchemaID, 0, keys.SchemaCommentType)
	return nil
}

func (m *visitor) RemoveIndexComment(_ context.Context, op scop.RemoveIndexComment) error {
	m.s.DeleteComment(op.TableID, int(op.IndexID), keys.IndexCommentType)
	return nil
}

func (m *visitor) RemoveColumnComment(_ context.Context, op scop.RemoveColumnComment) error {
	m.s.DeleteComment(op.TableID, int(op.ColumnID), keys.ColumnCommentType)
	return nil
}

func (m *visitor) RemoveConstraintComment(
	ctx context.Context, op scop.RemoveConstraintComment,
) error {
	return m.s.DeleteConstraintComment(ctx, op.TableID, op.ConstraintID)
}

func (m *visitor) RemoveDatabaseRoleSettings(
	ctx context.Context, op scop.RemoveDatabaseRoleSettings,
) error {
	return m.s.DeleteDatabaseRoleSettings(ctx, op.DatabaseID)
}

func (m *visitor) DeleteSchedule(_ context.Context, op scop.DeleteSchedule) error {
	if op.ScheduleID != 0 {
		m.s.DeleteSchedule(op.ScheduleID)
	}
	return nil
}
