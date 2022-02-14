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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
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
	desc, err := MustReadImmutableDescriptor(ctx, m.cr, op.TableID)
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
	m.cr.RemoveSyntheticDescriptor(op.DescID)
	desc, err := m.s.CheckOutDescriptor(ctx, op.DescID)
	if err != nil {
		return err
	}
	desc.SetDropped()
	return nil
}

func (m *visitor) MarkDescriptorAsDroppedSynthetically(
	ctx context.Context, op scop.MarkDescriptorAsDroppedSynthetically,
) error {
	desc, err := MustReadImmutableDescriptor(ctx, m.cr, op.DescID)
	if err != nil {
		return err
	}
	mut := desc.NewBuilder().BuildCreatedMutable()
	mut.SetDropped()
	m.cr.AddSyntheticDescriptor(mut)
	return nil
}

func (m *visitor) DrainDescriptorName(ctx context.Context, op scop.DrainDescriptorName) error {
	descriptor, err := MustReadImmutableDescriptor(ctx, m.cr, op.TableID)
	if err != nil {
		return err
	}
	// Queue up names for draining.
	nameDetails := descpb.NameInfo{
		ParentID:       descriptor.GetParentID(),
		ParentSchemaID: descriptor.GetParentSchemaID(),
		Name:           descriptor.GetName()}
	m.s.AddDrainedName(descriptor.GetID(), nameDetails)
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
	tbl, err := MustReadImmutableDescriptor(ctx, m.cr, op.TableID)
	if err != nil {
		return err
	}
	return m.s.DeleteConstraintComment(ctx, tbl.(catalog.TableDescriptor), op.ConstraintID)
}

func (m *visitor) RemoveDatabaseRoleSettings(
	ctx context.Context, op scop.RemoveDatabaseRoleSettings,
) error {
	db, err := MustReadImmutableDescriptor(ctx, m.cr, op.DatabaseID)
	if err != nil {
		return err
	}
	return m.s.DeleteDatabaseRoleSettings(ctx, db.(catalog.DatabaseDescriptor))
}
