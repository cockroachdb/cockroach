// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func NewMutationVisitor(descs MutableDescGetter) *MutationVisitor {
	return &MutationVisitor{descs: descs}
}

type MutationVisitor struct {
	descs MutableDescGetter
}

func (m *MutationVisitor) MakeAddedColumnDescriptorDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeAddedColumnDescriptorDeleteAndWriteOnly,
) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		ctx,
		table,
		getColumnMutation(op.ColumnID),
		descpb.DescriptorMutation_DELETE_ONLY,
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	)
}

func (m *MutationVisitor) MakeColumnDescriptorPublic(
	ctx context.Context, op scop.MakeColumnDescriptorPublic,
) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	mut, err := removeMutation(
		ctx,
		table,
		getColumnMutation(op.ColumnID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	)
	if err != nil {
		return err
	}
	// TODO(ajwerner): Should the op just have the column descriptor? What's the
	// type hydration status here? Cloning is going to blow away hydration. Is
	// that okay?
	table.Columns = append(table.Columns,
		*(protoutil.Clone(mut.GetColumn())).(*descpb.ColumnDescriptor))
	return nil
}

func (m *MutationVisitor) MakeDroppedNonPrimaryIndexDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeDroppedNonPrimaryIndexDeleteAndWriteOnly,
) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	var idx descpb.IndexDescriptor
	for i := range table.Indexes {
		if table.Indexes[i].ID != op.IndexID {
			continue
		}
		idx = table.Indexes[i]
		table.Indexes = append(table.Indexes[:i], table.Indexes[i+1:]...)
		break
	}
	if idx.ID == 0 {
		return errors.AssertionFailedf("failed to find index %d in descriptor %v",
			op.IndexID, table)
	}
	return table.AddIndexMutation(&idx, descpb.DescriptorMutation_DROP)
}

func (m *MutationVisitor) MakeDroppedColumnDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeDroppedColumnDeleteAndWriteOnly,
) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	var col descpb.ColumnDescriptor
	for i := range table.Columns {
		if table.Columns[i].ID != op.ColumnID {
			continue
		}
		col = table.Columns[i]
		table.Columns = append(table.Columns[:i], table.Columns[i+1:]...)
		break
	}
	if col.ID == 0 {
		return errors.AssertionFailedf("failed to find column %d in %v", col.ID, table)
	}
	table.AddColumnMutation(&col, descpb.DescriptorMutation_DROP)
	return nil
}

func (m *MutationVisitor) MakeDroppedColumnDeleteOnly(
	ctx context.Context, op scop.MakeDroppedColumnDeleteOnly,
) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		ctx,
		table,
		getColumnMutation(op.ColumnID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		descpb.DescriptorMutation_DELETE_ONLY,
	)
}

func (m *MutationVisitor) MakeColumnAbsent(ctx context.Context, op scop.MakeColumnAbsent) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	mut, err := removeMutation(
		ctx,
		table,
		getColumnMutation(op.ColumnID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	)
	if err != nil {
		return err
	}
	col := mut.GetColumn()
	// Remove the family if it exists.
	f, err := table.GetFamilyOfColumn(col.ID)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err,
			"removing column family")
	}
	for i := range f.ColumnIDs {
		if f.ColumnIDs[i] == col.ID {
			f.ColumnIDs = append(f.ColumnIDs[:i], f.ColumnIDs[i+1:]...)
			f.ColumnNames = append(f.ColumnNames[:i], f.ColumnNames[i+1:]...)
			break
		}
	}
	if len(f.ColumnIDs) == 0 {
		for i := range table.Families {
			if f == &table.Families[i] {
				table.Families = append(table.Families[:i], table.Families[i+1:]...)
			}
		}
	}
	return nil
}

func (m *MutationVisitor) MakeAddedIndexDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeAddedIndexDeleteAndWriteOnly,
) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		ctx,
		table,
		getIndexMutation(op.IndexID),
		descpb.DescriptorMutation_DELETE_ONLY,
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	)
}

func (m *MutationVisitor) MakeAddedColumnDescriptorDeleteOnly(
	ctx context.Context, op scop.MakeAddedColumnDescriptorDeleteOnly,
) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}

	// TODO(ajwerner): deal with ordering the indexes or sanity checking this
	// or what-not.
	if op.Column.ID >= table.NextColumnID {
		table.NextColumnID = op.Column.ID + 1
	}
	var foundFamily bool
	for i := range table.Families {
		fam := &table.Families[i]
		if foundFamily = fam.ID == op.ColumnFamily; foundFamily {
			fam.ColumnIDs = append(fam.ColumnIDs, op.Column.ID)
			fam.ColumnNames = append(fam.ColumnNames, op.Column.Name)
			break
		}
	}
	// TODO(ajwerner): Create the family
	if !foundFamily {
		panic("not yet implemented")
	}
	table.AddColumnMutation(&op.Column, descpb.DescriptorMutation_ADD)
	return nil
}

func (m *MutationVisitor) MakeDroppedIndexDeleteOnly(
	ctx context.Context, op scop.MakeDroppedIndexDeleteOnly,
) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		ctx,
		table,
		getIndexMutation(op.IndexID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		descpb.DescriptorMutation_DELETE_ONLY,
	)
}

func (m *MutationVisitor) MakeDroppedPrimaryIndexDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeDroppedPrimaryIndexDeleteAndWriteOnly,
) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}

	// NOTE: There is no ordering guarantee between operations which might
	// touch the primary index. Remove it if it has not already been overwritten.
	if table.PrimaryIndex.ID == op.Index.ID {
		table.PrimaryIndex = descpb.IndexDescriptor{}
	}

	idx := protoutil.Clone(&op.Index).(*descpb.IndexDescriptor)
	return table.AddIndexMutation(idx, descpb.DescriptorMutation_DROP)
}

func (m *MutationVisitor) MakeAddedIndexDeleteOnly(
	ctx context.Context, op scop.MakeAddedIndexDeleteOnly,
) error {

	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}

	// TODO(ajwerner): deal with ordering the indexes or sanity checking this
	// or what-not.
	if op.Index.ID >= table.NextIndexID {
		table.NextIndexID = op.Index.ID + 1
	}
	// Make some adjustments to the index descriptor so that it behaves correctly
	// as a secondary index while being added.
	idx := protoutil.Clone(&op.Index).(*descpb.IndexDescriptor)
	return table.AddIndexMutation(idx, descpb.DescriptorMutation_ADD)
}

func (m *MutationVisitor) AddCheckConstraint(
	ctx context.Context, op scop.AddCheckConstraint,
) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
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
	table.Checks = append(table.Checks, ck)
	return nil
}

func (m *MutationVisitor) MakeAddedPrimaryIndexPublic(
	ctx context.Context, op scop.MakeAddedPrimaryIndexPublic,
) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	if _, err := removeMutation(
		ctx,
		table,
		getIndexMutation(op.Index.ID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	); err != nil {
		return err
	}
	table.PrimaryIndex = *(protoutil.Clone(&op.Index)).(*descpb.IndexDescriptor)
	return nil
}

func (m *MutationVisitor) MakeDroppedColumnDescriptorDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeDroppedColumnDescriptorDeleteAndWriteOnly,
) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	table.MaybeIncrementVersion()
	var col descpb.ColumnDescriptor
	for i := range table.Columns {
		if table.Columns[i].ID != op.ColumnID {
			continue
		}
		col = table.Columns[i]
		table.Columns = append(table.Columns[:i], table.Columns[i+1:]...)
		break
	}
	if col.ID == 0 {
		return errors.AssertionFailedf("failed to find column")
	}
	table.AddColumnMutation(&col, descpb.DescriptorMutation_DROP)
	return nil
}

func (m *MutationVisitor) MakeIndexAbsent(ctx context.Context, op scop.MakeIndexAbsent) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	_, err = removeMutation(ctx, table, getIndexMutation(op.IndexID), descpb.DescriptorMutation_DELETE_ONLY)
	return err
}

func (m *MutationVisitor) AddColumnFamily(ctx context.Context, op scop.AddColumnFamily) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	table.AddFamily(op.Family)
	if op.Family.ID >= table.NextFamilyID {
		table.NextFamilyID = op.Family.ID + 1
	}
	return nil
}

var _ scop.MutationVisitor = (*MutationVisitor)(nil)
