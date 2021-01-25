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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// MutableDescGetter encapsulates the logic to retrieve descriptors.
// All retrieved descriptors are modified.
type MutableDescGetter interface {
	GetMutableTableByID(ctx context.Context, id descpb.ID) (*tabledesc.Mutable, error)
}

// NewMutationVisitor creates a new scop.MutationVisitor.
func NewMutationVisitor(descs MutableDescGetter) scop.MutationVisitor {
	return &visitor{descs: descs}
}

type visitor struct {
	descs MutableDescGetter
}

func (m *visitor) MakeAddedColumnDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeAddedColumnDeleteAndWriteOnly,
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

func (m *visitor) MakeColumnPublic(ctx context.Context, op scop.MakeColumnPublic) error {
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

func (m *visitor) MakeDroppedNonPrimaryIndexDeleteAndWriteOnly(
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

func (m *visitor) MakeDroppedColumnDeleteAndWriteOnly(
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

func (m *visitor) MakeDroppedColumnDeleteOnly(
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

func (m *visitor) MakeColumnAbsent(ctx context.Context, op scop.MakeColumnAbsent) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	mut, err := removeMutation(
		ctx,
		table,
		getColumnMutation(op.ColumnID),
		descpb.DescriptorMutation_DELETE_ONLY,
	)
	if err != nil {
		return err
	}
	col := mut.GetColumn()
	table.RemoveColumnFromFamily(col.ID)
	return nil
}

func (m *visitor) MakeAddedIndexDeleteAndWriteOnly(
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

func (m *visitor) MakeAddedColumnDeleteOnly(
	ctx context.Context, op scop.MakeAddedColumnDeleteOnly,
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
		if foundFamily = fam.ID == op.FamilyID; foundFamily {
			fam.ColumnIDs = append(fam.ColumnIDs, op.Column.ID)
			fam.ColumnNames = append(fam.ColumnNames, op.Column.Name)
			break
		}
	}
	if !foundFamily {
		table.Families = append(table.Families, descpb.ColumnFamilyDescriptor{
			Name:        op.FamilyName,
			ID:          op.FamilyID,
			ColumnNames: []string{op.Column.Name},
			ColumnIDs:   []descpb.ColumnID{op.Column.ID},
		})
		sort.Slice(table.Families, func(i, j int) bool {
			return table.Families[i].ID < table.Families[j].ID
		})
		if table.NextFamilyID <= op.FamilyID {
			table.NextFamilyID = op.FamilyID + 1
		}
	}
	table.AddColumnMutation(&op.Column, descpb.DescriptorMutation_ADD)
	return nil
}

func (m *visitor) MakeDroppedIndexDeleteOnly(
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

func (m *visitor) MakeDroppedPrimaryIndexDeleteAndWriteOnly(
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

func (m *visitor) MakeAddedIndexDeleteOnly(
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

func (m *visitor) AddCheckConstraint(ctx context.Context, op scop.AddCheckConstraint) error {
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

func (m *visitor) MakeAddedPrimaryIndexPublic(
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

func (m *visitor) MakeIndexAbsent(ctx context.Context, op scop.MakeIndexAbsent) error {
	table, err := m.descs.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	_, err = removeMutation(ctx, table, getIndexMutation(op.IndexID), descpb.DescriptorMutation_DELETE_ONLY)
	return err
}

func (m *visitor) AddColumnFamily(ctx context.Context, op scop.AddColumnFamily) error {
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

var _ scop.MutationVisitor = (*visitor)(nil)
