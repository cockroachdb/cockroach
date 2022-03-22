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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func (m *visitor) MakeAddedColumnDeleteOnly(
	ctx context.Context, op scop.MakeAddedColumnDeleteOnly,
) error {
	col := &descpb.ColumnDescriptor{
		ID:                      op.Column.ColumnID,
		Name:                    tabledesc.ColumnNamePlaceholder(op.Column.ColumnID),
		Hidden:                  op.Column.IsHidden,
		Inaccessible:            op.Column.IsInaccessible,
		GeneratedAsIdentityType: op.Column.GeneratedAsIdentityType,
		PGAttributeNum:          op.Column.PgAttributeNum,
	}
	if o := op.Column.GeneratedAsIdentitySequenceOption; o != "" {
		col.GeneratedAsIdentitySequenceOption = &o
	}
	tbl, err := m.checkOutTable(ctx, op.Column.TableID)
	if err != nil {
		return err
	}
	if col.ID >= tbl.NextColumnID {
		tbl.NextColumnID = col.ID + 1
	}
	return enqueueAddColumnMutation(tbl, col)
}

func (m *visitor) SetAddedColumnType(ctx context.Context, op scop.SetAddedColumnType) error {
	tbl, err := m.checkOutTable(ctx, op.ColumnType.TableID)
	if err != nil {
		return err
	}
	mut, err := FindMutation(tbl, MakeColumnIDMutationSelector(op.ColumnType.ColumnID))
	if err != nil {
		return err
	}
	col := mut.AsColumn().ColumnDesc()
	col.Type = op.ColumnType.Type
	col.Nullable = op.ColumnType.IsNullable
	col.Virtual = op.ColumnType.IsVirtual
	if ce := op.ColumnType.ComputeExpr; ce != nil {
		expr := string(ce.Expr)
		col.ComputeExpr = &expr
		col.UsesSequenceIds = ce.UsesSequenceIDs
	}
	if col.ComputeExpr == nil || !col.Virtual {
		for i := range tbl.Families {
			fam := &tbl.Families[i]
			if fam.ID == op.ColumnType.FamilyID {
				fam.ColumnIDs = append(fam.ColumnIDs, col.ID)
				fam.ColumnNames = append(fam.ColumnNames, col.Name)
				break
			}
		}
	}
	return nil
}

func (m *visitor) MakeAddedColumnDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeAddedColumnDeleteAndWriteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		tbl,
		MakeColumnIDMutationSelector(op.ColumnID),
		descpb.DescriptorMutation_DELETE_ONLY,
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	)
}

func (m *visitor) MakeColumnPublic(ctx context.Context, op scop.MakeColumnPublic) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	mut, err := removeMutation(
		tbl,
		MakeColumnIDMutationSelector(op.ColumnID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	)
	if err != nil {
		return err
	}
	// TODO(ajwerner): Should the op just have the column descriptor? What's the
	// type hydration status here? Cloning is going to blow away hydration. Is
	// that okay?
	tbl.Columns = append(tbl.Columns,
		*(protoutil.Clone(mut.GetColumn())).(*descpb.ColumnDescriptor))
	return nil
}

func (m *visitor) MakeDroppedColumnDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeDroppedColumnDeleteAndWriteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	for i, col := range tbl.PublicColumns() {
		if col.GetID() == op.ColumnID {
			desc := col.ColumnDescDeepCopy()
			tbl.Columns = append(tbl.Columns[:i], tbl.Columns[i+1:]...)
			return enqueueDropColumnMutation(tbl, &desc)
		}
	}
	return errors.AssertionFailedf("failed to find column %d in table %q (%d)",
		op.ColumnID, tbl.GetName(), tbl.GetID())
}

func (m *visitor) MakeDroppedColumnDeleteOnly(
	ctx context.Context, op scop.MakeDroppedColumnDeleteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		tbl,
		MakeColumnIDMutationSelector(op.ColumnID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		descpb.DescriptorMutation_DELETE_ONLY,
	)
}

func (m *visitor) RemoveDroppedColumnType(
	ctx context.Context, op scop.RemoveDroppedColumnType,
) error {
	if desc, err := m.s.GetDescriptor(ctx, op.TableID); err != nil || desc.Dropped() {
		return err
	}
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	mut, err := FindMutation(tbl, MakeColumnIDMutationSelector(op.ColumnID))
	if err != nil {
		return err
	}
	col := mut.AsColumn().ColumnDesc()
	col.ComputeExpr = nil
	col.Type = types.Any
	return nil
}

func (m *visitor) MakeColumnAbsent(ctx context.Context, op scop.MakeColumnAbsent) error {
	if desc, err := m.s.GetDescriptor(ctx, op.TableID); err != nil || desc.Dropped() {
		return err
	}
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	mut, err := removeMutation(
		tbl,
		MakeColumnIDMutationSelector(op.ColumnID),
		descpb.DescriptorMutation_DELETE_ONLY,
	)
	if err != nil {
		return err
	}
	col := mut.GetColumn()
	tbl.RemoveColumnFromFamilyAndPrimaryIndex(col.ID)
	return nil
}

func (m *visitor) AddColumnFamily(ctx context.Context, op scop.AddColumnFamily) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	family := descpb.ColumnFamilyDescriptor{
		Name: op.Name,
		ID:   op.FamilyID,
	}
	tbl.AddFamily(family)
	if family.ID >= tbl.NextFamilyID {
		tbl.NextFamilyID = family.ID + 1
	}
	return nil
}

func (m *visitor) SetColumnName(ctx context.Context, op scop.SetColumnName) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	col, err := tbl.FindColumnWithID(op.ColumnID)
	if err != nil {
		return errors.AssertionFailedf("column %d not found in table %q (%d)", op.ColumnID, tbl.GetName(), tbl.GetID())
	}
	return tabledesc.RenameColumnInTable(tbl, col, tree.Name(op.Name), nil /* isShardColumnRenameable */)
}

func (m *visitor) AddColumnDefaultExpression(
	ctx context.Context, op scop.AddColumnDefaultExpression,
) error {
	tbl, err := m.checkOutTable(ctx, op.Default.TableID)
	if err != nil {
		return err
	}
	col, err := tbl.FindColumnWithID(op.Default.ColumnID)
	if err != nil {
		return err
	}
	d := col.ColumnDesc()
	expr := string(op.Default.Expr)
	d.DefaultExpr = &expr
	refs := catalog.MakeDescriptorIDSet(d.UsesSequenceIds...)
	for _, seqID := range op.Default.UsesSequenceIDs {
		if refs.Contains(seqID) {
			continue
		}
		d.UsesSequenceIds = append(d.UsesSequenceIds, seqID)
		refs.Add(seqID)
	}
	return nil
}

func (m *visitor) RemoveColumnDefaultExpression(
	ctx context.Context, op scop.RemoveColumnDefaultExpression,
) error {
	if desc, err := m.s.GetDescriptor(ctx, op.TableID); err != nil || desc.Dropped() {
		return err
	}
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	col, err := tbl.FindColumnWithID(op.ColumnID)
	if err != nil {
		return err
	}
	d := col.ColumnDesc()
	d.DefaultExpr = nil
	return updateColumnExprSequenceUsage(d)
}

func (m *visitor) AddColumnOnUpdateExpression(
	ctx context.Context, op scop.AddColumnOnUpdateExpression,
) error {
	tbl, err := m.checkOutTable(ctx, op.OnUpdate.TableID)
	if err != nil {
		return err
	}
	col, err := tbl.FindColumnWithID(op.OnUpdate.ColumnID)
	if err != nil {
		return err
	}
	d := col.ColumnDesc()
	expr := string(op.OnUpdate.Expr)
	d.OnUpdateExpr = &expr
	refs := catalog.MakeDescriptorIDSet(d.UsesSequenceIds...)
	for _, seqID := range op.OnUpdate.UsesSequenceIDs {
		if refs.Contains(seqID) {
			continue
		}
		d.UsesSequenceIds = append(d.UsesSequenceIds, seqID)
		refs.Add(seqID)
	}
	return nil
}

func (m *visitor) RemoveColumnOnUpdateExpression(
	ctx context.Context, op scop.RemoveColumnOnUpdateExpression,
) error {
	if desc, err := m.s.GetDescriptor(ctx, op.TableID); err != nil || desc.Dropped() {
		return err
	}
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	col, err := tbl.FindColumnWithID(op.ColumnID)
	if err != nil {
		return err
	}
	d := col.ColumnDesc()
	d.OnUpdateExpr = nil
	return updateColumnExprSequenceUsage(d)
}
