// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachange"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func (i *immediateVisitor) MakeAbsentColumnDeleteOnly(
	ctx context.Context, op scop.MakeAbsentColumnDeleteOnly,
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
	tbl, err := i.checkOutTable(ctx, op.Column.TableID)
	if err != nil {
		return err
	}
	if col.ID >= tbl.NextColumnID {
		tbl.NextColumnID = col.ID + 1
	}
	enqueueNonIndexMutation(tbl, tbl.AddColumnMutation, col, descpb.DescriptorMutation_ADD)
	return nil
}

func (i *immediateVisitor) UpsertColumnType(ctx context.Context, op scop.UpsertColumnType) error {
	tbl, err := i.checkOutTable(ctx, op.ColumnType.TableID)
	if err != nil {
		return err
	}

	catCol, err := catalog.MustFindColumnByID(tbl, op.ColumnType.ColumnID)
	if err != nil {
		return err
	}
	col := catCol.ColumnDesc()

	// This can be called when adding a new column or for an update to existing
	// column. If the column type is set, then this implies we are updating the
	// type of an existing column.
	if catCol.HasType() {
		return i.updateExistingColumnType(ctx, op, col)
	}
	return i.addNewColumnType(ctx, op, tbl, col)
}

// addNewColumnType is called when adding a ColumnType for a new column.
func (i *immediateVisitor) addNewColumnType(
	ctx context.Context,
	op scop.UpsertColumnType,
	tbl *tabledesc.Mutable,
	col *descpb.ColumnDescriptor,
) error {
	col.Type = op.ColumnType.Type
	if op.ColumnType.ElementCreationMetadata.In_23_1OrLater {
		col.Nullable = true
	} else {
		col.Nullable = op.ColumnType.IsNullable
	}
	col.Virtual = op.ColumnType.IsVirtual
	// ComputeExpr is deprecated in favor of a separate element
	// (ColumnComputeExpression). Any changes in this if block
	// should also be made in the AddColumnComputeExpression function.
	if !op.ColumnType.ElementCreationMetadata.In_24_3OrLater {
		if ce := op.ColumnType.ComputeExpr; ce != nil {
			expr := string(ce.Expr)
			col.ComputeExpr = &expr
			col.UsesSequenceIds = ce.UsesSequenceIDs
		}
	}
	if !col.Virtual {
		for i := range tbl.Families {
			fam := &tbl.Families[i]
			if fam.ID == op.ColumnType.FamilyID {
				fam.ColumnIDs = append(fam.ColumnIDs, col.ID)
				fam.ColumnNames = append(fam.ColumnNames, col.Name)
				break
			}
		}
	}
	// Empty names are allowed for families, in which case AllocateIDs will assign
	// one.
	return tbl.AllocateIDsWithoutValidation(ctx, false /* createMissingPrimaryKey */)
}

// AddColumnComputeExpression will set a compute expression to a column.
func (i *immediateVisitor) AddColumnComputeExpression(
	ctx context.Context, op scop.AddColumnComputeExpression,
) error {
	return i.updateColumnComputeExpression(ctx, op.ComputeExpression.TableID, op.ComputeExpression.ColumnID,
		&op.ComputeExpression.Expr)
}

// RemoveColumnComputeExpression will drop a compute expression from a column.
func (i *immediateVisitor) RemoveColumnComputeExpression(
	ctx context.Context, op scop.RemoveColumnComputeExpression,
) error {
	return i.updateColumnComputeExpression(ctx, op.TableID, op.ColumnID, nil)
}

// updateColumnComputeExpression will handle add or removal of a compute expression.
func (i *immediateVisitor) updateColumnComputeExpression(
	ctx context.Context, tableID descpb.ID, columnID descpb.ColumnID, expr *catpb.Expression,
) error {
	tbl, err := i.checkOutTable(ctx, tableID)
	if err != nil {
		return err
	}

	catCol, err := catalog.MustFindColumnByID(tbl, columnID)
	if err != nil {
		return err
	}

	col := catCol.ColumnDesc()
	if expr == nil {
		clearComputedExpr(col)
	} else {
		expr := string(*expr)
		col.ComputeExpr = &expr
	}
	if err := updateColumnExprSequenceUsage(col); err != nil {
		return err
	}
	return updateColumnExprFunctionsUsage(col)
}

func (i *immediateVisitor) MakeDeleteOnlyColumnWriteOnly(
	ctx context.Context, op scop.MakeDeleteOnlyColumnWriteOnly,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		tbl,
		MakeColumnIDMutationSelector(op.ColumnID),
		descpb.DescriptorMutation_DELETE_ONLY,
		descpb.DescriptorMutation_WRITE_ONLY,
		descpb.DescriptorMutation_ADD,
	)
}

func (i *immediateVisitor) MakeWriteOnlyColumnPublic(
	ctx context.Context, op scop.MakeWriteOnlyColumnPublic,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	mut, err := RemoveMutation(
		tbl,
		MakeColumnIDMutationSelector(op.ColumnID),
		descpb.DescriptorMutation_WRITE_ONLY,
	)
	if err != nil {
		return err
	}
	// TODO(ajwerner): Should the op just have the column descriptor? What's the
	// type hydration status here? Cloning is going to blow away hydration. Is
	// that okay?
	tbl.Columns = append(tbl.Columns,
		*(protoutil.Clone(mut.GetColumn())).(*descpb.ColumnDescriptor))

	// Ensure that the column is added in the right location. This is important
	// when rolling back dropped columns.
	getID := func(col *descpb.ColumnDescriptor) int {
		if col.PGAttributeNum != 0 {
			return int(col.PGAttributeNum)
		}
		return int(col.ID)
	}
	sort.Slice(tbl.Columns, func(i, j int) bool {
		return getID(&tbl.Columns[i]) < getID(&tbl.Columns[j])
	})
	return nil
}

func (i *immediateVisitor) MakePublicColumnWriteOnly(
	ctx context.Context, op scop.MakePublicColumnWriteOnly,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	for i, col := range tbl.PublicColumns() {
		if col.GetID() == op.ColumnID {
			desc := col.ColumnDescDeepCopy()
			tbl.Columns = append(tbl.Columns[:i], tbl.Columns[i+1:]...)
			enqueueNonIndexMutation(tbl, tbl.AddColumnMutation, &desc, descpb.DescriptorMutation_DROP)
			return nil
		}
	}
	return errors.AssertionFailedf("failed to find column %d in table %q (%d)",
		op.ColumnID, tbl.GetName(), tbl.GetID())
}

func (i *immediateVisitor) MakeWriteOnlyColumnDeleteOnly(
	ctx context.Context, op scop.MakeWriteOnlyColumnDeleteOnly,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	return mutationStateChange(
		tbl,
		MakeColumnIDMutationSelector(op.ColumnID),
		descpb.DescriptorMutation_WRITE_ONLY,
		descpb.DescriptorMutation_DELETE_ONLY,
		descpb.DescriptorMutation_DROP,
	)
}

func (i *immediateVisitor) RemoveDroppedColumnType(
	ctx context.Context, op scop.RemoveDroppedColumnType,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	mut, err := FindMutation(tbl, MakeColumnIDMutationSelector(op.ColumnID))
	if err != nil || mut.AsColumn().IsSystemColumn() {
		return err
	}
	col := mut.AsColumn().ColumnDesc()
	col.Type = types.Any
	if col.IsComputed() {
		clearComputedExpr(col)
	}
	return nil
}

func (i *immediateVisitor) MakeDeleteOnlyColumnAbsent(
	ctx context.Context, op scop.MakeDeleteOnlyColumnAbsent,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	mut, err := RemoveMutation(
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

func (i *immediateVisitor) AddColumnFamily(ctx context.Context, op scop.AddColumnFamily) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
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

func (i *immediateVisitor) AssertColumnFamilyIsRemoved(
	ctx context.Context, op scop.AssertColumnFamilyIsRemoved,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	for idx := range tbl.Families {
		if tbl.Families[idx].ID == op.FamilyID {
			return errors.AssertionFailedf("column family was leaked during schema change %v",
				tbl.Families[idx])
		}
	}
	return nil
}

func (i *immediateVisitor) SetColumnName(ctx context.Context, op scop.SetColumnName) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	col, err := catalog.MustFindColumnByID(tbl, op.ColumnID)
	if err != nil {
		return errors.AssertionFailedf("column %d not found in table %q (%d)", op.ColumnID, tbl.GetName(), tbl.GetID())
	}
	return tabledesc.RenameColumnInTable(tbl, col, tree.Name(op.Name), nil /* isShardColumnRenameable */)
}

func (i *immediateVisitor) AddColumnDefaultExpression(
	ctx context.Context, op scop.AddColumnDefaultExpression,
) error {
	tbl, err := i.checkOutTable(ctx, op.Default.TableID)
	if err != nil {
		return err
	}
	col, err := catalog.MustFindColumnByID(tbl, op.Default.ColumnID)
	if err != nil {
		return err
	}
	d := col.ColumnDesc()
	expr := string(op.Default.Expr)
	d.DefaultExpr = &expr
	seqRefs := catalog.MakeDescriptorIDSet(d.UsesSequenceIds...)
	for _, seqID := range op.Default.UsesSequenceIDs {
		if seqRefs.Contains(seqID) {
			continue
		}
		d.UsesSequenceIds = append(d.UsesSequenceIds, seqID)
		seqRefs.Add(seqID)
	}

	fnRefs := catalog.MakeDescriptorIDSet(d.UsesFunctionIds...)
	for _, fnID := range op.Default.UsesFunctionIDs {
		if fnRefs.Contains(fnID) {
			continue
		}
		d.UsesFunctionIds = append(d.UsesFunctionIds, fnID)
		fnRefs.Add(fnID)
	}

	return nil
}

func (i *immediateVisitor) RemoveColumnDefaultExpression(
	ctx context.Context, op scop.RemoveColumnDefaultExpression,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	col, err := catalog.MustFindColumnByID(tbl, op.ColumnID)
	if err != nil {
		return err
	}
	d := col.ColumnDesc()
	d.DefaultExpr = nil
	if err := updateColumnExprSequenceUsage(d); err != nil {
		return err
	}
	return updateColumnExprFunctionsUsage(d)
}

func (i *immediateVisitor) AddColumnOnUpdateExpression(
	ctx context.Context, op scop.AddColumnOnUpdateExpression,
) error {
	tbl, err := i.checkOutTable(ctx, op.OnUpdate.TableID)
	if err != nil {
		return err
	}
	col, err := catalog.MustFindColumnByID(tbl, op.OnUpdate.ColumnID)
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

func (i *immediateVisitor) RemoveColumnOnUpdateExpression(
	ctx context.Context, op scop.RemoveColumnOnUpdateExpression,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	col, err := catalog.MustFindColumnByID(tbl, op.ColumnID)
	if err != nil {
		return err
	}
	d := col.ColumnDesc()
	d.OnUpdateExpr = nil
	return updateColumnExprSequenceUsage(d)
}

// updateExistingColumnType will handle data type changes to existing columns.
func (i *immediateVisitor) updateExistingColumnType(
	ctx context.Context, op scop.UpsertColumnType, desc *descpb.ColumnDescriptor,
) error {
	kind, err := schemachange.ClassifyConversion(ctx, desc.Type, op.ColumnType.Type)
	if err != nil {
		return err
	}
	switch kind {
	case schemachange.ColumnConversionTrivial, schemachange.ColumnConversionValidate:
		// This type of update are ones that don't do a backfill. So, we can simply
		// update the column type and be done.
		desc.Type = op.ColumnType.Type
	default:
		return errors.AssertionFailedf("unsupported column type change %v -> %v (%v)",
			desc.Type, op.ColumnType.Type, kind)
	}
	return nil
}

func clearComputedExpr(col *descpb.ColumnDescriptor) {
	// This operation zeros out the computed column expression to remove references
	// to sequences or other dependencies, but it can't always remove the expression entirely.
	//
	// For virtual computed columns, removing the expression would turn the column
	// into a virtual non-computed column, which doesn't make sense. When dropping
	// the expression for a column that still exists (e.g., a stored column), we do
	// want to remove the expression.
	if col.Virtual {
		null := tree.Serialize(tree.DNull)
		col.ComputeExpr = &null
	} else {
		col.ComputeExpr = nil
	}
}
