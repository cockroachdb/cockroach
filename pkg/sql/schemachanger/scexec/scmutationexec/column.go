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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func (m *visitor) MakeAddedColumnDeleteOnly(
	ctx context.Context, op scop.MakeAddedColumnDeleteOnly,
) error {
	name := tabledesc.ColumnNamePlaceholder(op.ColumnID)
	emptyStrToNil := func(v string) *string {
		if v == "" {
			return nil
		}
		return &v
	}
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}

	if op.ComputerExpr == "" ||
		!op.Virtual {
		foundFamily := false
		for i := range tbl.Families {
			fam := &tbl.Families[i]
			if foundFamily = fam.ID == op.FamilyID; foundFamily {
				fam.ColumnIDs = append(fam.ColumnIDs, op.ColumnID)
				fam.ColumnNames = append(fam.ColumnNames, name)
				break
			}
		}
		if !foundFamily {
			tbl.Families = append(tbl.Families, descpb.ColumnFamilyDescriptor{
				Name:        op.FamilyName,
				ID:          op.FamilyID,
				ColumnNames: []string{name},
				ColumnIDs:   []descpb.ColumnID{op.ColumnID},
			})
			sort.Slice(tbl.Families, func(i, j int) bool {
				return tbl.Families[i].ID < tbl.Families[j].ID
			})
			if tbl.NextFamilyID <= op.FamilyID {
				tbl.NextFamilyID = op.FamilyID + 1
			}
		}
	}

	// TODO(ajwerner): deal with ordering the indexes or sanity checking this
	// or what-not.
	if op.ColumnID >= tbl.NextColumnID {
		tbl.NextColumnID = op.ColumnID + 1
	}

	return enqueueAddColumnMutation(tbl, &descpb.ColumnDescriptor{
		ID:                                op.ColumnID,
		Name:                              name,
		Type:                              op.ColumnType,
		Nullable:                          op.Nullable,
		DefaultExpr:                       emptyStrToNil(op.DefaultExpr),
		OnUpdateExpr:                      emptyStrToNil(op.OnUpdateExpr),
		Hidden:                            op.Hidden,
		Inaccessible:                      op.Inaccessible,
		GeneratedAsIdentityType:           op.GeneratedAsIdentityType,
		GeneratedAsIdentitySequenceOption: emptyStrToNil(op.GeneratedAsIdentitySequenceOption),
		UsesSequenceIds:                   op.UsesSequenceIDs,
		ComputeExpr:                       emptyStrToNil(op.ComputerExpr),
		PGAttributeNum:                    op.PgAttributeNum,
		SystemColumnKind:                  op.SystemColumnKind,
		Virtual:                           op.Virtual,
	})
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
	return errors.AssertionFailedf("failed to find column %d in %v", op.ColumnID, tbl)
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

func (m *visitor) MakeColumnAbsent(ctx context.Context, op scop.MakeColumnAbsent) error {
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
	tbl.AddFamily(op.Family)
	if op.Family.ID >= tbl.NextFamilyID {
		tbl.NextFamilyID = op.Family.ID + 1
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

func (m *visitor) RemoveColumnDefaultExpression(
	ctx context.Context, op scop.RemoveColumnDefaultExpression,
) error {
	// Remove the descriptors namespaces as the last stage
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	column, err := tbl.FindColumnWithID(op.ColumnID)
	if err != nil {
		return err
	}

	// Clean up the default expression and the sequence ID's
	column.ColumnDesc().DefaultExpr = nil
	column.ColumnDesc().UsesSequenceIds = nil
	return nil
}
