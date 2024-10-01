// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/errors"
)

func (i *immediateVisitor) SetConstraintName(ctx context.Context, op scop.SetConstraintName) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	constraint, err := catalog.MustFindConstraintByID(tbl, op.ConstraintID)
	if err != nil {
		return err
	}
	if constraint.AsUniqueWithIndex() != nil {
		constraint.AsUniqueWithIndex().IndexDesc().Name = op.Name
	} else if constraint.AsUniqueWithoutIndex() != nil {
		constraint.AsUniqueWithoutIndex().UniqueWithoutIndexDesc().Name = op.Name
	} else if constraint.AsCheck() != nil {
		constraint.AsCheck().CheckDesc().Name = op.Name
	} else if constraint.AsForeignKey() != nil {
		oldName := constraint.AsForeignKey().ForeignKeyDesc().Name
		constraint.AsForeignKey().ForeignKeyDesc().Name = op.Name
		// Also attempt to set the FK constraint name in the referenced table.
		// This is needed on the dropping path (i.e. when dropping an existing
		// FK constraint).
		referencedTable, err := i.checkOutTable(ctx, constraint.AsForeignKey().GetReferencedTableID())
		if err != nil || referencedTable.Dropped() {
			return err
		}
		for _, inboundFK := range referencedTable.InboundForeignKeys() {
			// Unfortunately, because of a post-deserialization bug the IDs
			// between the origin and reference table may not properly sync in
			// restored descriptors. So, sadly we need to match by name when
			// updating names on the inbound side (TestRestoreOldVersions exposes this
			// scenario)
			if inboundFK.GetOriginTableID() == op.TableID && inboundFK.GetName() == oldName {
				inboundFK.ForeignKeyDesc().Name = op.Name
				break
			}
		}
	} else {
		return errors.AssertionFailedf("unknown constraint type")
	}
	return nil
}

func (i *immediateVisitor) AddCheckConstraint(
	ctx context.Context, op scop.AddCheckConstraint,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	if op.ConstraintID >= tbl.NextConstraintID {
		tbl.NextConstraintID = op.ConstraintID + 1
	}

	ck := &descpb.TableDescriptor_CheckConstraint{
		Expr:                  string(op.CheckExpr),
		Name:                  tabledesc.ConstraintNamePlaceholder(op.ConstraintID),
		Validity:              op.Validity,
		ColumnIDs:             op.ColumnIDs,
		FromHashShardedColumn: op.FromHashShardedColumn,
		ConstraintID:          op.ConstraintID,
	}
	if op.Validity == descpb.ConstraintValidity_Validating {
		// A validating constraint needs to transition through an intermediate state
		// so we enqueue a mutation for it and fast-forward it to WRITE_ONLY state.
		enqueueNonIndexMutation(tbl, tbl.AddCheckMutation, ck, descpb.DescriptorMutation_ADD)
		tbl.Mutations[len(tbl.Mutations)-1].State = descpb.DescriptorMutation_WRITE_ONLY
	}
	tbl.Checks = append(tbl.Checks, ck)
	return nil
}

func (i *immediateVisitor) MakeAbsentColumnNotNullWriteOnly(
	ctx context.Context, op scop.MakeAbsentColumnNotNullWriteOnly,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}

	col := catalog.FindColumnByID(tbl, op.ColumnID)
	if col == nil {
		return errors.AssertionFailedf("column-id \"%d\" does not exist", op.ColumnID)
	}

	ck := tabledesc.MakeNotNullCheckConstraint(tbl, col,
		descpb.ConstraintValidity_Validating, 0 /* constraintID */)
	enqueueNonIndexMutation(tbl, tbl.AddNotNullMutation, ck, descpb.DescriptorMutation_ADD)
	// Fast-forward the mutation state to WRITE_ONLY because this constraint
	// is now considered as enforced.
	tbl.Mutations[len(tbl.Mutations)-1].State = descpb.DescriptorMutation_WRITE_ONLY
	tbl.Checks = append(tbl.Checks, ck)
	return nil
}

func (i *immediateVisitor) MakeValidatedCheckConstraintPublic(
	ctx context.Context, op scop.MakeValidatedCheckConstraintPublic,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}

	var found bool
	for idx, mutation := range tbl.Mutations {
		if c := mutation.GetConstraint(); c != nil &&
			c.ConstraintType == descpb.ConstraintToUpdate_CHECK &&
			c.Check.ConstraintID == op.ConstraintID {
			// Remove the mutation from the mutation slice. The `MakeMutationComplete`
			// call will mark the check in the public "Checks" slice as VALIDATED.
			err = tbl.MakeMutationComplete(mutation)
			if err != nil {
				return err
			}
			tbl.Mutations = append(tbl.Mutations[:idx], tbl.Mutations[idx+1:]...)

			found = true
			break
		}
	}

	if !found {
		return errors.AssertionFailedf("failed to find mutation for check constraint %d in table %q (%d)",
			op.ConstraintID, tbl.GetName(), tbl.GetID())
	}

	if len(tbl.Mutations) == 0 {
		tbl.Mutations = nil
	}

	return nil
}

func (i *immediateVisitor) MakeValidatedColumnNotNullPublic(
	ctx context.Context, op scop.MakeValidatedColumnNotNullPublic,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}

	var found bool
	for idx, mutation := range tbl.Mutations {
		if c := mutation.GetConstraint(); c != nil &&
			c.ConstraintType == descpb.ConstraintToUpdate_NOT_NULL &&
			c.NotNullColumn == op.ColumnID {
			col := catalog.FindColumnByID(tbl, op.ColumnID)
			if col == nil {
				return errors.AssertionFailedf("column-id \"%d\" does not exist", op.ColumnID)
			}
			col.ColumnDesc().Nullable = false
			tbl.Mutations = append(tbl.Mutations[:idx], tbl.Mutations[idx+1:]...)
			if len(tbl.Mutations) == 0 {
				tbl.Mutations = nil
			}
			found = true

			// Don't forget to also remove the dummy check in the "Checks" slice!
			for idx, ck := range tbl.Checks {
				if ck.IsNonNullConstraint && ck.ColumnIDs[0] == op.ColumnID {
					tbl.Checks = append(tbl.Checks[:idx], tbl.Checks[idx+1:]...)
					break
				}
			}

			break
		}
	}

	if !found {
		return errors.AssertionFailedf("failed to find NOT NULL mutation for column %d "+
			"in table %q (%d)", op.ColumnID, tbl.GetName(), tbl.GetID())
	}
	return nil
}

func (i *immediateVisitor) MakePublicCheckConstraintValidated(
	ctx context.Context, op scop.MakePublicCheckConstraintValidated,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	for _, ck := range tbl.Checks {
		if ck.ConstraintID == op.ConstraintID {
			ck.Validity = descpb.ConstraintValidity_Dropping
			enqueueNonIndexMutation(tbl, tbl.AddCheckMutation, ck, descpb.DescriptorMutation_DROP)
			return nil
		}
	}

	return errors.AssertionFailedf("failed to find check constraint %d in descriptor %v", op.ConstraintID, tbl)
}

func (i *immediateVisitor) MakePublicColumnNotNullValidated(
	ctx context.Context, op scop.MakePublicColumnNotNullValidated,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}

	for _, col := range tbl.AllColumns() {
		if col.GetID() == op.ColumnID {
			col.ColumnDesc().Nullable = true
			// Add a check constraint equivalent to the non-null constraint and drop
			// it in the schema changer.
			ck := tabledesc.MakeNotNullCheckConstraint(tbl, col,
				descpb.ConstraintValidity_Dropping, 0 /* constraintID */)
			tbl.Checks = append(tbl.Checks, ck)
			enqueueNonIndexMutation(tbl, tbl.AddNotNullMutation, ck, descpb.DescriptorMutation_DROP)
			return nil
		}
	}

	return errors.AssertionFailedf("failed to find column %d in descriptor %v", op.ColumnID, tbl)
}

func (i *immediateVisitor) RemoveCheckConstraint(
	ctx context.Context, op scop.RemoveCheckConstraint,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	var found bool
	for idx, c := range tbl.Checks {
		if c.ConstraintID == op.ConstraintID {
			tbl.Checks = append(tbl.Checks[:idx], tbl.Checks[idx+1:]...)
			found = true
			break
		}
	}
	for idx, m := range tbl.Mutations {
		if c := m.GetConstraint(); c != nil &&
			c.ConstraintType == descpb.ConstraintToUpdate_CHECK &&
			c.Check.ConstraintID == op.ConstraintID {
			tbl.Mutations = append(tbl.Mutations[:idx], tbl.Mutations[idx+1:]...)
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

func (i *immediateVisitor) RemoveColumnNotNull(
	ctx context.Context, op scop.RemoveColumnNotNull,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	var found bool
	for i, c := range tbl.Checks {
		if c.IsNonNullConstraint && c.ColumnIDs[0] == op.ColumnID {
			tbl.Checks = append(tbl.Checks[:i], tbl.Checks[i+1:]...)
			found = true
			break
		}
	}
	for i, m := range tbl.Mutations {
		if c := m.GetConstraint(); c != nil &&
			c.ConstraintType == descpb.ConstraintToUpdate_NOT_NULL &&
			c.NotNullColumn == op.ColumnID {
			tbl.Mutations = append(tbl.Mutations[:i], tbl.Mutations[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return errors.AssertionFailedf("failed to find NOT NULL for column %d in table %q (%d)",
			op.ColumnID, tbl.GetName(), tbl.GetID())
	}
	return nil
}

func (i *immediateVisitor) RemoveForeignKeyConstraint(
	ctx context.Context, op scop.RemoveForeignKeyConstraint,
) error {
	out, err := i.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	var found bool
	for i, fk := range out.OutboundFKs {
		if fk.ConstraintID == op.ConstraintID {
			out.OutboundFKs = append(out.OutboundFKs[:i], out.OutboundFKs[i+1:]...)
			if len(out.OutboundFKs) == 0 {
				out.OutboundFKs = nil
			}
			found = true
			break
		}
	}
	for i, m := range out.Mutations {
		if c := m.GetConstraint(); c != nil &&
			c.ConstraintType == descpb.ConstraintToUpdate_FOREIGN_KEY &&
			c.ForeignKey.ConstraintID == op.ConstraintID {
			out.Mutations = append(out.Mutations[:i], out.Mutations[i+1:]...)
			if len(out.Mutations) == 0 {
				out.Mutations = nil
			}
			found = true
			break
		}
	}
	if !found {
		return errors.AssertionFailedf("foreign key with ID %d not found in origin table %q (%d)",
			op.ConstraintID, out.GetName(), out.GetID())
	}
	return nil
}

func (i *immediateVisitor) RemoveUniqueWithoutIndexConstraint(
	ctx context.Context, op scop.RemoveUniqueWithoutIndexConstraint,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	var found bool
	for i, c := range tbl.UniqueWithoutIndexConstraints {
		if c.ConstraintID == op.ConstraintID {
			tbl.UniqueWithoutIndexConstraints = append(tbl.UniqueWithoutIndexConstraints[:i], tbl.UniqueWithoutIndexConstraints[i+1:]...)
			if len(tbl.UniqueWithoutIndexConstraints) == 0 {
				tbl.UniqueWithoutIndexConstraints = nil
			}
			found = true
			break
		}
	}
	for i, m := range tbl.Mutations {
		if c := m.GetConstraint(); c != nil &&
			c.ConstraintType == descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX &&
			c.UniqueWithoutIndexConstraint.ConstraintID == op.ConstraintID {
			tbl.Mutations = append(tbl.Mutations[:i], tbl.Mutations[i+1:]...)
			if len(tbl.Mutations) == 0 {
				tbl.Mutations = nil
			}
			found = true
			break
		}
	}
	if !found {
		return errors.AssertionFailedf("failed to find unnique_without_index constraint %d in table %q (%d)",
			op.ConstraintID, tbl.GetName(), tbl.GetID())
	}
	return nil
}

func (i *immediateVisitor) AddForeignKeyConstraint(
	ctx context.Context, op scop.AddForeignKeyConstraint,
) error {
	out, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || out.Dropped() {
		return err
	}
	if op.ConstraintID >= out.NextConstraintID {
		out.NextConstraintID = op.ConstraintID + 1
	}

	fk := &descpb.ForeignKeyConstraint{
		OriginTableID:       op.TableID,
		OriginColumnIDs:     op.ColumnIDs,
		ReferencedColumnIDs: op.ReferencedColumnIDs,
		ReferencedTableID:   op.ReferencedTableID,
		Name:                tabledesc.ConstraintNamePlaceholder(op.ConstraintID),
		Validity:            op.Validity,
		OnDelete:            op.OnDeleteAction,
		OnUpdate:            op.OnUpdateAction,
		Match:               op.CompositeKeyMatchMethod,
		ConstraintID:        op.ConstraintID,
	}
	if op.Validity == descpb.ConstraintValidity_Unvalidated {
		// Unvalidated constraint doesn't need to transition through an intermediate
		// state, so we don't enqueue a mutation for it.
		out.OutboundFKs = append(out.OutboundFKs, *fk)
	} else {
		enqueueNonIndexMutation(out, out.AddForeignKeyMutation, fk, descpb.DescriptorMutation_ADD)
		out.Mutations[len(out.Mutations)-1].State = descpb.DescriptorMutation_WRITE_ONLY
	}

	// Add an entry in "InboundFKs" in the referenced table as company.
	in, err := i.checkOutTable(ctx, op.ReferencedTableID)
	if err != nil {
		return err
	}
	in.InboundFKs = append(in.InboundFKs, *fk)
	return nil
}

func (i *immediateVisitor) MakeValidatedForeignKeyConstraintPublic(
	ctx context.Context, op scop.MakeValidatedForeignKeyConstraintPublic,
) error {
	out, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || out.Dropped() {
		return err
	}
	in, err := i.checkOutTable(ctx, op.ReferencedTableID)
	if err != nil || in.Dropped() {
		return err
	}

	var found bool
	for idx, mutation := range out.Mutations {
		if c := mutation.GetConstraint(); c != nil &&
			c.ConstraintType == descpb.ConstraintToUpdate_FOREIGN_KEY &&
			c.ForeignKey.ConstraintID == op.ConstraintID {
			// Complete this mutation by marking the validity as validated,
			// removing it from the mutations slice, and publishing it into
			// OutboundFKs slice.
			c.ForeignKey.Validity = descpb.ConstraintValidity_Validated
			out.OutboundFKs = append(out.OutboundFKs, c.ForeignKey)
			out.Mutations = append(out.Mutations[:idx], out.Mutations[idx+1:]...)

			// Update the back-reference in the referenced table.
			for i, inboundFK := range in.InboundFKs {
				if inboundFK.OriginTableID == out.GetID() && inboundFK.ConstraintID == op.ConstraintID {
					in.InboundFKs[i].Validity = descpb.ConstraintValidity_Validated
				}
			}

			found = true
			break
		}
	}

	if !found {
		return errors.AssertionFailedf("failed to find foreign key constraint %d in table %q (%d)",
			op.ConstraintID, out.GetName(), out.GetID())
	}

	if len(out.Mutations) == 0 {
		out.Mutations = nil
	}

	return nil
}

func (i *immediateVisitor) MakePublicForeignKeyConstraintValidated(
	ctx context.Context, op scop.MakePublicForeignKeyConstraintValidated,
) error {
	// A helper function to update the inbound FK in referenced table to DROPPING.
	// Note that the constraintID in the referencedTable might not match that in
	// in the referencing table, so we pass in the FK name as identifier.
	updateInboundFKAsDropping := func(referencedTableID descpb.ID, fkName string) error {
		foundInReferencedTable := false
		in, err := i.checkOutTable(ctx, referencedTableID)
		if err != nil {
			return err
		}
		for idx, inboundFk := range in.InboundFKs {
			if inboundFk.OriginTableID == op.TableID && inboundFk.Name == fkName {
				in.InboundFKs[idx].Validity = descpb.ConstraintValidity_Dropping
				foundInReferencedTable = true
				break
			}
		}
		if !foundInReferencedTable {
			return errors.AssertionFailedf("failed to find accompanying inbound FK (%v) in"+
				" referenced table %v (%v)", op.ConstraintID, in.Name, in.ID)
		}
		return nil
	}

	out, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || out.Dropped() {
		return err
	}
	for idx, fk := range out.OutboundFKs {
		if fk.ConstraintID == op.ConstraintID {
			out.OutboundFKs = append(out.OutboundFKs[:idx], out.OutboundFKs[idx+1:]...)
			if len(out.OutboundFKs) == 0 {
				out.OutboundFKs = nil
			}
			fk.Validity = descpb.ConstraintValidity_Dropping
			enqueueNonIndexMutation(out, out.AddForeignKeyMutation, &fk, descpb.DescriptorMutation_DROP)
			return updateInboundFKAsDropping(fk.ReferencedTableID, fk.Name)
		}
	}

	return errors.AssertionFailedf("failed to find FK constraint %d in descriptor %v", op.ConstraintID, out)
}

func (i *immediateVisitor) AddUniqueWithoutIndexConstraint(
	ctx context.Context, op scop.AddUniqueWithoutIndexConstraint,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	if op.ConstraintID >= tbl.NextConstraintID {
		tbl.NextConstraintID = op.ConstraintID + 1
	}

	uwi := &descpb.UniqueWithoutIndexConstraint{
		TableID:      op.TableID,
		ColumnIDs:    op.ColumnIDs,
		Name:         tabledesc.ConstraintNamePlaceholder(op.ConstraintID),
		Validity:     op.Validity,
		ConstraintID: op.ConstraintID,
		Predicate:    string(op.PartialExpr),
	}
	if op.Validity == descpb.ConstraintValidity_Unvalidated {
		// Unvalidated constraint doesn't need to transition through an intermediate
		// state, so we don't enqueue a mutation for it.
		tbl.UniqueWithoutIndexConstraints = append(tbl.UniqueWithoutIndexConstraints, *uwi)
		return nil
	}
	enqueueNonIndexMutation(tbl, tbl.AddUniqueWithoutIndexMutation, uwi, descpb.DescriptorMutation_ADD)
	tbl.Mutations[len(tbl.Mutations)-1].State = descpb.DescriptorMutation_WRITE_ONLY
	return nil
}

func (i *immediateVisitor) MakeValidatedUniqueWithoutIndexConstraintPublic(
	ctx context.Context, op scop.MakeValidatedUniqueWithoutIndexConstraintPublic,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}

	var found bool
	for idx, mutation := range tbl.Mutations {
		if c := mutation.GetConstraint(); c != nil &&
			c.ConstraintType == descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX &&
			c.UniqueWithoutIndexConstraint.ConstraintID == op.ConstraintID {
			tbl.UniqueWithoutIndexConstraints = append(tbl.UniqueWithoutIndexConstraints, c.UniqueWithoutIndexConstraint)

			// Remove the mutation from the mutation slice. The `MakeMutationComplete`
			// call will also mark the above added unique_without_index as VALIDATED.
			// If this is a rollback of a drop, we are trying to add the
			// unique_without_index constraint back, so swap the direction before
			// making it complete.
			mutation.Direction = descpb.DescriptorMutation_ADD
			err = tbl.MakeMutationComplete(mutation)
			if err != nil {
				return err
			}
			tbl.Mutations = append(tbl.Mutations[:idx], tbl.Mutations[idx+1:]...)
			if len(tbl.Mutations) == 0 {
				tbl.Mutations = nil
			}

			found = true
			break
		}
	}

	if !found {
		return errors.AssertionFailedf("failed to find unique_without_index constraint %d in table %q (%d)",
			op.ConstraintID, tbl.GetName(), tbl.GetID())
	}

	return nil
}

func (i *immediateVisitor) MakePublicUniqueWithoutIndexConstraintValidated(
	ctx context.Context, op scop.MakePublicUniqueWithoutIndexConstraintValidated,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	for i, uwi := range tbl.UniqueWithoutIndexConstraints {
		if uwi.ConstraintID == op.ConstraintID {
			tbl.UniqueWithoutIndexConstraints = append(tbl.UniqueWithoutIndexConstraints[:i], tbl.UniqueWithoutIndexConstraints[i+1:]...)
			if len(tbl.UniqueWithoutIndexConstraints) == 0 {
				tbl.UniqueWithoutIndexConstraints = nil
			}
			uwi.Validity = descpb.ConstraintValidity_Dropping
			enqueueNonIndexMutation(tbl, tbl.AddUniqueWithoutIndexMutation, &uwi, descpb.DescriptorMutation_DROP)
			return nil
		}
	}

	return errors.AssertionFailedf("failed to find unique_without_index constraint %d in descriptor %v", op.ConstraintID, tbl)
}
