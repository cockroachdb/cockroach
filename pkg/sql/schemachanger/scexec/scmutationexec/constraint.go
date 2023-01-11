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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/errors"
)

func (m *visitor) SetConstraintName(ctx context.Context, op scop.SetConstraintName) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	constraint, err := tbl.FindConstraintWithID(op.ConstraintID)
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
		constraint.AsForeignKey().ForeignKeyDesc().Name = op.Name
	} else {
		return errors.AssertionFailedf("unknown constraint type")
	}
	return nil
}

func (m *visitor) MakeAbsentCheckConstraintWriteOnly(
	ctx context.Context, op scop.MakeAbsentCheckConstraintWriteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	if op.ConstraintID >= tbl.NextConstraintID {
		tbl.NextConstraintID = op.ConstraintID + 1
	}

	// We should have already validated that the check constraint
	// is syntactically valid in the builder, so we just need to
	// enqueue it to the descriptor's mutation slice.
	ck := &descpb.TableDescriptor_CheckConstraint{
		Expr:                  string(op.Expr),
		Name:                  tabledesc.ConstraintNamePlaceholder(op.ConstraintID),
		Validity:              descpb.ConstraintValidity_Validating,
		ColumnIDs:             op.ColumnIDs,
		FromHashShardedColumn: op.FromHashShardedColumn,
		ConstraintID:          op.ConstraintID,
		IsNonNullConstraint:   op.IsNotNull,
	}
	if op.IsNotNull {
		if err = enqueueAddNotNullMutation(tbl, ck); err != nil {
			return err
		}
	} else {
		if err = enqueueAddCheckConstraintMutation(tbl, ck); err != nil {
			return err
		}
	}
	// Fast-forward the mutation state to WRITE_ONLY because this constraint
	// is now considered as enforced.
	tbl.Mutations[len(tbl.Mutations)-1].State = descpb.DescriptorMutation_WRITE_ONLY
	return nil
}

func (m *visitor) MakeValidatedCheckConstraintPublic(
	ctx context.Context, op scop.MakeValidatedCheckConstraintPublic,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}

	var found bool
	for idx, mutation := range tbl.Mutations {
		if c := mutation.GetConstraint(); c != nil &&
			(c.ConstraintType == descpb.ConstraintToUpdate_CHECK || c.ConstraintType == descpb.ConstraintToUpdate_NOT_NULL) &&
			c.Check.ConstraintID == op.ConstraintID {
			tbl.Checks = append(tbl.Checks, &c.Check)

			// Remove the mutation from the mutation slice. The `MakeMutationComplete`
			// call will also mark the above added check as VALIDATED.
			// If this is a rollback of a drop, we are trying to add the check constraint
			// back, so swap the direction before making it complete.
			mutation.Direction = descpb.DescriptorMutation_ADD
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
		return errors.AssertionFailedf("failed to find check constraint %d in table %q (%d)",
			op.ConstraintID, tbl.GetName(), tbl.GetID())
	}

	if len(tbl.Mutations) == 0 {
		tbl.Mutations = nil
	}

	return nil
}

func (m *visitor) MakePublicCheckConstraintValidated(
	ctx context.Context, op scop.MakePublicCheckConstraintValidated,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	for _, ck := range tbl.Checks {
		if ck.ConstraintID == op.ConstraintID {
			ck.Validity = descpb.ConstraintValidity_Dropping
			return enqueueDropCheckConstraintMutation(tbl, ck)
		}
	}

	return errors.AssertionFailedf("failed to find check constraint %d in descriptor %v", op.ConstraintID, tbl)
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
			(c.ConstraintType == descpb.ConstraintToUpdate_CHECK || c.ConstraintType == descpb.ConstraintToUpdate_NOT_NULL) &&
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

func (m *visitor) RemoveForeignKeyConstraint(
	ctx context.Context, op scop.RemoveForeignKeyConstraint,
) error {
	out, err := m.checkOutTable(ctx, op.TableID)
	if err != nil || out.Dropped() {
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

func (m *visitor) RemoveUniqueWithoutIndexConstraint(
	ctx context.Context, op scop.RemoveUniqueWithoutIndexConstraint,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
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

func (m *visitor) MakeAbsentForeignKeyConstraintWriteOnly(
	ctx context.Context, op scop.MakeAbsentForeignKeyConstraintWriteOnly,
) error {
	out, err := m.checkOutTable(ctx, op.TableID)
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
		Validity:            descpb.ConstraintValidity_Validating,
		OnDelete:            op.OnDeleteAction,
		OnUpdate:            op.OnUpdateAction,
		Match:               op.CompositeKeyMatchMethod,
		ConstraintID:        op.ConstraintID,
	}
	if err = enqueueAddForeignKeyConstraintMutation(out, fk); err != nil {
		return err
	}
	out.Mutations[len(out.Mutations)-1].State = descpb.DescriptorMutation_WRITE_ONLY
	return nil
}

func (m *visitor) MakeValidatedForeignKeyConstraintPublic(
	ctx context.Context, op scop.MakeValidatedForeignKeyConstraintPublic,
) error {
	out, err := m.checkOutTable(ctx, op.TableID)
	if err != nil || out.Dropped() {
		return err
	}
	in, err := m.checkOutTable(ctx, op.ReferencedTableID)
	if err != nil || in.Dropped() {
		return err
	}

	var found bool
	for idx, mutation := range out.Mutations {
		if c := mutation.GetConstraint(); c != nil &&
			c.ConstraintType == descpb.ConstraintToUpdate_FOREIGN_KEY &&
			c.ForeignKey.ConstraintID == op.ConstraintID {
			out.OutboundFKs = append(out.OutboundFKs, c.ForeignKey)
			in.InboundFKs = append(in.InboundFKs, c.ForeignKey)

			// Remove the mutation from the mutation slice. The `MakeMutationComplete`
			// call will also mark the above added check as VALIDATED.
			// If this is a rollback of a drop, we are trying to add the foreign key constraint
			// back, so swap the direction before making it complete.
			mutation.Direction = descpb.DescriptorMutation_ADD
			err = out.MakeMutationComplete(mutation)
			if err != nil {
				return err
			}
			out.Mutations = append(out.Mutations[:idx], out.Mutations[idx+1:]...)

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

func (m *visitor) MakePublicForeignKeyConstraintValidated(
	ctx context.Context, op scop.MakePublicForeignKeyConstraintValidated,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	for i, fk := range tbl.OutboundFKs {
		if fk.ConstraintID == op.ConstraintID {
			tbl.OutboundFKs = append(tbl.OutboundFKs[:i], tbl.OutboundFKs[i+1:]...)
			if len(tbl.OutboundFKs) == 0 {
				tbl.OutboundFKs = nil
			}
			fk.Validity = descpb.ConstraintValidity_Dropping
			return enqueueDropForeignKeyConstraintMutation(tbl, &fk)
		}
	}

	return errors.AssertionFailedf("failed to find FK constraint %d in descriptor %v", op.ConstraintID, tbl)
}

func (m *visitor) MakeAbsentUniqueWithoutIndexConstraintWriteOnly(
	ctx context.Context, op scop.MakeAbsentUniqueWithoutIndexConstraintWriteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
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
		Validity:     descpb.ConstraintValidity_Validating,
		ConstraintID: op.ConstraintID,
	}
	if op.Predicate != nil {
		uwi.Predicate = string(op.Predicate.Expr)
	}
	if err = enqueueAddUniqueWithoutIndexConstraintMutation(tbl, uwi); err != nil {
		return err
	}
	// Fast-forward the mutation state to WRITE_ONLY because this constraint
	// is now considered as enforced.
	tbl.Mutations[len(tbl.Mutations)-1].State = descpb.DescriptorMutation_WRITE_ONLY
	return nil
}

func (m *visitor) MakeValidatedUniqueWithoutIndexConstraintPublic(
	ctx context.Context, op scop.MakeValidatedUniqueWithoutIndexConstraintPublic,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
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

func (m *visitor) MakePublicUniqueWithoutIndexConstraintValidated(
	ctx context.Context, op scop.MakePublicUniqueWithoutIndexConstraintValidated,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	for i, uwi := range tbl.UniqueWithoutIndexConstraints {
		if uwi.ConstraintID == op.ConstraintID {
			tbl.UniqueWithoutIndexConstraints = append(tbl.UniqueWithoutIndexConstraints[:i], tbl.UniqueWithoutIndexConstraints[i+1:]...)
			if len(tbl.UniqueWithoutIndexConstraints) == 0 {
				tbl.UniqueWithoutIndexConstraints = nil
			}
			uwi.Validity = descpb.ConstraintValidity_Dropping
			return enqueueDropUniqueWithoutIndexConstraintMutation(tbl, &uwi)
		}
	}

	return errors.AssertionFailedf("failed to find unique_without_index constraint %d in descriptor %v", op.ConstraintID, tbl)
}
