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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/errors"
)

func (m *visitor) checkOutTable(ctx context.Context, id descpb.ID) (*tabledesc.Mutable, error) {
	desc, err := m.s.CheckOutDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	mut, ok := desc.(*tabledesc.Mutable)
	if !ok {
		return nil, catalog.WrapTableDescRefErr(id, catalog.NewDescriptorTypeError(desc))
	}
	return mut, nil
}

func (m *visitor) checkOutDatabase(ctx context.Context, id descpb.ID) (*dbdesc.Mutable, error) {
	desc, err := m.s.CheckOutDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	mut, ok := desc.(*dbdesc.Mutable)
	if !ok {
		return nil, catalog.WrapDatabaseDescRefErr(id, catalog.NewDescriptorTypeError(desc))
	}
	return mut, nil
}

func (m *visitor) checkOutSchema(ctx context.Context, id descpb.ID) (*schemadesc.Mutable, error) {
	desc, err := m.s.CheckOutDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	mut, ok := desc.(*schemadesc.Mutable)
	if !ok {
		return nil, catalog.WrapSchemaDescRefErr(id, catalog.NewDescriptorTypeError(desc))
	}
	return mut, nil
}

// Stop the linter from complaining.
var _ = ((*visitor)(nil)).checkOutSchema

func (m *visitor) checkOutType(ctx context.Context, id descpb.ID) (*typedesc.Mutable, error) {
	desc, err := m.s.CheckOutDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	mut, ok := desc.(*typedesc.Mutable)
	if !ok {
		return nil, catalog.WrapTypeDescRefErr(id, catalog.NewDescriptorTypeError(desc))
	}
	return mut, nil
}

func mutationStateChange(
	tbl *tabledesc.Mutable,
	f MutationSelector,
	exp, next descpb.DescriptorMutation_State,
	direction descpb.DescriptorMutation_Direction,
) error {
	mut, err := FindMutation(tbl, f)
	if err != nil {
		return err
	}
	m := &tbl.TableDesc().Mutations[mut.MutationOrdinal()]
	if m.State != exp {
		return errors.AssertionFailedf("update mutation for %d from %v to %v: unexpected state: %v",
			tbl.GetID(), exp, m.State, tbl)
	}
	m.State = next
	m.Direction = direction
	return nil
}

func RemoveMutation(
	tbl *tabledesc.Mutable, f MutationSelector, exp ...descpb.DescriptorMutation_State,
) (descpb.DescriptorMutation, error) {
	mut, err := FindMutation(tbl, f)
	if err != nil {
		return descpb.DescriptorMutation{}, err
	}
	foundIdx := mut.MutationOrdinal()
	cpy := tbl.Mutations[foundIdx]
	var foundExpState bool
	for _, s := range exp {
		if cpy.State == s {
			foundExpState = true
			break
		}
	}
	if !foundExpState {
		return descpb.DescriptorMutation{}, errors.AssertionFailedf(
			"remove mutation from %d: unexpected state: got %v, expected %v: %v",
			tbl.GetID(), cpy.State, exp, tbl,
		)
	}
	tbl.Mutations = append(tbl.Mutations[:foundIdx], tbl.Mutations[foundIdx+1:]...)
	return cpy, nil
}

// MutationSelector defines a predicate on a catalog.Mutation with no
// side-effects.
type MutationSelector func(mutation catalog.Mutation) (matches bool)

// FindMutation returns the first mutation in table for which the selector
// returns true.
// Such a mutation is expected to exist, if none are found, an internal error
// is returned.
func FindMutation(
	tbl catalog.TableDescriptor, selector MutationSelector,
) (catalog.Mutation, error) {
	for _, mut := range tbl.AllMutations() {
		if selector(mut) {
			return mut, nil
		}
	}
	return nil, errors.AssertionFailedf("matching mutation not found in table %d", tbl.GetID())
}

// MakeIndexIDMutationSelector returns a MutationSelector which matches an
// index mutation with the correct ID.
func MakeIndexIDMutationSelector(indexID descpb.IndexID) MutationSelector {
	return func(mut catalog.Mutation) bool {
		if mut.AsIndex() == nil {
			return false
		}
		return mut.AsIndex().GetID() == indexID
	}
}

// MakeColumnIDMutationSelector returns a MutationSelector which matches a
// column mutation with the correct ID.
func MakeColumnIDMutationSelector(columnID descpb.ColumnID) MutationSelector {
	return func(mut catalog.Mutation) bool {
		if mut.AsColumn() == nil {
			return false
		}
		return mut.AsColumn().GetID() == columnID
	}
}

// MakeMutationIDMutationSelector returns a MutationSelector which matches the
// first mutation with this ID.
func MakeMutationIDMutationSelector(mutationID descpb.MutationID) MutationSelector {
	return func(mut catalog.Mutation) bool {
		return mut.MutationID() == mutationID
	}
}

func enqueueAddColumnMutation(tbl *tabledesc.Mutable, col *descpb.ColumnDescriptor) error {
	tbl.AddColumnMutation(col, descpb.DescriptorMutation_ADD)
	tbl.NextMutationID--
	return nil
}

func enqueueDropColumnMutation(tbl *tabledesc.Mutable, col *descpb.ColumnDescriptor) error {
	tbl.AddColumnMutation(col, descpb.DescriptorMutation_DROP)
	tbl.NextMutationID--
	return nil
}

func enqueueAddCheckConstraintMutation(
	tbl *tabledesc.Mutable, ck *descpb.TableDescriptor_CheckConstraint,
) error {
	tbl.AddCheckMutation(ck, descpb.DescriptorMutation_ADD)
	tbl.NextMutationID--
	return nil
}

func enqueueAddUniqueWithoutIndexConstraintMutation(
	tbl *tabledesc.Mutable, uwi *descpb.UniqueWithoutIndexConstraint,
) error {
	tbl.AddUniqueWithoutIndexMutation(uwi, descpb.DescriptorMutation_ADD)
	tbl.NextMutationID--
	return nil
}

func enqueueDropCheckConstraintMutation(
	tbl *tabledesc.Mutable, ck *descpb.TableDescriptor_CheckConstraint,
) error {
	tbl.AddCheckMutation(ck, descpb.DescriptorMutation_DROP)
	tbl.NextMutationID--
	return nil
}

func enqueueDropUniqueWithoutIndexConstraintMutation(
	tbl *tabledesc.Mutable, uwi *descpb.UniqueWithoutIndexConstraint,
) error {
	tbl.AddUniqueWithoutIndexMutation(uwi, descpb.DescriptorMutation_DROP)
	tbl.NextMutationID--
	return nil
}

func enqueueAddForeignKeyConstraintMutation(
	tbl *tabledesc.Mutable, fk *descpb.ForeignKeyConstraint,
) error {
	tbl.AddForeignKeyMutation(fk, descpb.DescriptorMutation_ADD)
	tbl.NextMutationID--
	return nil
}

func enqueueDropForeignKeyConstraintMutation(
	tbl *tabledesc.Mutable, fk *descpb.ForeignKeyConstraint,
) error {
	tbl.AddForeignKeyMutation(fk, descpb.DescriptorMutation_DROP)
	tbl.NextMutationID--
	return nil
}

func enqueueAddIndexMutation(
	tbl *tabledesc.Mutable, idx *descpb.IndexDescriptor, state descpb.DescriptorMutation_State,
) error {
	if err := tbl.AddIndexMutation(
		idx, descpb.DescriptorMutation_ADD, state,
	); err != nil {
		return err
	}
	tbl.NextMutationID--
	return nil
}

func enqueueDropIndexMutation(tbl *tabledesc.Mutable, idx *descpb.IndexDescriptor) error {
	if err := tbl.AddIndexMutation(
		idx, descpb.DescriptorMutation_DROP, descpb.DescriptorMutation_WRITE_ONLY,
	); err != nil {
		return err
	}
	tbl.NextMutationID--
	return nil
}

func updateColumnExprSequenceUsage(d *descpb.ColumnDescriptor) error {
	var all catalog.DescriptorIDSet
	for _, expr := range [3]*string{d.ComputeExpr, d.DefaultExpr, d.OnUpdateExpr} {
		if expr == nil {
			continue
		}
		ids, err := sequenceIDsInExpr(*expr)
		if err != nil {
			return err
		}
		ids.ForEach(all.Add)
	}
	d.UsesSequenceIds = all.Ordered()
	return nil
}

func sequenceIDsInExpr(expr string) (ids catalog.DescriptorIDSet, _ error) {
	e, err := parser.ParseExpr(expr)
	if err != nil {
		return ids, err
	}
	seqIdents, err := seqexpr.GetUsedSequences(e)
	if err != nil {
		return ids, err
	}
	for _, si := range seqIdents {
		ids.Add(descpb.ID(si.SeqID))
	}
	return ids, nil
}
