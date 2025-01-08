// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/errors"
)

func (i *immediateVisitor) getDescriptor(
	ctx context.Context, id descpb.ID,
) (catalog.Descriptor, error) {
	if checkedOut := i.MaybeGetCheckedOutDescriptor(id); checkedOut != nil {
		return checkedOut, nil
	}
	read, err := i.descriptorReader.MustReadImmutableDescriptors(ctx, id)
	if err != nil {
		return nil, err
	}
	return read[0], nil
}

func (i *immediateVisitor) checkOutDescriptor(
	ctx context.Context, id descpb.ID,
) (catalog.MutableDescriptor, error) {
	if checkedOut := i.MaybeGetCheckedOutDescriptor(id); checkedOut != nil {
		return checkedOut, nil
	}
	mut, err := i.descriptorReader.MustReadMutableDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	i.AddToCheckedOutDescriptors(mut)
	return mut, nil
}

func (i *immediateVisitor) checkOutTable(
	ctx context.Context, id descpb.ID,
) (*tabledesc.Mutable, error) {
	desc, err := i.checkOutDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	mut, ok := desc.(*tabledesc.Mutable)
	if !ok {
		return nil, catalog.WrapTableDescRefErr(id, catalog.NewDescriptorTypeError(desc))
	}
	return mut, nil
}

func (i *immediateVisitor) checkOutDatabase(
	ctx context.Context, id descpb.ID,
) (*dbdesc.Mutable, error) {
	desc, err := i.checkOutDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	mut, ok := desc.(*dbdesc.Mutable)
	if !ok {
		return nil, catalog.WrapDatabaseDescRefErr(id, catalog.NewDescriptorTypeError(desc))
	}
	return mut, nil
}

func (i *immediateVisitor) checkOutSchema(
	ctx context.Context, id descpb.ID,
) (*schemadesc.Mutable, error) {
	desc, err := i.checkOutDescriptor(ctx, id)
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
var _ = ((*immediateVisitor)(nil)).checkOutSchema

func (i *immediateVisitor) checkOutType(
	ctx context.Context, id descpb.ID,
) (*typedesc.Mutable, error) {
	desc, err := i.checkOutDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	mut, ok := desc.(*typedesc.Mutable)
	if !ok {
		return nil, catalog.WrapTypeDescRefErr(id, catalog.NewDescriptorTypeError(desc))
	}
	return mut, nil
}

func (i *immediateVisitor) checkOutFunction(
	ctx context.Context, id descpb.ID,
) (*funcdesc.Mutable, error) {
	desc, err := i.checkOutDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	mut, ok := desc.(*funcdesc.Mutable)
	if !ok {
		return nil, catalog.WrapTypeDescRefErr(id, catalog.NewDescriptorTypeError(desc))
	}
	return mut, nil
}

func (i *immediateVisitor) checkOutTrigger(
	ctx context.Context, tableID descpb.ID, triggerID catid.TriggerID,
) (*descpb.TriggerDescriptor, error) {
	tbl, err := i.checkOutTable(ctx, tableID)
	if err != nil {
		return nil, err
	}
	trigger := catalog.FindTriggerByID(tbl, triggerID)
	if trigger != nil {
		return trigger, nil
	}
	panic(errors.AssertionFailedf("failed to find trigger with ID %d in table %q (%d)",
		triggerID, tbl.GetName(), tbl.GetID()))
}

func (i *immediateVisitor) checkOutPolicy(
	ctx context.Context, tableID descpb.ID, policyID catid.PolicyID,
) (*descpb.PolicyDescriptor, error) {
	tbl, err := i.checkOutTable(ctx, tableID)
	if err != nil {
		return nil, err
	}
	policy := catalog.FindPolicyByID(tbl, policyID)
	if policy != nil {
		return policy, nil
	}
	panic(errors.AssertionFailedf("failed to find policy with ID %d in table %q (%d)",
		policyID, tbl.GetName(), tbl.GetID()))
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

// enqueueNonIndexMutation enqueues a non-index mutation `m` (of generic type M)
// with direction `dir` without increasing the next mutation ID.
// The mutation state will be DELETE_ONLY if `dir=ADD` and WRITE_ONLY if `dir=DROP`.
func enqueueNonIndexMutation[M any](
	tbl *tabledesc.Mutable,
	enqueueFunc func(M, descpb.DescriptorMutation_Direction),
	m M,
	dir descpb.DescriptorMutation_Direction,
) {
	enqueueFunc(m, dir)
	tbl.NextMutationID--
}

// enqueueIndexMutation is like enqueueNonIndexMutation but allows caller to
// specify the mutation's initial state.
func enqueueIndexMutation(
	tbl *tabledesc.Mutable,
	idx *descpb.IndexDescriptor,
	state descpb.DescriptorMutation_State,
	dir descpb.DescriptorMutation_Direction,
) error {
	if err := tbl.AddIndexMutation(idx, dir, state); err != nil {
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
	d.OwnsSequenceIds = all.Ordered()
	return nil
}

func updateColumnExprFunctionsUsage(d *descpb.ColumnDescriptor) error {
	var all catalog.DescriptorIDSet
	for _, expr := range [3]*string{d.ComputeExpr, d.DefaultExpr, d.OnUpdateExpr} {
		if expr == nil {
			continue
		}
		ids, err := schemaexpr.GetUDFIDsFromExprStr(*expr)
		if err != nil {
			return err
		}
		ids.ForEach(all.Add)
	}
	d.UsesFunctionIds = all.Ordered()
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
