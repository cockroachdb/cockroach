package schemachanger

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/errors"
)

type op interface {
	apply(ctx context.Context, state *SchemaChanger)
}

type step interface {
	op
	numOps() int
	getOp(i int) op
}

type descriptorMutationStep interface {
	step
	run(ctx context.Context, mutation depsForDescriptorMutation) error
}

type validationStep interface {
	step
	validationOp
}

type validationOps []validationOp

func (d validationOps) numOps() int {
	return len(d)
}

func (d validationOps) getOp(i int) op {
	return d[i]
}

func (d validationOps) run(ctx context.Context, descriptors depsForValidation) error {
	panic("unimplemented")
}

func (d validationOps) apply(ctx context.Context, state *SchemaChanger) {
	for i := range d {
		d[i].apply(ctx, state)
	}
}

var _ step = (validationOps)(nil)

type validationOp interface {
	op
	run(ctx context.Context, deps depsForValidation) error
}

type depsForValidation interface{}

type backfillStep interface {
	step
	backfillOp
}

type backfillOps []backfillOp

func (d backfillOps) numOps() int {
	return len(d)
}

func (d backfillOps) getOp(i int) op {
	return d[i]
}

func (d backfillOps) run(ctx context.Context, descriptors depsForBackfill) error {
	panic("unimplemented")
}

func (d backfillOps) apply(ctx context.Context, state *SchemaChanger) {
	for i := range d {
		d[i].apply(ctx, state)
	}
}

var _ step = (validationOps)(nil)

type backfillOp interface {
	op
	run(ctx context.Context, deps depsForBackfill) error
}

type depsForBackfill interface {
	// IndexBackfiller thing.
	// Checkpointing and reading checkpoints.
	catalog.DescGetter
}

type descriptorMutationOps []descriptorMutationOp

func (d descriptorMutationOps) numOps() int {
	return len(d)
}

func (d descriptorMutationOps) getOp(i int) op {
	return d[i]
}

func (d descriptorMutationOps) run(
	ctx context.Context, descriptors depsForDescriptorMutation,
) error {
	for i := range d {
		if err := d[i].run(ctx, descriptors); err != nil {
			return err
		}
	}
	return nil
}

func (d descriptorMutationOps) apply(ctx context.Context, state *SchemaChanger) {
	for i := range d {
		d[i].apply(ctx, state)
	}
}

var _ step = (descriptorMutationOps)(nil)

type descriptorMutationOp interface {
	run(ctx context.Context, mutation depsForDescriptorMutation /* dependencies for a descriptor mutation */) error
	apply(ctx context.Context, state *SchemaChanger)
}

type addColumnChangeStateOp struct {
	tableID   descpb.ID
	columnID  descpb.ColumnID
	nextState elemState
}

type depsForDescriptorMutation interface {
	catalog.DescGetter
	WriteDesc(ctx context.Context, desc catalog.MutableDescriptor) error
}

func (a addColumnChangeStateOp) run(ctx context.Context, deps depsForDescriptorMutation) error {
	// Get descriptor from the collection.
	// Find the column.
	// Validate the state transition.
	// Update the state.
	// Write back the descriptor.
	desc, err := deps.GetDesc(ctx, a.tableID)
	if err != nil {
		return err
	}
	table, ok := desc.(*tabledesc.Mutable)
	if !ok {
		return catalog.ErrDescriptorNotFound
	}
	var mutation *descpb.DescriptorMutation
	mutationIdx := -1
	for i := range table.Mutations {
		m := &table.Mutations[i]
		if col := m.GetColumn(); col != nil && col.ID == a.columnID {
			mutation = m
			mutationIdx = i
			break
		}
	}
	if mutation == nil {
		return errors.AssertionFailedf("mutation for column %d not found", a.columnID)
	}
	// TODO (lucy): validate the current column descriptor state.
	switch a.nextState {
	case elemDeleteOnly:
		mutation.State = descpb.DescriptorMutation_DELETE_ONLY
	case elemDeleteAndWriteOnly:
		mutation.State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
	case elemPublic:
		table.Columns = append(table.Columns, *mutation.GetColumn())
		table.Mutations = append(table.Mutations[:mutationIdx], table.Mutations[mutationIdx+1:]...)
	case elemRemoved:
		table.Mutations = append(table.Mutations[:mutationIdx], table.Mutations[mutationIdx+1:]...)
	}
	return deps.WriteDesc(ctx, table)
}

func (a addColumnChangeStateOp) apply(ctx context.Context, sc *SchemaChanger) {
	for _, e := range sc.state.elements {
		if addCol, ok := e.(*addColumn); ok && addCol.tableID == a.tableID && addCol.columnID == a.columnID {
			addCol.state = a.nextState
			break
		}
	}
}

var _ descriptorMutationOp = (*addColumnChangeStateOp)(nil)

type indexBackfillOp struct {
	tableID descpb.ID
	indexID descpb.IndexID
}

func (i indexBackfillOp) apply(ctx context.Context, state *SchemaChanger) {
	panic("implement me")
}

func (i indexBackfillOp) run(ctx context.Context, deps depsForBackfill) error {
	panic("implement me")
}

var _ backfillOp = (*indexBackfillOp)(nil)

type columnBackfillOp struct {
	tableID descpb.ID

	storedColumnsToAdd    []descpb.ColumnID
	storedColumnsToRemove []descpb.ColumnID
}

var _ backfillOp = (*columnBackfillOp)(nil)

func (c columnBackfillOp) apply(ctx context.Context, state *SchemaChanger) {
	// panic("implement me")
}

func (c columnBackfillOp) run(ctx context.Context, deps depsForBackfill) error {
	// panic("implement me")
	return nil
}

type uniqueIndexValidationOp struct {
}
