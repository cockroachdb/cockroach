package schemachanger

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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

type validationOp interface {
	op
	run(ctx context.Context, deps depsForValidation) error
}

type depsForValidation interface{}

type backfillStep interface {
	step
	backfillOp
}

type backfillOp interface {
	op
	run(ctx context.Context, deps depsForBackfill) error
}

type depsForBackfill interface {
	// IndexBackfiller thing.
	// Checkpointing and reading checkpoints.
	catalog.DescGetter
}

type descriptorMutationsStep []descriptorMutationOp

func (d descriptorMutationsStep) numOps() int {
	panic("implement me")
}

func (d descriptorMutationsStep) getOp(i int) op {
	panic("implement me")
}

func (d descriptorMutationsStep) run(
	ctx context.Context, descriptors depsForDescriptorMutation,
) error {
	for i := range d {
		if err := d[i].run(ctx, descriptors); err != nil {
			return err
		}
	}
	return nil
}

func (d descriptorMutationsStep) apply(ctx context.Context, state *SchemaChanger) {
	for i := range d {
		d[i].apply(ctx, state)
	}
}

var _ step = (descriptorMutationsStep)(nil)

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
	panic("implement me")
}

func (a addColumnChangeStateOp) apply(ctx context.Context, state *SchemaChanger) {
	panic("implement me")
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
	panic("implement me")
}

func (c columnBackfillOp) run(ctx context.Context, deps depsForBackfill) error {
	panic("implement me")
}

type uniqueIndexValidationOp struct {
}
