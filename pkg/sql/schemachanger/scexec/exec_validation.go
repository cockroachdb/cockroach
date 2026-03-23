// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

func executeValidateIndexes(
	ctx context.Context, deps Dependencies, ops []*scop.ValidateIndex,
) error {
	// Nothing to validate.
	if len(ops) == 0 {
		return nil
	}
	// Validate all validation operations are against the same table.
	if buildutil.CrdbTestBuild {
		for _, op := range ops {
			if op.TableID != ops[0].TableID {
				return errors.AssertionFailedf("all validation operations must be against the same table")
			}
		}
	}

	descs, err := deps.Catalog().MustReadImmutableDescriptors(ctx, ops[0].TableID)
	if err != nil {
		return err
	}
	desc := descs[0]
	table, ok := desc.(catalog.TableDescriptor)
	if !ok {
		return catalog.WrapTableDescRefErr(desc.GetID(), catalog.NewDescriptorTypeError(desc))
	}
	indexTypes := make(map[idxtype.T][]catalog.Index)
	for _, validateIndex := range ops {
		index, err := catalog.MustFindIndexByID(table, validateIndex.IndexID)
		if err != nil {
			return err
		}
		indexTypes[index.GetType()] = append(indexTypes[index.GetType()], index)
	}
	// Execute the validation operation as a node user.
	execOverride := sessiondata.NodeUserSessionDataOverride
	// Execute each type of index together, so that the table counts are only
	// fetched once.
	for typ, indexes := range indexTypes {
		var err error
		switch typ {
		case idxtype.FORWARD:
			err = deps.Validator().ValidateForwardIndexes(ctx, deps.TransactionalJobRegistry().CurrentJob(), table, indexes, execOverride)
		case idxtype.INVERTED:
			err = deps.Validator().ValidateInvertedIndexes(ctx, deps.TransactionalJobRegistry().CurrentJob(), table, indexes, execOverride)
		case idxtype.VECTOR:
			// TODO(drewk): consider whether we can perform useful validation for
			// vector indexes.
			continue
		default:
			return errors.AssertionFailedf("unexpected index type %v", typ)
		}
		if err != nil {
			return scerrors.SchemaChangerUserError(err)
		}
	}
	return nil
}

func executeValidateConstraint(
	ctx context.Context, deps Dependencies, op *scop.ValidateConstraint,
) error {
	descs, err := deps.Catalog().MustReadImmutableDescriptors(ctx, op.TableID)
	if err != nil {
		return err
	}
	desc := descs[0]
	table, err := catalog.AsTableDescriptor(desc)
	if err != nil {
		return err
	}
	constraint, err := catalog.MustFindConstraintByID(table, op.ConstraintID)
	if err != nil {
		return err
	}

	// Execute the validation operation as a node user.
	execOverride := sessiondata.NodeUserSessionDataOverride
	err = deps.Validator().ValidateConstraint(ctx, table, constraint, op.IndexIDForValidation, execOverride)
	if err != nil {
		return scerrors.SchemaChangerUserError(err)
	}
	return nil
}

func executeValidateColumnNotNull(
	ctx context.Context, deps Dependencies, op *scop.ValidateColumnNotNull,
) error {
	descs, err := deps.Catalog().MustReadImmutableDescriptors(ctx, op.TableID)
	if err != nil {
		return err
	}
	desc := descs[0]
	table, err := catalog.AsTableDescriptor(desc)
	if err != nil {
		return err
	}

	var constraint catalog.Constraint
	for _, ck := range table.CheckConstraints() {
		if ck.IsNotNullColumnConstraint() && ck.GetReferencedColumnID(0) == op.ColumnID {
			constraint = ck
		}
	}

	// Execute the validation operation as a node user.
	execOverride := sessiondata.NodeUserSessionDataOverride
	err = deps.Validator().ValidateConstraint(ctx, table, constraint, op.IndexIDForValidation, execOverride)
	if err != nil {
		return scerrors.SchemaChangerUserError(err)
	}
	return nil
}

func executeValidationOps(ctx context.Context, deps Dependencies, ops []scop.Op) (err error) {
	v := makeValidationAccumulator(ops)
	return v.validate(ctx, deps)
}

// validationAccumulator batches validation operations on a per-table basis, so
// that index validations can be batched together.
type validationAccumulator struct {
	indexes     map[descpb.ID][]*scop.ValidateIndex
	constraints map[descpb.ID][]*scop.ValidateConstraint
	notNulls    map[descpb.ID][]*scop.ValidateColumnNotNull
}

// makeValidationAccumulator creates a validationAccumulator from a list of
// validation operations.
func makeValidationAccumulator(ops []scop.Op) validationAccumulator {
	v := validationAccumulator{
		indexes:     make(map[descpb.ID][]*scop.ValidateIndex),
		constraints: make(map[descpb.ID][]*scop.ValidateConstraint),
		notNulls:    make(map[descpb.ID][]*scop.ValidateColumnNotNull),
	}
	for _, op := range ops {
		switch op := op.(type) {
		case *scop.ValidateIndex:
			v.indexes[op.TableID] = append(v.indexes[op.TableID], op)
		case *scop.ValidateConstraint:
			v.constraints[op.TableID] = append(v.constraints[op.TableID], op)
		case *scop.ValidateColumnNotNull:
			v.notNulls[op.TableID] = append(v.notNulls[op.TableID], op)
		default:
			panic("unimplemented")
		}
	}
	return v
}

// validate executes all validation operations in the accumulator.
func (v validationAccumulator) validate(ctx context.Context, deps Dependencies) error {
	// Batch all index operations per table for efficient execution.
	for _, tableIndexes := range v.indexes {
		if err := executeValidateIndexes(ctx, deps, tableIndexes); err != nil {
			// Error is wrapped with the operation inside the validation method.
			return errors.Wrapf(err, "%T: %v", tableIndexes, tableIndexes)
		}
	}
	// These operations do not support any type of batching.
	for _, tableNotNulls := range v.notNulls {
		for _, notNull := range tableNotNulls {
			if err := executeValidateColumnNotNull(ctx, deps, notNull); err != nil {
				if scerrors.HasSchemaChangerUserError(err) {
					return err
				}
				return errors.Wrapf(err, "%T: %v", notNull, notNull)
			}
		}
	}
	for _, tableConstraints := range v.constraints {
		for _, constraint := range tableConstraints {
			if err := executeValidateConstraint(ctx, deps, constraint); err != nil {
				if scerrors.HasSchemaChangerUserError(err) {
					return err
				}
				return errors.Wrapf(err, "%T: %v", constraint, constraint)
			}
		}
	}
	return nil
}
