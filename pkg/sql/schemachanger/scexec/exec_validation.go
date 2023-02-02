// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

func executeValidateUniqueIndex(
	ctx context.Context, deps Dependencies, op *scop.ValidateIndex,
) error {
	descs, err := deps.Catalog().MustReadImmutableDescriptors(ctx, op.TableID)
	if err != nil {
		return err
	}
	desc := descs[0]
	table, ok := desc.(catalog.TableDescriptor)
	if !ok {
		return catalog.WrapTableDescRefErr(desc.GetID(), catalog.NewDescriptorTypeError(desc))
	}
	index, err := catalog.MustFindIndexByID(table, op.IndexID)
	if err != nil {
		return err
	}
	// Execute the validation operation as a root user.
	execOverride := sessiondata.RootUserSessionDataOverride
	if index.GetType() == descpb.IndexDescriptor_FORWARD {
		err = deps.Validator().ValidateForwardIndexes(ctx, deps.TransactionalJobRegistry().CurrentJob(), table, []catalog.Index{index}, execOverride)
	} else {
		err = deps.Validator().ValidateInvertedIndexes(ctx, deps.TransactionalJobRegistry().CurrentJob(), table, []catalog.Index{index}, execOverride)
	}
	if err != nil {
		return scerrors.SchemaChangerUserError(err)
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

	// Execute the validation operation as a root user.
	execOverride := sessiondata.RootUserSessionDataOverride
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

	// Execute the validation operation as a root user.
	execOverride := sessiondata.RootUserSessionDataOverride
	err = deps.Validator().ValidateConstraint(ctx, table, constraint, op.IndexIDForValidation, execOverride)
	if err != nil {
		return scerrors.SchemaChangerUserError(err)
	}
	return nil
}

func executeValidationOps(ctx context.Context, deps Dependencies, ops []scop.Op) (err error) {
	for _, op := range ops {
		if err = executeValidationOp(ctx, deps, op); err != nil {
			return err
		}
	}
	return nil
}

func executeValidationOp(ctx context.Context, deps Dependencies, op scop.Op) (err error) {
	switch op := op.(type) {
	case *scop.ValidateIndex:
		if err = executeValidateUniqueIndex(ctx, deps, op); err != nil {
			if !scerrors.HasSchemaChangerUserError(err) {
				return errors.Wrapf(err, "%T: %v", op, op)
			}
			return err
		}
	case *scop.ValidateConstraint:
		if err = executeValidateConstraint(ctx, deps, op); err != nil {
			if !scerrors.HasSchemaChangerUserError(err) {
				return errors.Wrapf(err, "%T: %v", op, op)
			}
			return err
		}
	case *scop.ValidateColumnNotNull:
		if err = executeValidateColumnNotNull(ctx, deps, op); err != nil {
			if !scerrors.HasSchemaChangerUserError(err) {
				return errors.Wrapf(err, "%T: %v", op, op)
			}
			return err
		}

	default:
		panic("unimplemented")
	}
	return nil
}
