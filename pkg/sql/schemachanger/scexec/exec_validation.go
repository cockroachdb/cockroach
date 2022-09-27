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

	"github.com/cockroachdb/cockroach/pkg/security/username"
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
	index, err := table.FindIndexWithID(op.IndexID)
	if err != nil {
		return err
	}
	// Execute the validation operation as a root user.
	execOverride := sessiondata.InternalExecutorOverride{
		User: username.RootUserName(),
	}
	if index.GetType() == descpb.IndexDescriptor_FORWARD {
		err = deps.IndexValidator().ValidateForwardIndexes(ctx, table, []catalog.Index{index}, execOverride)
	} else {
		err = deps.IndexValidator().ValidateInvertedIndexes(ctx, table, []catalog.Index{index}, execOverride)
	}
	if err != nil {
		return scerrors.SchemaChangerUserError(err)
	}
	return nil
}

func executeValidateCheckConstraint(
	ctx context.Context, deps Dependencies, op *scop.ValidateCheckConstraint,
) error {
	return errors.Errorf("executeValidateCheckConstraint is not implemented")
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
			return errors.Wrapf(err, "%T: %v", op, op)
		}
	case *scop.ValidateCheckConstraint:
		if err = executeValidateCheckConstraint(ctx, deps, op); err != nil {
			return errors.Wrapf(err, "%T: %v", op, op)
		}
	default:
		panic("unimplemented")
	}
	return nil
}
