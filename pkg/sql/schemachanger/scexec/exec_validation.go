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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

func executeValidateUniqueIndex(
	ctx context.Context, deps Dependencies, op *scop.ValidateUniqueIndex,
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
		User: security.RootUserName(),
	}
	if index.GetType() == descpb.IndexDescriptor_FORWARD {
		err = deps.IndexValidator().ValidateForwardIndexes(ctx, table, []catalog.Index{index}, execOverride)
	} else {
		err = deps.IndexValidator().ValidateInvertedIndexes(ctx, table, []catalog.Index{index}, execOverride)
	}
	return err
}

func executeValidateCheckConstraint(
	ctx context.Context, deps Dependencies, op *scop.ValidateCheckConstraint,
) error {
	return errors.Errorf("executeValidateCheckConstraint is not implemented")
}

func executeValidationOps(ctx context.Context, deps Dependencies, execute []scop.Op) error {
	for _, op := range execute {
		switch op := op.(type) {
		case *scop.ValidateUniqueIndex:
			return executeValidateUniqueIndex(ctx, deps, op)
		case *scop.ValidateCheckConstraint:
			return executeValidateCheckConstraint(ctx, deps, op)
		default:
			panic("unimplemented")
		}
	}
	return nil
}
