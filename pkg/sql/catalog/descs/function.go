// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// MustGetImmutableFunctionByID returns a immutable function descriptor.
func (tc *Collection) MustGetImmutableFunctionByID(
	ctx context.Context, txn *kv.Txn, fnID descpb.ID, options ...LookupOption,
) (catalog.FunctionDescriptor, error) {
	return tc.MayGetImmutableFunctionByID(ctx, txn, fnID, prependWithRequired(options)...)
}

// MayGetImmutableFunctionByID returns a immutable function descriptor.
func (tc *Collection) MayGetImmutableFunctionByID(
	ctx context.Context, txn *kv.Txn, fnID descpb.ID, options ...LookupOption,
) (catalog.FunctionDescriptor, error) {
	var flags catalog.ObjectLookupFlags
	for _, opt := range options {
		opt.applyObject(&flags)
	}
	desc, err := tc.getFunctionByID(ctx, txn, fnID, flags)
	if err != nil {
		return nil, err
	}
	return desc, nil
}

// MustGetMutableFunctionByID returns a mutable function descriptor.
func (tc *Collection) MustGetMutableFunctionByID(
	ctx context.Context, txn *kv.Txn, fnID descpb.ID, options ...LookupOption,
) (*funcdesc.Mutable, error) {
	return tc.MayGetMutableFunctionByID(ctx, txn, fnID, prependWithRequired(options)...)
}

// MayGetMutableFunctionByID returns a mutable function descriptor.
func (tc *Collection) MayGetMutableFunctionByID(
	ctx context.Context, txn *kv.Txn, fnID descpb.ID, options ...LookupOption,
) (*funcdesc.Mutable, error) {
	var flags catalog.ObjectLookupFlags
	flags.RequireMutable = true
	for _, opt := range options {
		opt.applyObject(&flags)
	}
	desc, err := tc.getFunctionByID(ctx, txn, fnID, flags)
	if err != nil {
		return nil, err
	}
	return desc.(*funcdesc.Mutable), nil
}

func (tc *Collection) getFunctionByID(
	ctx context.Context, txn *kv.Txn, fnID descpb.ID, flags catalog.ObjectLookupFlags,
) (catalog.FunctionDescriptor, error) {
	desc, err := tc.getDescriptorByID(ctx, txn, flags.CommonLookupFlags, fnID)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, errors.Wrapf(tree.ErrFunctionUndefined, "function %d does not exist", fnID)
		}
		return nil, err
	}
	fn, ok := desc.(catalog.FunctionDescriptor)
	if !ok {
		return nil, errors.Wrapf(tree.ErrFunctionUndefined, "function %d does not exist", fnID)
	}
	return fn, nil
}
