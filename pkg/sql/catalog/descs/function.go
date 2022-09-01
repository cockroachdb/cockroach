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

// GetImmutableFunctionByID returns a immutable function descriptor.
func (tc *Collection) GetImmutableFunctionByID(
	ctx context.Context, txn *kv.Txn, fnID descpb.ID, flags tree.ObjectLookupFlags,
) (catalog.FunctionDescriptor, error) {
	flags.RequireMutable = false
	desc, err := tc.getFunctionByID(ctx, txn, fnID, flags)
	if err != nil {
		return nil, err
	}
	return desc, nil
}

// GetMutableFunctionByID returns a mutable function descriptor.
func (tc *Collection) GetMutableFunctionByID(
	ctx context.Context, txn *kv.Txn, fnID descpb.ID, flags tree.ObjectLookupFlags,
) (*funcdesc.Mutable, error) {
	flags.RequireMutable = true
	desc, err := tc.getFunctionByID(ctx, txn, fnID, flags)
	if err != nil {
		return nil, err
	}
	return desc.(*funcdesc.Mutable), nil
}

func (tc *Collection) getFunctionByID(
	ctx context.Context, txn *kv.Txn, fnID descpb.ID, flags tree.ObjectLookupFlags,
) (catalog.FunctionDescriptor, error) {
	descs, err := tc.getDescriptorsByID(ctx, txn, flags.CommonLookupFlags, fnID)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, errors.Wrapf(tree.ErrFunctionUndefined, "function %d does not exist", fnID)
		}
		return nil, err
	}
	fn, ok := descs[0].(catalog.FunctionDescriptor)
	if !ok {
		return nil, errors.Wrapf(tree.ErrFunctionUndefined, "function %d does not exist", fnID)
	}
	return fn, nil
}
