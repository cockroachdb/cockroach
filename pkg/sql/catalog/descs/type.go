// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// GetMutableTypeByName returns a mutable type descriptor with properties
// according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetMutableTypeByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (found bool, _ *typedesc.Mutable, _ error) {
	flags.RequireMutable = true
	found, desc, err := tc.getTypeByName(ctx, txn, name, flags)
	if err != nil || !found {
		return false, nil, err
	}
	return true, desc.(*typedesc.Mutable), nil
}

// GetImmutableTypeByName returns a mutable type descriptor with properties
// according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetImmutableTypeByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (found bool, _ catalog.TypeDescriptor, _ error) {
	flags.RequireMutable = false
	return tc.getTypeByName(ctx, txn, name, flags)
}

// getTypeByName returns a type descriptor with properties according to the
// provided lookup flags.
func (tc *Collection) getTypeByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (found bool, _ catalog.TypeDescriptor, err error) {
	flags.DesiredObjectKind = tree.TypeObject
	_, desc, err := tc.getObjectByName(
		ctx, txn, name.Catalog(), name.Schema(), name.Object(), flags)
	if err != nil || desc == nil {
		return false, nil, err
	}
	return true, desc.(catalog.TypeDescriptor), nil
}

// GetMutableTypeVersionByID is the equivalent of GetMutableTableDescriptorByID
// but for accessing types.
// Deprecated in favor of GetMutableTypeByID.
// TODO (lucy): Usages should be replaced with GetMutableTypeByID, but this
// needs a careful look at what flags should be passed in at each call site.
func (tc *Collection) GetMutableTypeVersionByID(
	ctx context.Context, txn *kv.Txn, typeID descpb.ID,
) (*typedesc.Mutable, error) {
	return tc.GetMutableTypeByID(ctx, txn, typeID, tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{
			IncludeOffline: true,
			IncludeDropped: true,
		},
	})
}

// GetMutableTypeByID returns a mutable type descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
// Required is ignored, and an error is always returned if no descriptor with
// the ID exists.
func (tc *Collection) GetMutableTypeByID(
	ctx context.Context, txn *kv.Txn, typeID descpb.ID, flags tree.ObjectLookupFlags,
) (*typedesc.Mutable, error) {
	flags.RequireMutable = true
	desc, err := tc.getTypeByID(ctx, txn, typeID, flags)
	if err != nil {
		return nil, err
	}
	return desc.(*typedesc.Mutable), nil
}

// GetImmutableTypeByID returns an immutable type descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
// Required is ignored, and an error is always returned if no descriptor with
// the ID exists.
func (tc *Collection) GetImmutableTypeByID(
	ctx context.Context, txn *kv.Txn, typeID descpb.ID, flags tree.ObjectLookupFlags,
) (catalog.TypeDescriptor, error) {
	flags.RequireMutable = false
	return tc.getTypeByID(ctx, txn, typeID, flags)
}

func (tc *Collection) getTypeByID(
	ctx context.Context, txn *kv.Txn, typeID descpb.ID, flags tree.ObjectLookupFlags,
) (catalog.TypeDescriptor, error) {
	desc, err := tc.getDescriptorByID(ctx, txn, typeID, flags.CommonLookupFlags)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, pgerror.Newf(
				pgcode.UndefinedObject, "type with ID %d does not exist", typeID)
		}
		return nil, err
	}
	typ, ok := desc.(catalog.TypeDescriptor)
	if !ok {
		return nil, pgerror.Newf(
			pgcode.UndefinedObject, "type with ID %d does not exist", typeID)
	}
	return typ, nil
}
