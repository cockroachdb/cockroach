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

// ErrMutableTableImplicitType indicates that a table implicit type was fetched
// as a mutable, which is not allowed.
var ErrMutableTableImplicitType = pgerror.Newf(pgcode.DependentObjectsStillExist, "table implicit type not mutable")

// MustGetMutableTypeByName returns a mutable type descriptor with properties
// according to the provided lookup options.
func (tc *Collection) MustGetMutableTypeByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, options ...LookupOption,
) (*typedesc.Mutable, error) {
	return tc.MayGetMutableTypeByName(ctx, txn, name, prependWithRequired(options)...)
}

// MayGetMutableTypeByName returns a mutable type descriptor with properties
// according to the provided lookup options.
func (tc *Collection) MayGetMutableTypeByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, options ...LookupOption,
) (*typedesc.Mutable, error) {
	var flags catalog.ObjectLookupFlags
	flags.RequireMutable = true
	for _, opt := range options {
		opt.applyObject(&flags)
	}
	_, desc, err := tc.getTypeByName(ctx, txn, name, flags)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(*typedesc.Mutable), nil
}

// MustGetImmutableTypeByName returns an immutable type descriptor with properties
// according to the provided lookup options.
func (tc *Collection) MustGetImmutableTypeByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, options ...LookupOption,
) (catalog.TypeDescriptor, error) {
	return tc.MayGetImmutableTypeByName(ctx, txn, name, prependWithRequired(options)...)
}

// MayGetImmutableTypeByName returns an immutable type descriptor with properties
// according to the provided lookup options.
func (tc *Collection) MayGetImmutableTypeByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, options ...LookupOption,
) (catalog.TypeDescriptor, error) {
	var flags catalog.ObjectLookupFlags
	for _, opt := range options {
		opt.applyObject(&flags)
	}
	_, desc, err := tc.getTypeByName(ctx, txn, name, flags)
	return desc, err
}

// getTypeByName returns a type descriptor with properties according to the
// provided lookup flags.
func (tc *Collection) getTypeByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags catalog.ObjectLookupFlags,
) (found bool, _ catalog.TypeDescriptor, err error) {
	flags.DesiredObjectKind = catalog.TypeObject
	_, desc, err := tc.GetObjectByName(
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
	return tc.MayGetMutableTypeByID(
		ctx,
		txn,
		typeID,
		WithOffline(),
		WithDropped(),
	)
}

// MustGetMutableTypeByID returns a mutable type descriptor with
// properties according to the provided lookup options.
func (tc *Collection) MustGetMutableTypeByID(
	ctx context.Context, txn *kv.Txn, typeID descpb.ID, options ...LookupOption,
) (*typedesc.Mutable, error) {
	return tc.MayGetMutableTypeByID(ctx, txn, typeID, prependWithRequired(options)...)
}

// MayGetMutableTypeByID returns a mutable type descriptor with
// properties according to the provided lookup options.
func (tc *Collection) MayGetMutableTypeByID(
	ctx context.Context, txn *kv.Txn, typeID descpb.ID, options ...LookupOption,
) (*typedesc.Mutable, error) {
	var flags catalog.ObjectLookupFlags
	flags.RequireMutable = true
	for _, opt := range options {
		opt.applyObject(&flags)
	}
	desc, err := tc.getTypeByID(ctx, txn, typeID, flags)
	if err != nil || desc == nil {
		return nil, err
	}
	switch t := desc.(type) {
	case *typedesc.Mutable:
		return t, nil
	case *typedesc.TableImplicitRecordType:
		return nil, errors.Wrapf(ErrMutableTableImplicitType, "cannot modify table record type %q", desc.GetName())
	}
	return nil, errors.AssertionFailedf("unhandled type descriptor type %T during GetMutableTypeByID", desc)
}

// MustGetImmutableTypeByID returns an immutable type descriptor with
// properties according to the provided lookup options.
func (tc *Collection) MustGetImmutableTypeByID(
	ctx context.Context, txn *kv.Txn, typeID descpb.ID, options ...LookupOption,
) (catalog.TypeDescriptor, error) {
	var flags catalog.ObjectLookupFlags
	flags.Required = true
	for _, opt := range options {
		opt.applyObject(&flags)
	}
	return tc.getTypeByID(ctx, txn, typeID, flags)
}

// MayGetImmutableTypeByID returns an immutable type descriptor with
// properties according to the provided lookup options.
func (tc *Collection) MayGetImmutableTypeByID(
	ctx context.Context, txn *kv.Txn, typeID descpb.ID, options ...LookupOption,
) (catalog.TypeDescriptor, error) {
	var flags catalog.ObjectLookupFlags
	for _, opt := range options {
		opt.applyObject(&flags)
	}
	return tc.getTypeByID(ctx, txn, typeID, flags)
}

func (tc *Collection) getTypeByID(
	ctx context.Context, txn *kv.Txn, typeID descpb.ID, flags catalog.ObjectLookupFlags,
) (catalog.TypeDescriptor, error) {
	descs, err := tc.getDescriptorsByID(ctx, txn, flags.CommonLookupFlags, typeID)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, pgerror.Newf(
				pgcode.UndefinedObject, "type with ID %d does not exist", typeID)
		}
		return nil, err
	}
	switch t := descs[0].(type) {
	case catalog.TypeDescriptor:
		// User-defined type.
		return t, nil
	case catalog.TableDescriptor:
		return typedesc.CreateImplicitRecordTypeFromTableDesc(t)
	}
	return nil, pgerror.Newf(
		pgcode.UndefinedObject, "type with ID %d does not exist", typeID)
}
