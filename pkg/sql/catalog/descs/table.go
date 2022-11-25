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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// MustGetMutableTableByName returns a mutable table descriptor with properties
// according to the provided lookup options.
func (tc *Collection) MustGetMutableTableByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, options ...LookupOption,
) (*tabledesc.Mutable, error) {
	return tc.MayGetMutableTableByName(ctx, txn, name, prependWithRequired(options)...)
}

// MayGetMutableTableByName returns a mutable table descriptor with properties
// according to the provided lookup options.
func (tc *Collection) MayGetMutableTableByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, options ...LookupOption,
) (*tabledesc.Mutable, error) {
	var flags catalog.ObjectLookupFlags
	flags.RequireMutable = true
	for _, opt := range options {
		opt.applyObject(&flags)
	}
	found, desc, err := tc.getTableByName(ctx, txn, name, flags)
	if err != nil || !found {
		return nil, err
	}
	return desc.(*tabledesc.Mutable), nil
}

// MustGetImmutableTableByName returns a immutable table descriptor with properties
// according to the provided lookup options.
func (tc *Collection) MustGetImmutableTableByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, options ...LookupOption,
) (catalog.TableDescriptor, error) {
	return tc.MayGetImmutableTableByName(ctx, txn, name, prependWithRequired(options)...)
}

// MayGetImmutableTableByName returns a immutable table descriptor with properties
// according to the provided lookup options.
func (tc *Collection) MayGetImmutableTableByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, options ...LookupOption,
) (catalog.TableDescriptor, error) {
	var flags catalog.ObjectLookupFlags
	for _, opt := range options {
		opt.applyObject(&flags)
	}
	_, tbl, err := tc.getTableByName(ctx, txn, name, flags)
	return tbl, err
}

// getTableByName returns a table descriptor with properties according to the
// provided lookup flags.
func (tc *Collection) getTableByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags catalog.ObjectLookupFlags,
) (found bool, _ catalog.TableDescriptor, err error) {
	flags.DesiredObjectKind = catalog.TableObject
	_, desc, err := tc.GetObjectByName(
		ctx, txn, name.Catalog(), name.Schema(), name.Object(), flags)
	if err != nil || desc == nil {
		return false, nil, err
	}
	return true, desc.(catalog.TableDescriptor), nil
}

// GetLeasedImmutableTableByID returns a leased immutable table descriptor by
// its ID.
func (tc *Collection) GetLeasedImmutableTableByID(
	ctx context.Context, txn *kv.Txn, tableID descpb.ID,
) (catalog.TableDescriptor, error) {
	desc, _, err := tc.leased.getByID(ctx, tc.deadlineHolder(txn), tableID)
	if err != nil || desc == nil {
		return nil, err
	}
	descs := []catalog.Descriptor{desc}
	err = tc.hydrateDescriptors(ctx, txn, catalog.CommonLookupFlags{}, descs)
	if err != nil {
		return nil, err
	}
	return catalog.AsTableDescriptor(descs[0])
}

// GetUncommittedMutableTableByID returns an uncommitted mutable table by its
// ID.
func (tc *Collection) GetUncommittedMutableTableByID(
	id descpb.ID,
) (catalog.TableDescriptor, *tabledesc.Mutable, error) {
	original := tc.uncommitted.getOriginalByID(id)
	mut := tc.uncommitted.getUncommittedMutableByID(id)
	if mut == nil {
		return nil, nil, nil
	}
	if _, err := catalog.AsTableDescriptor(mut); err != nil {
		return nil, nil, err
	}
	if original == nil {
		return nil, mut.(*tabledesc.Mutable), nil
	}
	if _, err := catalog.AsTableDescriptor(original); err != nil {
		return nil, nil, err
	}
	return original.(catalog.TableDescriptor), mut.(*tabledesc.Mutable), nil
}

// MustGetMutableTableByID returns a mutable table descriptor with
// properties according to the provided lookup options.
func (tc *Collection) MustGetMutableTableByID(
	ctx context.Context, txn *kv.Txn, tableID descpb.ID, options ...LookupOption,
) (*tabledesc.Mutable, error) {
	return tc.MayGetMutableTableByID(ctx, txn, tableID, prependWithRequired(options)...)
}

// MayGetMutableTableByID returns a mutable table descriptor with
// properties according to the provided lookup options.
func (tc *Collection) MayGetMutableTableByID(
	ctx context.Context, txn *kv.Txn, tableID descpb.ID, options ...LookupOption,
) (*tabledesc.Mutable, error) {
	var flags catalog.ObjectLookupFlags
	flags.RequireMutable = true
	for _, opt := range options {
		opt.applyObject(&flags)
	}
	desc, err := tc.getTableByID(ctx, txn, tableID, flags)
	if err != nil {
		return nil, err
	}
	return desc.(*tabledesc.Mutable), nil
}

// GetMutableTableVersionByID is a variant of sqlbase.getTableDescFromID which returns a mutable
// table descriptor of the table modified in the same transaction.
// Deprecated in favor of GetMutableTableByID.
// TODO (lucy): Usages should be replaced with GetMutableTableByID, but this
// needs a careful look at what flags should be passed in at each call site.
func (tc *Collection) GetMutableTableVersionByID(
	ctx context.Context, tableID descpb.ID, txn *kv.Txn,
) (*tabledesc.Mutable, error) {
	return tc.MayGetMutableTableByID(ctx, txn, tableID, WithOffline(), WithDropped())
}

// MustGetImmutableTableByID returns an immutable table descriptor with
// properties according to the provided lookup options.
func (tc *Collection) MustGetImmutableTableByID(
	ctx context.Context, txn *kv.Txn, tableID descpb.ID, options ...LookupOption,
) (catalog.TableDescriptor, error) {
	var flags catalog.ObjectLookupFlags
	flags.Required = true
	for _, opt := range options {
		opt.applyObject(&flags)
	}
	return tc.getTableByID(ctx, txn, tableID, flags)
}

// MayGetImmutableTableByID returns an immutable table descriptor with
// properties according to the provided lookup options.
func (tc *Collection) MayGetImmutableTableByID(
	ctx context.Context, txn *kv.Txn, tableID descpb.ID, options ...LookupOption,
) (catalog.TableDescriptor, error) {
	var flags catalog.ObjectLookupFlags
	for _, opt := range options {
		opt.applyObject(&flags)
	}
	return tc.getTableByID(ctx, txn, tableID, flags)
}

func (tc *Collection) getTableByID(
	ctx context.Context, txn *kv.Txn, tableID descpb.ID, flags catalog.ObjectLookupFlags,
) (catalog.TableDescriptor, error) {
	descs, err := tc.getDescriptorsByID(ctx, txn, flags.CommonLookupFlags, tableID)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, sqlerrors.NewUndefinedRelationError(
				&tree.TableRef{TableID: int64(tableID)})
		}
		return nil, err
	}
	table, ok := descs[0].(catalog.TableDescriptor)
	if !ok {
		return nil, sqlerrors.NewUndefinedRelationError(
			&tree.TableRef{TableID: int64(tableID)})
	}
	return table, nil
}
