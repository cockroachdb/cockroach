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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// MustGetMutableDatabaseByName returns a mutable database descriptor with
// properties according to the provided lookup options.
func (tc *Collection) MustGetMutableDatabaseByName(
	ctx context.Context, txn *kv.Txn, name string, options ...LookupOption,
) (*dbdesc.Mutable, error) {
	return tc.MayGetMutableDatabaseByName(ctx, txn, name, prependWithRequired(options)...)
}

// MayGetMutableDatabaseByName returns a mutable database descriptor with
// properties according to the provided lookup options.
func (tc *Collection) MayGetMutableDatabaseByName(
	ctx context.Context, txn *kv.Txn, name string, options ...LookupOption,
) (*dbdesc.Mutable, error) {
	flags := catalog.DatabaseLookupFlags{RequireMutable: true}
	for _, opt := range options {
		opt.apply(&flags)
	}
	desc, err := tc.getDatabaseByName(ctx, txn, name, flags)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(*dbdesc.Mutable), nil
}

// MustGetImmutableDatabaseByName returns an immutable database descriptor with
// properties according to the provided lookup options.
func (tc *Collection) MustGetImmutableDatabaseByName(
	ctx context.Context, txn *kv.Txn, name string, options ...LookupOption,
) (catalog.DatabaseDescriptor, error) {
	return tc.MayGetImmutableDatabaseByName(ctx, txn, name, prependWithRequired(options)...)
}

// MayGetImmutableDatabaseByName returns an immutable database descriptor with
// properties according to the provided lookup options.
func (tc *Collection) MayGetImmutableDatabaseByName(
	ctx context.Context, txn *kv.Txn, name string, options ...LookupOption,
) (catalog.DatabaseDescriptor, error) {
	flags := catalog.DatabaseLookupFlags{}
	for _, opt := range options {
		opt.apply(&flags)
	}
	flags.RequireMutable = false
	return tc.getDatabaseByName(ctx, txn, name, flags)
}

// MustGetImmutableUnleasedDatabaseByName returns an immutable database descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) MustGetImmutableUnleasedDatabaseByName(
	ctx context.Context, txn *kv.Txn, name string,
) (catalog.DatabaseDescriptor, error) {
	return tc.MustGetImmutableDatabaseByName(ctx, txn, name, WithoutLeased())
}

// getDatabaseByName returns a database descriptor with properties according to
// the provided lookup flags.
func (tc *Collection) getDatabaseByName(
	ctx context.Context, txn *kv.Txn, name string, flags catalog.DatabaseLookupFlags,
) (catalog.DatabaseDescriptor, error) {
	desc, err := tc.getDescriptorByName(ctx, txn, nil /* db */, nil /* sc */, name, flags, catalog.Database)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		if flags.Required {
			return nil, sqlerrors.NewUndefinedDatabaseError(name)
		}
		return nil, nil
	}
	db, ok := desc.(catalog.DatabaseDescriptor)
	if !ok {
		if flags.Required {
			return nil, sqlerrors.NewUndefinedDatabaseError(name)
		}
		return nil, nil
	}
	return db, nil
}

// MustGetImmutableDatabaseByID returns an immutable database descriptor with
// properties according to the provided lookup options.
func (tc *Collection) MustGetImmutableDatabaseByID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, options ...LookupOption,
) (catalog.DatabaseDescriptor, error) {
	return tc.MayGetImmutableDatabaseByID(ctx, txn, dbID, prependWithRequired(options)...)
}

// MayGetImmutableDatabaseByID returns an immutable database descriptor with
// properties according to the provided lookup options.
func (tc *Collection) MayGetImmutableDatabaseByID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, options ...LookupOption,
) (catalog.DatabaseDescriptor, error) {
	flags := catalog.DatabaseLookupFlags{}
	for _, opt := range options {
		opt.apply(&flags)
	}
	flags.RequireMutable = false
	_, db, err := tc.getDatabaseByID(ctx, txn, dbID, flags)
	return db, err
}

func (tc *Collection) getDatabaseByID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, flags catalog.DatabaseLookupFlags,
) (bool, catalog.DatabaseDescriptor, error) {
	descs, err := tc.getDescriptorsByID(ctx, txn, flags, dbID)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			if flags.Required {
				return false, nil, sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", dbID))
			}
			return false, nil, nil
		}
		return false, nil, err
	}
	db, ok := descs[0].(catalog.DatabaseDescriptor)
	if !ok {
		return false, nil, sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", dbID))
	}
	return true, db, nil
}
