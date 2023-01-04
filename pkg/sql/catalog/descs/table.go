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
)

// GetMutableTableByName returns a mutable table descriptor with properties
// according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetMutableTableByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, objectFlags tree.ObjectLookupFlags,
) (found bool, _ *tabledesc.Mutable, _ error) {
	b := tc.ByName(txn).WithObjFlags(objectFlags)
	p, err := getObjectPrefix(ctx, b.Immutable(), name.Catalog(), name.Schema())
	if err != nil || p.Schema == nil {
		return false, nil, err
	}
	tbl, err := b.Mutable().Table(ctx, p.Database, p.Schema, name.Object())
	return tbl != nil, tbl, err
}

// GetImmutableTableByName returns a immutable table descriptor with properties
// according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetImmutableTableByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, objectFlags tree.ObjectLookupFlags,
) (found bool, _ catalog.TableDescriptor, _ error) {
	g := tc.ByName(txn).WithObjFlags(objectFlags).Immutable()
	p, err := getObjectPrefix(ctx, g, name.Catalog(), name.Schema())
	if err != nil || p.Schema == nil {
		return false, nil, err
	}
	tbl, err := g.Table(ctx, p.Database, p.Schema, name.Object())
	return tbl != nil, tbl, err
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
	err = tc.hydrateDescriptors(ctx, txn, defaultFlags(), descs)
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

// GetMutableTableByID returns a mutable table descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
// Required is ignored, and an error is always returned if no descriptor with
// the ID exists.
func (tc *Collection) GetMutableTableByID(
	ctx context.Context, txn *kv.Txn, tableID descpb.ID, flags tree.ObjectLookupFlags,
) (*tabledesc.Mutable, error) {
	return tc.ByID(txn).WithFlags(flags.CommonLookupFlags).Mutable().Table(ctx, tableID)
}

// GetMutableTableVersionByID is a variant of sqlbase.getTableDescFromID which returns a mutable
// table descriptor of the table modified in the same transaction.
// TODO (lucy): Usages should be replaced with GetMutableTableByID, but this
// needs a careful look at what flags should be passed in at each call site.
func (tc *Collection) GetMutableTableVersionByID(
	ctx context.Context, tableID descpb.ID, txn *kv.Txn,
) (*tabledesc.Mutable, error) {
	return tc.ByID(txn).Mutable().Table(ctx, tableID)
}
