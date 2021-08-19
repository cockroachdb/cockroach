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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/errors"
)

// ValidateUncommittedDescriptors validates all uncommitted descriptors.
// Validation includes cross-reference checks. Referenced descriptors are
// read from the store unless they happen to also be part of the uncommitted
// descriptor set. We purposefully avoid using leased descriptors as those may
// be one version behind, in which case it's possible (and legitimate) that
// those are missing back-references which would cause validation to fail.
func (tc *Collection) ValidateUncommittedDescriptors(ctx context.Context, txn *kv.Txn) (err error) {
	if tc.skipValidationOnWrite || !ValidateOnWriteEnabled.Get(&tc.settings.SV) {
		return nil
	}
	descs := tc.uncommitted.getUncommittedDescriptorsForValidation()
	if len(descs) == 0 {
		return nil
	}

	bdg := collectionBatchDescGetter{tc: tc, txn: txn}
	return errors.CombineErrors(err, catalog.Validate(
		ctx,
		bdg,
		catalog.ValidationWriteTelemetry,
		catalog.ValidationLevelAllPreTxnCommit,
		descs...,
	).CombinedError())
}

// collectionBatchDescGetter wraps a Collection to implement the
// catalog.BatchDescGetter interface for validation.
type collectionBatchDescGetter struct {
	tc  *Collection
	txn *kv.Txn
}

var _ catalog.BatchDescGetter = &collectionBatchDescGetter{}

func (c collectionBatchDescGetter) fallback() catalog.BatchDescGetter {
	return catalogkv.NewOneLevelUncachedDescGetter(c.txn, c.tc.codec())
}

// GetDescs implements the catalog.BatchDescGetter interface by leveraging the
// collection's uncommitted descriptors.
func (c collectionBatchDescGetter) GetDescs(
	ctx context.Context, reqs []descpb.ID,
) (ret []catalog.Descriptor, _ error) {
	ret = make([]catalog.Descriptor, len(reqs))
	fallbackReqs := make([]descpb.ID, 0, len(reqs))
	fallbackRetIndexes := make([]int, 0, len(reqs))
	for i, id := range reqs {
		desc, err := c.fastDescLookup(ctx, id)
		if err != nil {
			return nil, err
		}
		if desc == nil {
			fallbackReqs = append(fallbackReqs, id)
			fallbackRetIndexes = append(fallbackRetIndexes, i)
		} else {
			ret[i] = desc
		}
	}
	if len(fallbackReqs) > 0 {
		fallbackRet, err := c.fallback().GetDescs(ctx, fallbackReqs)
		if err != nil {
			return nil, err
		}
		for j, desc := range fallbackRet {
			ret[fallbackRetIndexes[j]] = desc
		}
	}
	return ret, nil
}

func (c collectionBatchDescGetter) fastDescLookup(
	ctx context.Context, id descpb.ID,
) (catalog.Descriptor, error) {
	leaseCacheEntry := c.tc.leased.cache.GetByID(id)
	if leaseCacheEntry != nil {
		return leaseCacheEntry.(lease.LeasedDescriptor).Underlying(), nil
	}
	return c.tc.uncommitted.getByID(id), nil
}

// GetNamespaceEntries implements the catalog.BatchDescGetter interface by
// delegating to the fallback catalogkv implementation.
func (c collectionBatchDescGetter) GetNamespaceEntries(
	ctx context.Context, reqs []descpb.NameInfo,
) (ret []descpb.ID, _ error) {
	ret = make([]descpb.ID, len(reqs))
	fallbackReqs := make([]descpb.NameInfo, 0, len(reqs))
	fallbackRetIndexes := make([]int, 0, len(reqs))
	for i, req := range reqs {
		found, id, err := c.fastNamespaceLookup(ctx, req)
		if err != nil {
			return nil, err
		}
		if found {
			ret[i] = id
		} else {
			fallbackReqs = append(fallbackReqs, req)
			fallbackRetIndexes = append(fallbackRetIndexes, i)
		}
	}
	if len(fallbackReqs) > 0 {
		fallbackRet, err := c.fallback().GetNamespaceEntries(ctx, fallbackReqs)
		if err != nil {
			return nil, err
		}
		for j, id := range fallbackRet {
			ret[fallbackRetIndexes[j]] = id
		}
	}
	return ret, nil
}

func (c collectionBatchDescGetter) fastNamespaceLookup(
	ctx context.Context, req descpb.NameInfo,
) (found bool, id descpb.ID, err error) {
	// Handle special cases.
	// TODO(postamar): namespace lookups should go through Collection
	switch req.ParentID {
	case descpb.InvalidID:
		if req.ParentSchemaID == descpb.InvalidID && req.Name == catconstants.SystemDatabaseName {
			// Looking up system database ID, which is hard-coded.
			return true, keys.SystemDatabaseID, nil
		}
	case keys.SystemDatabaseID:
		// Looking up system database objects, which are cached.
		id, err = lookupSystemDatabaseNamespaceCache(ctx, c.tc.codec(), req.ParentSchemaID, req.Name)
		return id != descpb.InvalidID, id, err
	}
	return false, descpb.InvalidID, nil
}

// GetDesc implements the catalog.BatchDescGetter interface by delegating to
// GetDescs.
func (c collectionBatchDescGetter) GetDesc(
	ctx context.Context, id descpb.ID,
) (catalog.Descriptor, error) {
	descs, err := c.GetDescs(ctx, []descpb.ID{id})
	if err != nil {
		return nil, err
	}
	return descs[0], nil
}

// GetNamespaceEntry implements the catalog.BatchDescGetter interface by
// delegating to GetNamespaceEntries.
func (c collectionBatchDescGetter) GetNamespaceEntry(
	ctx context.Context, parentID, parentSchemaID descpb.ID, name string,
) (descpb.ID, error) {
	return c.fallback().GetNamespaceEntry(ctx, parentID, parentSchemaID, name)
}
