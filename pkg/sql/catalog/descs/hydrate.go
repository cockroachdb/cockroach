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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydrateddesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// hydrateDescriptors installs user defined type metadata in all types.T present
// in the descriptors slice.
//
// Hydration is not thread-safe while immutable descriptors are, therefore this
// method will replace un-hydrated immutable descriptors in the slice with
// hydrated copies. Mutable descriptors are not thread-safe and so are hydrated
// in-place. Dropped descriptors do not get hydrated.
//
// Optionally, when hydrating we can include offline descriptors and avoid
// leasing depending on the context. This is set via the flags.
//
// Collection method callers expect the descriptors to come back hydrated.
// In practice, array types here are not hydrated, and that's a bummer.
// Nobody presently is upset about it, but it's not a good thing.
// Ideally we'd have a clearer contract regarding hydration and the values
// stored in the various maps inside the collection. One might want to
// store only hydrated values in the various maps. This turns out to be
// somewhat tricky because we'd need to make sure to properly re-hydrate
// all the relevant descriptors when a type descriptor change. Leased
// descriptors are at least as tricky, plus, there we have a cache that
// works relatively well.
//
// TODO(ajwerner): Sort out the hydration mess; define clearly what is
// hydrated where and test the API boundary accordingly.
func (tc *Collection) hydrateDescriptors(
	ctx context.Context, txn *kv.Txn, flags tree.CommonLookupFlags, descs []catalog.Descriptor,
) error {

	var hydratableMutableIndexes, hydratableImmutableIndexes intsets.Fast
	for i, desc := range descs {
		if desc == nil || !hydrateddesc.IsHydratable(desc) {
			continue
		}
		if _, ok := desc.(catalog.MutableDescriptor); ok {
			hydratableMutableIndexes.Add(i)
		} else {
			hydratableImmutableIndexes.Add(i)
		}
	}

	// Hydrate mutable hydratable descriptors of the slice in-place.
	if !hydratableMutableIndexes.Empty() {
		typeFn := makeMutableTypeLookupFunc(tc, txn, descs)
		for _, i := range hydratableMutableIndexes.Ordered() {
			if err := hydrateddesc.Hydrate(ctx, descs[i], typeFn); err != nil {
				return err
			}
		}
	}

	// Replace immutable hydratable descriptors in the slice with hydrated copies
	// from the cache, or otherwise by creating a copy and hydrating it.
	if !hydratableImmutableIndexes.Empty() {
		typeFn := makeImmutableTypeLookupFunc(tc, txn, flags, descs)
		for _, i := range hydratableImmutableIndexes.Ordered() {
			desc := descs[i]
			// Utilize the cache of hydrated tables if we have one and this descriptor
			// was leased.
			// TODO(ajwerner): Consider surfacing the mechanism used to retrieve the
			// descriptor up to this layer.
			if hd := desc.(catalog.HydratableDescriptor); tc.canUseHydratedDescriptorCache(hd.GetID()) {
				if cached, err := tc.hydrated.GetHydratedDescriptor(ctx, hd, typeFn); err != nil {
					return err
				} else if cached != nil {
					descs[i] = cached
					continue
				}
			}
			// The cache decided not to give back a hydrated descriptor, likely
			// because either we've modified the table or one of the types or because
			// this transaction has a stale view of one of the relevant descriptors.
			// Proceed to hydrating a fresh copy.
			desc = desc.NewBuilder().BuildImmutable()
			if err := hydrateddesc.Hydrate(ctx, desc, typeFn); err != nil {
				return err
			}
			descs[i] = desc
		}
	}
	return nil
}

func makeMutableTypeLookupFunc(
	tc *Collection, txn *kv.Txn, descs []catalog.Descriptor,
) typedesc.TypeLookupFunc {
	var mut nstree.MutableCatalog
	for _, desc := range descs {
		if desc == nil {
			continue
		}
		if _, ok := desc.(catalog.MutableDescriptor); !ok {
			continue
		}
		mut.UpsertDescriptor(desc)
	}
	mutableLookupFunc := func(ctx context.Context, id descpb.ID, skipHydration bool) (catalog.Descriptor, error) {
		// This special case exists to deal with the desire to use enums in the
		// system database, and the fact that the hydration contract is such that
		// when we resolve types mutably, we resolve the mutable type descriptors
		// they reference, which may have in-memory changes. The problem with this
		// is that one is not permitted to mutably resolve the public schema
		// descriptor for the system database. We only want it for the name, so
		// let the caller have the immutable copy.
		if id == catconstants.PublicSchemaID {
			return tc.GetImmutableDescriptorByID(ctx, txn, id, tree.CommonLookupFlags{
				Required:      true,
				AvoidLeased:   true,
				SkipHydration: skipHydration,
			})
		}
		return tc.getDescriptorByID(ctx, txn, tree.CommonLookupFlags{
			Required:       true,
			AvoidLeased:    true,
			RequireMutable: true,
			IncludeOffline: true,
			IncludeDropped: true,
			SkipHydration:  skipHydration,
		}, id)
	}
	return hydrateddesc.MakeTypeLookupFuncForHydration(mut, mutableLookupFunc)
}

func makeImmutableTypeLookupFunc(
	tc *Collection, txn *kv.Txn, flags tree.CommonLookupFlags, descs []catalog.Descriptor,
) typedesc.TypeLookupFunc {
	var imm nstree.MutableCatalog
	for _, desc := range descs {
		if desc == nil {
			continue
		}
		if _, ok := desc.(catalog.MutableDescriptor); ok {
			continue
		}
		imm.UpsertDescriptor(desc)
	}
	immutableLookupFunc := func(ctx context.Context, id descpb.ID, skipHydration bool) (catalog.Descriptor, error) {
		return tc.GetImmutableDescriptorByID(ctx, txn, id, tree.CommonLookupFlags{
			Required:       true,
			AvoidLeased:    flags.AvoidLeased,
			IncludeOffline: flags.IncludeOffline,
			AvoidSynthetic: true,
			SkipHydration:  skipHydration,
		})
	}
	return hydrateddesc.MakeTypeLookupFuncForHydration(imm, immutableLookupFunc)
}

// HydrateCatalog installs type metadata in the type.T objects present for all
// objects referencing them in the catalog.
func HydrateCatalog(ctx context.Context, c nstree.MutableCatalog) error {
	ctx, sp := tracing.ChildSpan(ctx, "descs.HydrateCatalog")
	defer sp.Finish()

	fakeLookupFunc := func(_ context.Context, id descpb.ID, skipHydration bool) (catalog.Descriptor, error) {
		return nil, catalog.WrapDescRefErr(id, catalog.ErrDescriptorNotFound)
	}
	typeLookupFunc := hydrateddesc.MakeTypeLookupFuncForHydration(c, fakeLookupFunc)
	return c.ForEachDescriptor(func(desc catalog.Descriptor) error {
		if !hydrateddesc.IsHydratable(desc) {
			return nil
		}
		if _, isMutable := desc.(catalog.MutableDescriptor); isMutable {
			return hydrateddesc.Hydrate(ctx, desc, typeLookupFunc)
		}
		// Deep-copy the immutable descriptor and overwrite the catalog entry.
		desc = desc.NewBuilder().BuildImmutable()
		defer c.UpsertDescriptor(desc)
		return hydrateddesc.Hydrate(ctx, desc, typeLookupFunc)
	})
}

func (tc *Collection) canUseHydratedDescriptorCache(id descpb.ID) bool {
	return tc.hydrated != nil &&
		!tc.cr.IsIDInCache(id) &&
		tc.uncommitted.getUncommittedByID(id) == nil &&
		tc.synthetic.getSyntheticByID(id) == nil
}
