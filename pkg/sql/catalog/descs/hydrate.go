// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
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
	ctx context.Context, txn *kv.Txn, flags getterFlags, descs []catalog.Descriptor,
) error {
	if flags.layerFilters.withoutHydration {
		return nil
	}
	var hydratableMutableIndexes, hydratableImmutableIndexes intsets.Fast
	for i, desc := range descs {
		if desc == nil || !isHydratable(desc) {
			continue
		}
		if _, ok := desc.(catalog.MutableDescriptor); ok {
			hydratableMutableIndexes.Add(i)
		} else {
			hydratableImmutableIndexes.Add(i)
		}
	}

	// hydrate mutable hydratable descriptors of the slice in-place.
	if !hydratableMutableIndexes.Empty() {
		typeFn := makeMutableTypeLookupFunc(tc, txn, descs)
		for _, i := range hydratableMutableIndexes.Ordered() {
			if err := hydrate(ctx, descs[i], typeFn); err != nil {
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
			if tc.canUseHydratedDescriptorCache(desc.GetID()) {
				if cached, err := tc.hydrated.GetHydratedDescriptor(ctx, desc, typeFn); err != nil {
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
			if err := hydrate(ctx, desc, typeFn); err != nil {
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
	var mc nstree.MutableCatalog
	for _, desc := range descs {
		if desc == nil {
			continue
		}
		if _, ok := desc.(catalog.MutableDescriptor); !ok {
			continue
		}
		mc.UpsertDescriptor(desc)
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
			return schemadesc.GetPublicSchema(), nil
		}
		flags := getterFlags{
			contextFlags: contextFlags{
				isMutable: true,
			},
			layerFilters: layerFilters{
				withoutSynthetic: true,
				withoutLeased:    true,
				withoutHydration: skipHydration,
			},
		}
		g := ByIDGetter(makeGetterBase(txn, tc, flags))
		return g.Desc(ctx, id)
	}
	return makeTypeLookupFuncForHydration(mc, mutableLookupFunc)
}

func makeImmutableTypeLookupFunc(
	tc *Collection, txn *kv.Txn, flags getterFlags, descs []catalog.Descriptor,
) typedesc.TypeLookupFunc {
	var mc nstree.MutableCatalog
	for _, desc := range descs {
		if desc == nil {
			continue
		}
		if _, ok := desc.(catalog.MutableDescriptor); ok {
			continue
		}
		mc.UpsertDescriptor(desc)
	}
	immutableLookupFunc := func(ctx context.Context, id descpb.ID, skipHydration bool) (catalog.Descriptor, error) {
		f := getterFlags{
			layerFilters: layerFilters{
				withoutSynthetic: true,
				withoutLeased:    flags.layerFilters.withoutLeased,
				withoutHydration: skipHydration,
			},
			descFilters: descFilters{
				withoutDropped: true,
				withoutOffline: flags.descFilters.withoutOffline,
			},
		}
		g := ByIDGetter(makeGetterBase(txn, tc, f))
		return g.Desc(ctx, id)
	}
	return makeTypeLookupFuncForHydration(mc, immutableLookupFunc)
}

// HydrateCatalog installs type metadata in the type.T objects present for all
// objects referencing them in the catalog.
func HydrateCatalog(ctx context.Context, c nstree.MutableCatalog) error {
	ctx, sp := tracing.ChildSpan(ctx, "descs.HydrateCatalog")
	defer sp.Finish()

	fakeLookupFunc := func(_ context.Context, id descpb.ID, skipHydration bool) (catalog.Descriptor, error) {
		return nil, catalog.NewDescriptorNotFoundError(id)
	}
	typeLookupFunc := makeTypeLookupFuncForHydration(c, fakeLookupFunc)
	var hydratable []catalog.Descriptor
	_ = c.ForEachDescriptor(func(desc catalog.Descriptor) error {
		if isHydratable(desc) {
			hydratable = append(hydratable, desc)
		}
		return nil
	})
	for _, desc := range hydratable {
		if _, isMutable := desc.(catalog.MutableDescriptor); !isMutable {
			// Deep-copy the immutable descriptor and overwrite the catalog entry.
			desc = desc.NewBuilder().BuildImmutable()
			c.UpsertDescriptor(desc)
		}
		if err := hydrate(ctx, desc, typeLookupFunc); err != nil {
			return err
		}
	}
	return nil
}

func (tc *Collection) canUseHydratedDescriptorCache(id descpb.ID) bool {
	return tc.hydrated != nil &&
		!tc.cr.IsIDInCache(id) &&
		tc.uncommitted.getUncommittedByID(id) == nil &&
		tc.synthetic.getSyntheticByID(id) == nil
}

// hydrationLookupFunc is the type of function required to look up type
// descriptors and their parent schemas and databases when hydrating an object.
type hydrationLookupFunc func(ctx context.Context, id descpb.ID, skipHydration bool) (catalog.Descriptor, error)

// isHydratable returns false iff the descriptor definitely does not require
// hydration
func isHydratable(desc catalog.Descriptor) bool {
	if desc.Dropped() {
		// Don't hydrate dropped descriptors.
		return false
	}
	return desc.MaybeRequiresTypeHydration()
}

// hydrate ensures that type metadata is present in any type.T objects
// referenced by the descriptor. Beware when calling on immutable descriptors:
// this is not thread-safe.
func hydrate(
	ctx context.Context, desc catalog.Descriptor, typeLookupFunc typedesc.TypeLookupFunc,
) error {
	if !isHydratable(desc) {
		return nil
	}
	return typedesc.HydrateTypesInDescriptor(ctx, desc, typeLookupFunc)
}

// makeTypeLookupFuncForHydration builds a typedesc.TypeLookupFunc for the
// use with hydrate. Type descriptors and their parent schema and database are
// looked up in the nstree.Catalog object before being looked up via the
// hydrationLookupFunc.
func makeTypeLookupFuncForHydration(
	c nstree.MutableCatalog, lookupFn hydrationLookupFunc,
) typedesc.TypeLookupFunc {
	var typeLookupFunc func(ctx context.Context, id descpb.ID) (tn tree.TypeName, typ catalog.TypeDescriptor, err error)

	typeLookupFunc = func(ctx context.Context, id descpb.ID) (tn tree.TypeName, typ catalog.TypeDescriptor, err error) {
		typDesc := c.LookupDescriptor(id)
		if typDesc == nil {
			typDesc, err = lookupFn(ctx, id, false /* skipHydration */)
			if err != nil {
				if errors.Is(err, catalog.ErrDescriptorNotFound) {
					n := tree.Name(fmt.Sprintf("[%d]", id))
					return tree.TypeName{}, nil, sqlerrors.NewUndefinedTypeError(&n)
				}
				return tree.TypeName{}, nil, err
			}
			c.UpsertDescriptor(typDesc)
		}
		switch t := typDesc.(type) {
		case catalog.TypeDescriptor:
			typ = t
		case catalog.TableDescriptor:
			if isHydratable(t) {
				if err := hydrate(ctx, t, typeLookupFunc); err != nil {
					return tree.TypeName{}, nil, err
				}
				c.UpsertDescriptor(t)
			}
			typ, err = typedesc.CreateImplicitRecordTypeFromTableDesc(t)
		default:
			typ, err = catalog.AsTypeDescriptor(typDesc)
		}
		if err != nil {
			return tree.TypeName{}, nil, err
		}
		dbDesc := c.LookupDescriptor(typ.GetParentID())
		if dbDesc == nil {
			dbDesc, err = lookupFn(ctx, typ.GetParentID(), true /* skipHydration */)
			if err != nil {
				if errors.Is(err, catalog.ErrDescriptorNotFound) {
					n := fmt.Sprintf("[%d]", typ.GetParentID())
					return tree.TypeName{}, nil, sqlerrors.NewUndefinedDatabaseError(n)
				}
				return tree.TypeName{}, nil, err
			}
			c.UpsertDescriptor(dbDesc)
		}
		if _, err = catalog.AsDatabaseDescriptor(dbDesc); err != nil {
			return tree.TypeName{}, nil, err
		}
		scDesc := c.LookupDescriptor(typ.GetParentSchemaID())
		if scDesc == nil {
			scDesc, err = lookupFn(ctx, typ.GetParentSchemaID(), true /* skipHydration */)
			if err != nil {
				if errors.Is(err, catalog.ErrDescriptorNotFound) {
					n := fmt.Sprintf("[%d]", typ.GetParentSchemaID())
					return tree.TypeName{}, nil, sqlerrors.NewUndefinedSchemaError(n)
				}
				return tree.TypeName{}, nil, err
			}
			c.UpsertDescriptor(scDesc)
		}
		if _, err = catalog.AsSchemaDescriptor(scDesc); err != nil {
			return tree.TypeName{}, nil, err
		}
		tn = tree.MakeQualifiedTypeName(dbDesc.GetName(), scDesc.GetName(), typ.GetName())
		return tn, typ, nil
	}

	return typeLookupFunc
}
