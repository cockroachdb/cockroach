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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// hydrateTypesInTableDesc installs user defined type metadata in all types.T
// present in the input TableDescriptor. See hydrateTypesInDescWithOptions.
func (tc *Collection) hydrateTypesInTableDesc(
	ctx context.Context, txn *kv.Txn, desc catalog.TableDescriptor,
) (catalog.TableDescriptor, error) {
	ret, err := tc.hydrateTypesInDescWithOptions(ctx,
		txn,
		desc,
		false, /* includeOffline */
		false /*avoidLeased*/)
	return ret.(catalog.TableDescriptor), err
}

// hydrateTypesInDescWithOptions installs user defined type metadata in all
// types.T present in the input Descriptor. It always returns the same type of
// Descriptor that was passed in. It ensures that immutable Descriptors are not
// modified during the process of metadata installation. Dropped descriptors do
// not get hydrated. Optionally, when hydrating types we can include offline
// descriptors and avoid leasing depending on the context.
func (tc *Collection) hydrateTypesInDescWithOptions(
	ctx context.Context,
	txn *kv.Txn,
	desc catalog.HydratableDescriptor,
	includeOffline bool,
	avoidLeased bool,
) (catalog.HydratableDescriptor, error) {
	if desc.Dropped() {
		return desc, nil
	}
	// If there aren't any user defined types in the descriptor, then return
	// early.
	if !desc.ContainsUserDefinedTypes() {
		return desc, nil
	}

	// It is safe to hydrate directly into Mutable since it is not shared. When
	// hydrating mutable descriptors, use the mutable access method to access
	// types.
	sc, _ := desc.(catalog.SchemaDescriptor)
	switch t := desc.(type) {
	case *tabledesc.Mutable:
		return desc, typedesc.HydrateTypesInTableDescriptor(ctx, t.TableDesc(), getMutableTypeLookupFunc(tc, txn, sc))
	case *schemadesc.Mutable:
		return desc, typedesc.HydrateTypesInSchemaDescriptor(ctx, t.SchemaDesc(), getMutableTypeLookupFunc(tc, txn, sc))
	case *funcdesc.Mutable:
		return desc, typedesc.HydrateTypesInFunctionDescriptor(ctx, t.FuncDesc(), getMutableTypeLookupFunc(tc, txn, sc))
	default:
		// Utilize the cache of hydrated tables if we have one and this descriptor
		// was leased.
		// TODO(ajwerner): Consider surfacing the mechanism used to retrieve the
		// descriptor up to this layer.
		if tc.canUseHydratedDescriptorCache(desc.GetID()) {
			hydrated, err := tc.hydrated.GetHydratedDescriptor(
				ctx, t, getImmutableTypeLookupFunc(tc, txn, includeOffline, avoidLeased, sc),
			)
			if err != nil {
				return nil, err
			}
			if hydrated != nil {
				return hydrated, nil
			}
		}
		// The cache decided not to give back a hydrated descriptor, likely
		// because either we've modified the table or one of the types or because
		// this transaction has a stale view of one of the relevant descriptors.
		// Proceed to hydrating a fresh copy.
	}

	// ImmutableTableDescriptors need to be copied before hydration, because
	// they are potentially read by multiple threads.
	getType := getImmutableTypeLookupFunc(tc, txn, includeOffline, avoidLeased, sc)
	switch t := desc.(type) {
	case catalog.TableDescriptor:
		mut := t.NewBuilder().BuildExistingMutable().(*tabledesc.Mutable)
		if err := typedesc.HydrateTypesInTableDescriptor(ctx, mut.TableDesc(), getType); err != nil {
			return nil, err
		}
		return mut.ImmutableCopy().(catalog.TableDescriptor), nil
	case catalog.SchemaDescriptor:
		mut := t.NewBuilder().BuildExistingMutable().(*schemadesc.Mutable)
		if err := typedesc.HydrateTypesInSchemaDescriptor(ctx, mut.SchemaDesc(), getType); err != nil {
			return nil, err
		}
		return mut.ImmutableCopy().(catalog.SchemaDescriptor), nil
	case catalog.FunctionDescriptor:
		mut := t.NewBuilder().BuildExistingMutable().(*funcdesc.Mutable)
		if err := typedesc.HydrateTypesInFunctionDescriptor(ctx, mut.FuncDesc(), getType); err != nil {
			return nil, err
		}
		return mut.ImmutableCopy().(catalog.FunctionDescriptor), nil
	}

	return desc, nil
}

// HydrateGivenDescriptors installs type metadata in the types present for all
// table descriptors in the slice of descriptors. It is exported so resolution
// on sets of descriptors can hydrate a set of descriptors (i.e. on BACKUPs).
func HydrateGivenDescriptors(ctx context.Context, descs []catalog.Descriptor) error {
	// Collect the needed information to set up metadata in those types.
	dbDescs := make(map[descpb.ID]catalog.DatabaseDescriptor)
	typDescs := make(map[descpb.ID]catalog.TypeDescriptor)
	schemaDescs := make(map[descpb.ID]catalog.SchemaDescriptor)
	for _, desc := range descs {
		switch desc := desc.(type) {
		case catalog.DatabaseDescriptor:
			dbDescs[desc.GetID()] = desc
		case catalog.TypeDescriptor:
			typDescs[desc.GetID()] = desc
		case catalog.SchemaDescriptor:
			schemaDescs[desc.GetID()] = desc
		}
	}
	// If we found any type descriptors, that means that some of the tables we
	// scanned might have types that need hydrating.
	if len(typDescs) > 0 {
		// Since we just scanned all the descriptors, we already have everything
		// we need to hydrate our types. Set up an accessor for the type hydration
		// method to look into the scanned set of descriptors.
		typeLookup := func(ctx context.Context, id descpb.ID) (tree.TypeName, catalog.TypeDescriptor, error) {
			typDesc, ok := typDescs[id]
			if !ok {
				n := tree.MakeUnresolvedName(fmt.Sprintf("[%d]", id))
				return tree.TypeName{}, nil, sqlerrors.NewUndefinedObjectError(&n,
					tree.TypeObject)
			}
			dbDesc, ok := dbDescs[typDesc.GetParentID()]
			if !ok {
				n := fmt.Sprintf("[%d]", typDesc.GetParentID())
				return tree.TypeName{}, nil, sqlerrors.NewUndefinedDatabaseError(n)
			}
			// We don't use the collection's ResolveSchemaByID method here because
			// we already have all of the descriptors. User defined types are only
			// members of the public schema or a user defined schema, so those are
			// the only cases we have to consider here.
			var scName string
			switch typDesc.GetParentSchemaID() {
			// TODO(richardjcai): Remove case for keys.PublicSchemaID in 22.2.
			case keys.PublicSchemaID:
				scName = tree.PublicSchema
			default:
				scName = schemaDescs[typDesc.GetParentSchemaID()].GetName()
			}
			name := tree.MakeQualifiedTypeName(dbDesc.GetName(), scName, typDesc.GetName())
			return name, typDesc, nil
		}
		// Now hydrate all table descriptors.
		for i := range descs {
			desc := descs[i]
			// Never hydrate dropped descriptors.
			if desc.Dropped() {
				continue
			}

			var err error
			switch t := desc.(type) {
			case catalog.TableDescriptor:
				err = typedesc.HydrateTypesInTableDescriptor(ctx, t.TableDesc(), typedesc.TypeLookupFunc(typeLookup))
			case catalog.FunctionDescriptor:
				err = typedesc.HydrateTypesInFunctionDescriptor(ctx, t.FuncDesc(), typedesc.TypeLookupFunc(typeLookup))
			case catalog.SchemaDescriptor:
				err = typedesc.HydrateTypesInSchemaDescriptor(ctx, t.SchemaDesc(), typedesc.TypeLookupFunc(typeLookup))
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// hydrateCatalog installs type metadata in the types present for all
// table descriptors in a catalog.
func hydrateCatalog(ctx context.Context, cat nstree.Catalog) error {
	// Since we have a catalog, we already have everything we need to hydrate our
	// types. Set up an accessor for the type hydration method to look into the
	// catalog.
	typeLookup := func(ctx context.Context, id descpb.ID) (tree.TypeName, catalog.TypeDescriptor, error) {
		desc := cat.LookupDescriptorEntry(id)
		if desc == nil {
			n := tree.MakeUnresolvedName(fmt.Sprintf("[%d]", id))
			return tree.TypeName{}, nil, sqlerrors.NewUndefinedObjectError(&n,
				tree.TypeObject)
		}
		typDesc, ok := desc.(catalog.TypeDescriptor)
		if !ok {
			return tree.TypeName{}, nil, errors.Newf(
				"found %s while looking for type [%d]", desc.DescriptorType(), id,
			)
		}
		dbDesc := cat.LookupDescriptorEntry(typDesc.GetParentID())
		if dbDesc == nil {
			n := fmt.Sprintf("[%d]", typDesc.GetParentID())
			return tree.TypeName{}, nil, sqlerrors.NewUndefinedDatabaseError(n)
		}
		scDesc := cat.LookupDescriptorEntry(typDesc.GetParentSchemaID())
		if scDesc == nil {
			n := fmt.Sprintf("[%d]", typDesc.GetParentSchemaID())
			return tree.TypeName{}, nil, sqlerrors.NewUndefinedSchemaError(n)
		}
		name := tree.MakeQualifiedTypeName(dbDesc.GetName(), scDesc.GetName(), typDesc.GetName())
		return name, typDesc, nil
	}
	// Now hydrate all table descriptors.
	return cat.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
		// Never hydrate dropped descriptors.
		if desc.Dropped() {
			return nil
		}

		var err error
		switch t := desc.(type) {
		case catalog.TableDescriptor:
			err = typedesc.HydrateTypesInTableDescriptor(ctx, t.TableDesc(), typedesc.TypeLookupFunc(typeLookup))
		case catalog.FunctionDescriptor:
			err = typedesc.HydrateTypesInFunctionDescriptor(ctx, t.FuncDesc(), typedesc.TypeLookupFunc(typeLookup))
		case catalog.SchemaDescriptor:
			err = typedesc.HydrateTypesInSchemaDescriptor(ctx, t.SchemaDesc(), typedesc.TypeLookupFunc(typeLookup))
		}
		if err != nil {
			return err
		}
		return nil
	})
}

func (tc *Collection) canUseHydratedDescriptorCache(id descpb.ID) bool {
	return tc.hydrated != nil &&
		tc.stored.descs.GetByID(id) == nil &&
		tc.synthetic.descs.GetByID(id) == nil
}

func getMutableTypeLookupFunc(
	tc *Collection, txn *kv.Txn, schema catalog.SchemaDescriptor,
) typedesc.TypeLookupFunc {
	return func(ctx context.Context, id descpb.ID) (tree.TypeName, catalog.TypeDescriptor, error) {
		// Note that getting mutable table implicit type is not allowed. To
		// hydrate table implicit types, we don't really need a mutable type
		// descriptor since we are not going to mutate the table because we simply
		// need the tuple type and some metadata. So it's adequate here to get a
		// fresh immutable.
		flags := tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				Required:       true,
				IncludeDropped: true,
				IncludeOffline: true,
				AvoidLeased:    true,
			},
		}
		typDesc, err := tc.GetImmutableTypeByID(ctx, txn, id, flags)
		if err != nil {
			return tree.TypeName{}, nil, err
		}

		_, dbDesc, err := tc.GetImmutableDatabaseByID(
			ctx, txn, typDesc.GetParentID(),
			tree.DatabaseLookupFlags{
				Required:       true,
				IncludeDropped: true,
				IncludeOffline: true,
				AvoidLeased:    true,
			},
		)
		if err != nil {
			return tree.TypeName{}, nil, err
		}

		var scName string
		if schema != nil {
			scName = schema.GetName()
		} else {
			sc, err := tc.getSchemaByID(
				ctx, txn, typDesc.GetParentSchemaID(),
				tree.SchemaLookupFlags{
					Required:       true,
					IncludeDropped: true,
					IncludeOffline: true,
					AvoidLeased:    true,
				},
			)
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			scName = sc.GetName()
		}
		name := tree.MakeQualifiedTypeName(dbDesc.GetName(), scName, typDesc.GetName())
		return name, typDesc, nil
	}
}

func getImmutableTypeLookupFunc(
	tc *Collection,
	txn *kv.Txn,
	includeOffline bool,
	avoidLeased bool,
	schema catalog.SchemaDescriptor,
) typedesc.TypeLookupFunc {
	// "schema" is optional, it's needed only when a schema is being hydrated.
	// This is a hack to prevent a dead loop in which a schema need to be type
	// hydrated and, to qualify the type name within the same schema, we need the
	// same schema itself (to get the schema itself, type hydration is required as
	// well, and here is the dead loop). Since we already know the schema, we just
	// use the name from the descriptor.

	return func(ctx context.Context, id descpb.ID) (tree.TypeName, catalog.TypeDescriptor, error) {
		desc, err := tc.GetImmutableTypeByID(ctx, txn, id, tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				Required:       true,
				AvoidSynthetic: true,
				IncludeOffline: includeOffline,
				AvoidLeased:    avoidLeased,
			},
		})
		if err != nil {
			return tree.TypeName{}, nil, err
		}
		_, dbDesc, err := tc.GetImmutableDatabaseByID(ctx, txn, desc.GetParentID(),
			tree.DatabaseLookupFlags{
				Required:       true,
				AvoidSynthetic: true,
				IncludeOffline: includeOffline,
				AvoidLeased:    avoidLeased,
			})
		if err != nil {
			return tree.TypeName{}, nil, err
		}

		var scName string
		if schema != nil && schema.GetID() == desc.GetParentSchemaID() {
			scName = schema.GetName()
		} else {
			sc, err := tc.GetImmutableSchemaByID(
				ctx, txn, desc.GetParentSchemaID(), tree.SchemaLookupFlags{
					Required:       true,
					AvoidSynthetic: true,
					IncludeOffline: includeOffline,
					AvoidLeased:    avoidLeased,
				})
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			scName = sc.GetName()
		}
		name := tree.MakeQualifiedTypeName(dbDesc.GetName(), scName, desc.GetName())
		return name, desc, nil
	}
}
