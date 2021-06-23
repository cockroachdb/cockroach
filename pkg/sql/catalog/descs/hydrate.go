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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// hydrateTypesInTableDesc installs user defined type metadata in all types.T
// present in the input TableDescriptor. It always returns the same type of
// TableDescriptor that was passed in. It ensures that ImmutableTableDescriptors
// are not modified during the process of metadata installation. Dropped tables
// do not get hydrated.
//
// TODO(ajwerner): This should accept flags to indicate whether we can resolve
// offline descriptors.
func (tc *Collection) hydrateTypesInTableDesc(
	ctx context.Context, txn *kv.Txn, desc catalog.TableDescriptor,
) (catalog.TableDescriptor, error) {
	if desc.Dropped() {
		return desc, nil
	}
	switch t := desc.(type) {
	case *tabledesc.Mutable:
		// It is safe to hydrate directly into Mutable since it is
		// not shared. When hydrating mutable descriptors, use the mutable access
		// method to access types.
		getType := func(ctx context.Context, id descpb.ID) (tree.TypeName, catalog.TypeDescriptor, error) {
			desc, err := tc.GetMutableTypeVersionByID(ctx, txn, id)
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			dbDesc, err := tc.GetMutableDescriptorByID(ctx, desc.ParentID, txn)
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			sc, err := tc.getSchemaByID(
				ctx, txn, desc.ParentSchemaID,
				tree.SchemaLookupFlags{
					IncludeOffline: true,
					RequireMutable: true,
				},
			)
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			name := tree.MakeQualifiedTypeName(dbDesc.GetName(), sc.GetName(), desc.Name)
			return name, desc, nil
		}

		return desc, typedesc.HydrateTypesInTableDescriptor(ctx, t.TableDesc(), typedesc.TypeLookupFunc(getType))
	case catalog.TableDescriptor:
		// ImmutableTableDescriptors need to be copied before hydration, because
		// they are potentially read by multiple threads. If there aren't any user
		// defined types in the descriptor, then return early.
		if !t.ContainsUserDefinedTypes() {
			return desc, nil
		}

		getType := typedesc.TypeLookupFunc(func(
			ctx context.Context, id descpb.ID,
		) (tree.TypeName, catalog.TypeDescriptor, error) {
			desc, err := tc.GetImmutableTypeByID(ctx, txn, id, tree.ObjectLookupFlags{})
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			_, dbDesc, err := tc.GetImmutableDatabaseByID(ctx, txn, desc.GetParentID(),
				tree.DatabaseLookupFlags{Required: true})
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			sc, err := tc.GetImmutableSchemaByID(
				ctx, txn, desc.GetParentSchemaID(), tree.SchemaLookupFlags{})
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			name := tree.MakeQualifiedTypeName(dbDesc.GetName(), sc.GetName(), desc.GetName())
			return name, desc, nil
		})

		// Utilize the cache of hydrated tables if we have one.
		if tc.hydratedTables != nil {
			hydrated, err := tc.hydratedTables.GetHydratedTableDescriptor(ctx, t, getType)
			if err != nil {
				return nil, err
			}
			if hydrated != nil {
				return hydrated, nil
			}
			// The cache decided not to give back a hydrated descriptor, likely
			// because either we've modified the table or one of the types or because
			// this transaction has a stale view of one of the relevant descriptors.
			// Proceed to hydrating a fresh copy.
		}

		// Make a copy of the underlying descriptor before hydration.
		descBase := protoutil.Clone(t.TableDesc()).(*descpb.TableDescriptor)
		if err := typedesc.HydrateTypesInTableDescriptor(ctx, descBase, getType); err != nil {
			return nil, err
		}
		if t.IsUncommittedVersion() {
			return tabledesc.NewBuilderForUncommittedVersion(descBase).BuildImmutableTable(), nil
		}
		return tabledesc.NewBuilder(descBase).BuildImmutableTable(), nil
	default:
		return desc, nil
	}
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
			tblDesc, ok := desc.(catalog.TableDescriptor)
			if ok {
				if err := typedesc.HydrateTypesInTableDescriptor(
					ctx,
					tblDesc.TableDesc(),
					typedesc.TypeLookupFunc(typeLookup),
				); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
