// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package hydrateddesc

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// HydrationLookupFunc is the type of function required to look up type
// descriptors and their parent schemas and databases when hydrating an object.
type HydrationLookupFunc func(ctx context.Context, id descpb.ID, skipHydration bool) (catalog.Descriptor, error)

// IsHydratable returns false iff the descriptor definitely does not require
// hydration
func IsHydratable(desc catalog.Descriptor) bool {
	if desc.Dropped() {
		// Don't hydrate dropped descriptors.
		return false
	}
	hd, ok := desc.(catalog.HydratableDescriptor)
	return ok && hd.ContainsUserDefinedTypes()
}

// Hydrate ensures that type metadata is present in any type.T objects
// referenced by the descriptor. Beware when calling on immutable descriptors:
// this is not thread-safe.
func Hydrate(
	ctx context.Context, desc catalog.Descriptor, typeLookupFunc typedesc.TypeLookupFunc,
) error {
	if !IsHydratable(desc) {
		return nil
	}

	switch t := desc.(type) {
	case catalog.TableDescriptor:
		return typedesc.HydrateTypesInTableDescriptor(ctx, t.TableDesc(), typeLookupFunc)
	case catalog.SchemaDescriptor:
		return typedesc.HydrateTypesInSchemaDescriptor(ctx, t.SchemaDesc(), typeLookupFunc)
	case catalog.FunctionDescriptor:
		return typedesc.HydrateTypesInFunctionDescriptor(ctx, t.FuncDesc(), typeLookupFunc)
	}
	return errors.AssertionFailedf("unknown hydratable type %T", desc)
}

// MakeTypeLookupFuncForHydration builds a typedesc.TypeLookupFunc for the
// use with Hydrate. Type descriptors and their parent schema and database are
// looked up in the nstree.Catalog object before being looked up via the
// HydrationLookupFunc.
func MakeTypeLookupFuncForHydration(
	c nstree.Catalog, lookupFn HydrationLookupFunc,
) typedesc.TypeLookupFunc {
	return func(ctx context.Context, id descpb.ID) (tn tree.TypeName, typ catalog.TypeDescriptor, err error) {
		typDesc := c.LookupDescriptor(id)
		// If the type in the fast lookup catalog is a table implicit record type
		// and  not fully hydrated, we need to make sure we get a hydrated version
		// of it.
		// TODO(chengxiong): we may also do topological sort on the descritpors need
		// to be hydrated and hydrated them in order. In the mean while, we need to
		// upsert the hydrated version into the fast lookup catalog. This may save
		// some round trips.
		if typDesc == nil ||
			(typDesc.DescriptorType() == catalog.Table && !typDesc.(catalog.TableDescriptor).Hydrated()) {

			typDesc, err = lookupFn(ctx, id, false /* skipHydration */)
			if err != nil {
				if errors.Is(err, catalog.ErrDescriptorNotFound) {
					n := tree.Name(fmt.Sprintf("[%d]", id))
					return tree.TypeName{}, nil, sqlerrors.NewUndefinedTypeError(&n)
				}
				return tree.TypeName{}, nil, err
			}
		}
		switch t := typDesc.(type) {
		case catalog.TypeDescriptor:
			typ = t
		case catalog.TableDescriptor:
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
		}
		if _, err = catalog.AsSchemaDescriptor(scDesc); err != nil {
			return tree.TypeName{}, nil, err
		}
		tn = tree.MakeQualifiedTypeName(dbDesc.GetName(), scDesc.GetName(), typ.GetName())
		return tn, typ, nil
	}
}
