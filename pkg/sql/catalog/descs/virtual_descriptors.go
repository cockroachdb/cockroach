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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

type virtualDescriptors struct {
	vs catalog.VirtualSchemas
}

func makeVirtualDescriptors(schemas catalog.VirtualSchemas) virtualDescriptors {
	return virtualDescriptors{vs: schemas}
}

func (tc *virtualDescriptors) getSchemaByName(schemaName string) catalog.SchemaDescriptor {
	if tc.vs == nil {
		return nil
	}
	if sc, ok := tc.vs.GetVirtualSchema(schemaName); ok {
		return sc.Desc()
	}
	return nil
}

func (tc *virtualDescriptors) getObjectByName(
	schema string, object string, flags tree.ObjectLookupFlags, db string,
) (isVirtual bool, _ catalog.Descriptor, _ error) {
	if tc.vs == nil {
		return false, nil, nil
	}
	scEntry, ok := tc.vs.GetVirtualSchema(schema)
	if !ok {
		return false, nil, nil
	}
	desc, err := scEntry.GetObjectByName(object, flags)
	if err != nil {
		return true, nil, err
	}
	if desc == nil {
		if flags.Required {
			obj := tree.NewQualifiedObjectName(db, schema, object, flags.DesiredObjectKind)
			return true, nil, sqlerrors.NewUndefinedObjectError(obj, flags.DesiredObjectKind)
		}
		return true, nil, nil
	}
	if flags.RequireMutable {
		return true, nil, catalog.NewMutableAccessToVirtualSchemaError(scEntry, object)
	}
	return true, desc.Desc(), nil
}

func (tc virtualDescriptors) getByID(
	ctx context.Context, id descpb.ID, mutable bool,
) (catalog.Descriptor, error) {
	if tc.vs == nil {
		return nil, nil
	}
	if vd, found := tc.vs.GetVirtualObjectByID(id); found {
		if mutable {
			vs, found := tc.vs.GetVirtualSchemaByID(vd.Desc().GetParentSchemaID())
			if !found {
				return nil, errors.AssertionFailedf(
					"cannot resolve mutable virtual descriptor %d with unknown parent schema %d",
					id, vd.Desc().GetParentSchemaID(),
				)
			}
			return nil, catalog.NewMutableAccessToVirtualSchemaError(vs, vd.Desc().GetName())
		}
		return vd.Desc(), nil
	}
	return tc.getSchemaByID(ctx, id, mutable)
}

func (tc virtualDescriptors) getSchemaByID(
	ctx context.Context, id descpb.ID, mutable bool,
) (catalog.SchemaDescriptor, error) {
	if tc.vs == nil {
		return nil, nil
	}
	vs, found := tc.vs.GetVirtualSchemaByID(id)
	switch {
	case !found:
		return nil, nil
	case mutable:
		return nil, catalog.NewMutableAccessToVirtualSchemaError(vs, vs.Desc().GetName())
	default:
		return vs.Desc(), nil
	}
}

func (tc virtualDescriptors) maybeGetObjectNamesAndIDs(
	scName string, dbDesc catalog.DatabaseDescriptor, flags tree.DatabaseListFlags,
) (isVirtual bool, _ tree.TableNames, _ descpb.IDs) {
	if tc.vs == nil {
		return false, nil, nil
	}
	entry, ok := tc.vs.GetVirtualSchema(scName)
	if !ok {
		return false, nil, nil
	}
	names := make(tree.TableNames, 0, entry.NumTables())
	IDs := make(descpb.IDs, 0, entry.NumTables())
	schemaDesc := entry.Desc()
	entry.VisitTables(func(table catalog.VirtualObject) {
		name := tree.MakeTableNameWithSchema(
			tree.Name(dbDesc.GetName()), tree.Name(schemaDesc.GetName()), tree.Name(table.Desc().GetName()))
		name.ExplicitCatalog = flags.ExplicitPrefix
		name.ExplicitSchema = flags.ExplicitPrefix
		names = append(names, name)
		IDs = append(IDs, table.Desc().GetID())
	})
	return true, names, IDs
}
