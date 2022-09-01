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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// GetObjectByName looks up an object by name and returns both its
// descriptor and that of its parent database and schema.
//
// If the object is not found and flags.required is true, an error is returned,
// otherwise a nil reference is returned.
func (tc *Collection) GetObjectByName(
	ctx context.Context,
	txn *kv.Txn,
	catalogName, schemaName, objectName string,
	flags tree.ObjectLookupFlags,
) (prefix catalog.ResolvedObjectPrefix, desc catalog.Descriptor, err error) {
	prefix, err = tc.getObjectPrefixByName(ctx, txn, catalogName, schemaName, flags)
	if err != nil || prefix.Schema == nil {
		return prefix, nil, err
	}
	// Read object descriptor and handle errors and absence.
	{
		var requestedType catalog.DescriptorType
		switch flags.DesiredObjectKind {
		case tree.TableObject:
			requestedType = catalog.Table
		case tree.TypeObject:
			requestedType = catalog.Type
		default:
			return prefix, nil, errors.AssertionFailedf(
				"unknown DesiredObjectKind value %v", flags.DesiredObjectKind)
		}
		desc, err = tc.getDescriptorByName(
			ctx, txn, prefix.Database, prefix.Schema, objectName, flags.CommonLookupFlags, requestedType,
		)
		if err != nil {
			return prefix, nil, err
		}
		if desc == nil {
			if flags.Required {
				tn := tree.MakeTableNameWithSchema(
					tree.Name(catalogName),
					tree.Name(schemaName),
					tree.Name(objectName))
				return prefix, nil, sqlerrors.NewUndefinedRelationError(&tn)
			}
			return prefix, nil, nil
		}
	}
	// At this point the descriptor is not nil.
	switch t := desc.(type) {
	case catalog.TableDescriptor:
		// A given table name can resolve to either a type descriptor or a table
		// descriptor, because every table descriptor also defines an implicit
		// record type with the same name as the table. Thus, depending on the
		// requested descriptor type, we return either the table descriptor itself,
		// or the table descriptor's implicit record type.
		switch flags.DesiredObjectKind {
		case tree.TableObject, tree.TypeObject:
		default:
			return prefix, nil, nil
		}
		if flags.DesiredObjectKind == tree.TypeObject {
			// Since a type descriptor was requested, we need to return the implicitly
			// created record type for the table that we found.
			if flags.RequireMutable {
				// ... but, we can't do it if we need a mutable descriptor - we don't
				// have the capability of returning a mutable type descriptor for a
				// table's implicit record type.
				return prefix, nil, pgerror.Newf(pgcode.InsufficientPrivilege,
					"cannot modify table record type %q", objectName)
			}
			desc, err = typedesc.CreateImplicitRecordTypeFromTableDesc(t)
			if err != nil {
				return prefix, nil, err
			}
		}
	case catalog.TypeDescriptor:
		if flags.DesiredObjectKind != tree.TypeObject {
			return prefix, nil, nil
		}
	default:
		return prefix, nil, errors.AssertionFailedf(
			"unexpected object of type %T", t,
		)
	}
	return prefix, desc, nil
}

func (tc *Collection) getObjectPrefixByName(
	ctx context.Context, txn *kv.Txn, catalogName, schemaName string, objFlags tree.ObjectLookupFlags,
) (prefix catalog.ResolvedObjectPrefix, err error) {
	// If we're reading the object descriptor from the store,
	// we should read its parents from the store too to ensure
	// that subsequent name resolution finds the latest name
	// in the face of a concurrent rename.
	flags := tree.CommonLookupFlags{
		Required:       objFlags.Required,
		AvoidLeased:    objFlags.AvoidLeased || objFlags.RequireMutable,
		IncludeDropped: objFlags.IncludeDropped,
		IncludeOffline: objFlags.IncludeOffline,
	}
	if catalogName != "" {
		prefix.Database, err = tc.GetImmutableDatabaseByName(ctx, txn, catalogName, flags)
		if err != nil {
			return prefix, err
		}
		if prefix.Database == nil {
			if flags.Required {
				return prefix, sqlerrors.NewUndefinedDatabaseError(catalogName)
			}
			return prefix, nil
		}
	}
	prefix.Schema, err = tc.GetImmutableSchemaByName(ctx, txn, prefix.Database, schemaName, flags)
	if err != nil {
		return prefix, err
	}
	if prefix.Schema == nil {
		if flags.Required {
			return prefix, sqlerrors.NewUndefinedSchemaError(schemaName)
		}
		return prefix, nil
	}
	return prefix, nil
}
