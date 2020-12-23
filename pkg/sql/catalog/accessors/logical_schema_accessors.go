// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package accessors

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// This file provides reference implementations of the schema accessor
// interfaces defined in schema_accessors.go.
//

// NewLogicalAccessor constructs a new accessor given an underlying physical
// accessor and VirtualSchemas.
func NewLogicalAccessor(
	descsCol *descs.Collection, vs catalog.VirtualSchemas,
) *LogicalSchemaAccessor {
	return &LogicalSchemaAccessor{
		tc: descsCol,
		vs: vs,
	}
}

// LogicalSchemaAccessor extends an existing DatabaseLister with the
// ability to list tables in a virtual schema.
type LogicalSchemaAccessor struct {
	tc *descs.Collection
	vs catalog.VirtualSchemas
}

// GetDatabaseDesc implements the Accessor interface.
//
// Warning: This method uses no virtual schema information and only exists to
// accommodate the existing resolver.SchemaResolver interface (see #58228).
// Use GetMutableDatabaseByName() and GetImmutableDatabaseByName() on
// descs.Collection instead when possible.
func (l *LogicalSchemaAccessor) GetDatabaseDesc(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	name string,
	flags tree.DatabaseLookupFlags,
) (desc catalog.DatabaseDescriptor, err error) {
	var found bool
	if flags.RequireMutable {
		found, desc, err = l.tc.GetMutableDatabaseByName(ctx, txn, name, flags)
	} else {
		found, desc, err = l.tc.GetImmutableDatabaseByName(ctx, txn, name, flags)
	}
	if err != nil || !found {
		return nil, err
	}
	return desc, err
}

// GetSchema implements the Accessor interface.
func (l *LogicalSchemaAccessor) GetSchema(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	dbID descpb.ID,
	scName string,
	flags tree.SchemaLookupFlags,
) (bool, catalog.ResolvedSchema, error) {
	if _, ok := l.vs.GetVirtualSchema(scName); ok {
		return true, catalog.ResolvedSchema{Kind: catalog.SchemaVirtual, Name: scName}, nil
	}

	// Fallthrough.
	if flags.RequireMutable {
		return l.tc.GetMutableSchemaByName(ctx, txn, dbID, scName, flags)
	}
	return l.tc.GetImmutableSchemaByName(ctx, txn, dbID, scName, flags)
}

// GetObjectNames implements the DatabaseLister interface.
func (l *LogicalSchemaAccessor) GetObjectNames(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	dbDesc catalog.DatabaseDescriptor,
	scName string,
	flags tree.DatabaseListFlags,
) (tree.TableNames, error) {
	if entry, ok := l.vs.GetVirtualSchema(scName); ok {
		names := make(tree.TableNames, 0, entry.NumTables())
		schemaDesc := entry.Desc()
		entry.VisitTables(func(table catalog.VirtualObject) {
			name := tree.MakeTableNameWithSchema(
				tree.Name(dbDesc.GetName()), tree.Name(schemaDesc.GetName()), tree.Name(table.Desc().GetName()))
			name.ExplicitCatalog = flags.ExplicitPrefix
			name.ExplicitSchema = flags.ExplicitPrefix
			names = append(names, name)
		})
		return names, nil
	}

	// Fallthrough.
	return l.tc.GetObjectNames(ctx, txn, dbDesc, scName, flags)
}

// GetObjectDesc implements the ObjectAccessor interface.
func (l *LogicalSchemaAccessor) GetObjectDesc(
	ctx context.Context,
	txn *kv.Txn,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	db, schema, object string,
	flags tree.ObjectLookupFlags,
) (desc catalog.Descriptor, err error) {
	if scEntry, ok := l.vs.GetVirtualSchema(schema); ok {
		desc, err := scEntry.GetObjectByName(object, flags)
		if err != nil {
			return nil, err
		}
		if desc == nil {
			if flags.Required {
				obj := tree.NewQualifiedObjectName(db, schema, object, flags.DesiredObjectKind)
				return nil, sqlerrors.NewUndefinedObjectError(obj, flags.DesiredObjectKind)
			}
			return nil, nil
		}
		if flags.RequireMutable {
			return nil, newMutableAccessToVirtualSchemaError(scEntry, object)
		}
		return desc.Desc(), nil
	}

	// Resolve type aliases which are usually available in the PostgreSQL as an extension
	// on the public schema.
	if schema == tree.PublicSchema && flags.DesiredObjectKind == tree.TypeObject {
		if alias, ok := types.PublicSchemaAliases[object]; ok {
			if flags.RequireMutable {
				return nil, errors.Newf("cannot use mutable descriptor of aliased type %s.%s", schema, object)
			}
			return typedesc.MakeSimpleAlias(alias, keys.PublicSchemaID), nil
		}
	}

	// Fall back to physical descriptor access.
	var found bool
	switch flags.DesiredObjectKind {
	case tree.TypeObject:
		typeName := tree.MakeNewQualifiedTypeName(db, schema, object)
		if flags.RequireMutable {
			found, desc, err = l.tc.GetMutableTypeByName(ctx, txn, &typeName, flags)
		} else {
			found, desc, err = l.tc.GetImmutableTypeByName(ctx, txn, &typeName, flags)
		}
	case tree.TableObject:
		tableName := tree.MakeTableNameWithSchema(tree.Name(db), tree.Name(schema), tree.Name(object))
		if flags.RequireMutable {
			found, desc, err = l.tc.GetMutableTableByName(ctx, txn, &tableName, flags)
		} else {
			found, desc, err = l.tc.GetImmutableTableByName(ctx, txn, &tableName, flags)
		}
	default:
		return nil, errors.AssertionFailedf("unknown desired object kind %d", flags.DesiredObjectKind)
	}
	if err != nil || !found {
		return nil, err
	}
	return desc, nil
}

func newMutableAccessToVirtualSchemaError(entry catalog.VirtualSchema, object string) error {
	switch entry.Desc().GetName() {
	case "pg_catalog":
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s is a system catalog", tree.ErrNameString(object))
	default:
		return pgerror.Newf(pgcode.WrongObjectType,
			"%s is a virtual object and cannot be modified", tree.ErrNameString(object))
	}
}
