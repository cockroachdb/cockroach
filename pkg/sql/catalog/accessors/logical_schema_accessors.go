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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// This file provides reference implementations of the schema accessor
// interfaces defined in schema_accessors.go.
//

// NewLogicalAccessor constructs a new accessor given an underlying physical
// accessor and VirtualSchemas.
func NewLogicalAccessor(
	physicalAccessor catalog.Accessor, vs catalog.VirtualSchemas,
) *LogicalSchemaAccessor {
	return &LogicalSchemaAccessor{
		Accessor: physicalAccessor,
		vs:       vs,
	}
}

// LogicalSchemaAccessor extends an existing DatabaseLister with the
// ability to list tables in a virtual schema.
type LogicalSchemaAccessor struct {
	catalog.Accessor
	vs catalog.VirtualSchemas
}

var _ catalog.Accessor = &LogicalSchemaAccessor{}

// GetSchema implements the Accessor interface.
func (l *LogicalSchemaAccessor) GetSchema(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, dbID descpb.ID, scName string,
) (bool, catalog.ResolvedSchema, error) {
	if _, ok := l.vs.GetVirtualSchema(scName); ok {
		return true, catalog.ResolvedSchema{Kind: catalog.SchemaVirtual, Name: scName}, nil
	}

	// Fallthrough.
	return l.Accessor.GetSchema(ctx, txn, codec, dbID, scName)
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
	return l.Accessor.GetObjectNames(ctx, txn, codec, dbDesc, scName, flags)
}

// GetObjectDesc implements the ObjectAccessor interface.
func (l *LogicalSchemaAccessor) GetObjectDesc(
	ctx context.Context,
	txn *kv.Txn,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	db, schema, object string,
	flags tree.ObjectLookupFlags,
) (catalog.Descriptor, error) {
	if scEntry, ok := l.vs.GetVirtualSchema(schema); ok {
		desc, err := scEntry.GetObjectByName(object, flags)
		if err != nil {
			return nil, err
		}
		if desc == nil {
			if flags.Required {
				obj := tree.NewQualifiedObjectName(db, schema, object, flags.DesiredObjectKind)
				return nil, sqlbase.NewUndefinedObjectError(obj, flags.DesiredObjectKind)
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
			return typedesc.MakeSimpleAliasTypeDescriptor(alias), nil
		}
	}

	// Fallthrough.
	return l.Accessor.GetObjectDesc(ctx, txn, settings, codec, db, schema, object, flags)
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
