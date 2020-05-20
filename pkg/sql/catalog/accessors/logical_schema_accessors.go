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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	// Used to avoid allocations.
	tn tree.TableName
}

var _ catalog.Accessor = &LogicalSchemaAccessor{}

// IsValidSchema implements the DatabaseLister interface.
func (l *LogicalSchemaAccessor) IsValidSchema(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, dbID sqlbase.ID, scName string,
) (bool, sqlbase.ID, error) {
	if _, ok := l.vs.GetVirtualSchema(scName); ok {
		return true, sqlbase.InvalidID, nil
	}

	// Fallthrough.
	return l.Accessor.IsValidSchema(ctx, txn, codec, dbID, scName)
}

// GetObjectNames implements the DatabaseLister interface.
func (l *LogicalSchemaAccessor) GetObjectNames(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	dbDesc *sqlbase.DatabaseDescriptor,
	scName string,
	flags tree.DatabaseListFlags,
) (tree.TableNames, error) {

	if entry, ok := l.vs.GetVirtualSchema(scName); ok {
		names := make(tree.TableNames, 0, entry.NumTables())
		desc := entry.Desc().TableDesc()
		entry.VisitTables(func(table catalog.VirtualObject) {
			name := tree.MakeTableNameWithSchema(
				tree.Name(dbDesc.Name), tree.Name(desc.Name), tree.Name(table.Desc().TableDesc().Name))
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
		desc, err := scEntry.GetObjectByName(object, flags.DesiredObjectKind)
		if err != nil {
			return nil, err
		}
		if desc == nil {
			if flags.Required {
				l.tn = tree.MakeTableNameWithSchema(tree.Name(db), tree.Name(schema), tree.Name(object))
				return nil, sqlbase.NewUndefinedObjectError(&l.tn, flags.DesiredObjectKind)
			}
			return nil, nil
		}
		return desc.Desc(), nil
	}
	// Fallthrough.
	return l.Accessor.GetObjectDesc(ctx, txn, settings, codec, db, schema, object, flags)
}
