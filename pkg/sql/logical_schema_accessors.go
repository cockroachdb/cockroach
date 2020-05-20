// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// This file provides reference implementations of the schema accessor
// interfaces defined in schema_accessors.go.
//

// LogicalSchemaAccessor extends an existing DatabaseLister with the
// ability to list tables in a virtual schema.
type LogicalSchemaAccessor struct {
	catalog.Accessor
	vt VirtualTabler
	// Used to avoid allocations.
	tn TableName
}

var _ catalog.Accessor = &LogicalSchemaAccessor{}

// IsValidSchema implements the DatabaseLister interface.
func (l *LogicalSchemaAccessor) IsValidSchema(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, dbID sqlbase.ID, scName string,
) (bool, sqlbase.ID, error) {
	if _, ok := l.vt.getVirtualSchemaEntry(scName); ok {
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
	dbDesc *DatabaseDescriptor,
	scName string,
	flags tree.DatabaseListFlags,
) (TableNames, error) {
	if entry, ok := l.vt.getVirtualSchemaEntry(scName); ok {
		names := make(TableNames, len(entry.orderedDefNames))
		for i, name := range entry.orderedDefNames {
			names[i] = tree.MakeTableNameWithSchema(
				tree.Name(dbDesc.Name), tree.Name(entry.desc.Name), tree.Name(name))
			names[i].ExplicitCatalog = flags.ExplicitPrefix
			names[i].ExplicitSchema = flags.ExplicitPrefix
		}

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
) (catalog.ObjectDescriptor, error) {
	switch flags.DesiredObjectKind {
	case tree.TypeObject:
		return (UncachedPhysicalAccessor{}).GetObjectDesc(ctx, txn, settings, codec, db, schema, object, flags)
	case tree.TableObject:
		l.tn = tree.MakeTableNameWithSchema(tree.Name(db), tree.Name(schema), tree.Name(object))
		if scEntry, ok := l.vt.getVirtualSchemaEntry(l.tn.Schema()); ok {
			tableName := l.tn.Table()
			if t, ok := scEntry.defs[tableName]; ok {
				if flags.RequireMutable {
					return sqlbase.NewMutableExistingTableDescriptor(*t.desc), nil
				}
				return sqlbase.NewImmutableTableDescriptor(*t.desc), nil
			}
			if _, ok := scEntry.allTableNames[tableName]; ok {
				return nil, unimplemented.Newf(l.tn.Schema()+"."+tableName,
					"virtual schema table not implemented: %s.%s", l.tn.Schema(), tableName)
			}

			if flags.Required {
				return nil, sqlbase.NewUndefinedRelationError(&l.tn)
			}
			return nil, nil
		}

		// Fallthrough.
		return l.Accessor.GetObjectDesc(ctx, txn, settings, codec, db, schema, object, flags)
	default:
		return nil, errors.AssertionFailedf("unknown desired object kind %d", flags.DesiredObjectKind)
	}
}
