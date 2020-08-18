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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// This file provides reference implementations of the schema accessor
// interface defined in schema_accessors.go.
//
// They are meant to be used to access stored descriptors only.
// For a higher-level implementation that also knows about
// virtual schemas, check out logical_schema_accessors.go.
//
// The following implementations are provided:
//
// - UncachedPhysicalAccessor, for uncached db accessors
//
// - CachedPhysicalAccessor, which adds an object cache
//   - plugged on top another Accessor.
//   - uses a `*Collection` (table.go) as cache.
//

// NewCachedAccessor constructs a new cached accessor using a physical accessor
// and a descs.Collection.
func NewCachedAccessor(
	physicalAccessor catalog.Accessor, descsCol *descs.Collection,
) *CachedPhysicalAccessor {
	return &CachedPhysicalAccessor{
		Accessor: physicalAccessor,
		tc:       descsCol,
	}
}

// CachedPhysicalAccessor adds a cache on top of any Accessor.
type CachedPhysicalAccessor struct {
	catalog.Accessor
	tc *descs.Collection
	// Used to avoid allocations.
	tableName tree.TableName
	typeName  tree.TypeName
}

var _ catalog.Accessor = &CachedPhysicalAccessor{}

// GetDatabaseDesc implements the Accessor interface.
func (a *CachedPhysicalAccessor) GetDatabaseDesc(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	name string,
	flags tree.DatabaseLookupFlags,
) (desc sqlbase.DatabaseDescriptor, err error) {
	if flags.RequireMutable {
		db, err := a.tc.GetMutableDatabaseDescriptor(ctx, txn, name, flags)
		if db == nil {
			return nil, err
		}
		return db, err
	}
	typ, err := a.tc.GetDatabaseVersion(ctx, txn, name, flags)
	if typ == nil {
		return nil, err
	}
	return typ, err
}

// GetSchema implements the Accessor interface.
func (a *CachedPhysicalAccessor) GetSchema(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, dbID descpb.ID, scName string,
) (bool, sqlbase.ResolvedSchema, error) {
	return a.tc.ResolveSchema(ctx, txn, dbID, scName)
}

// GetObjectDesc implements the Accessor interface.
func (a *CachedPhysicalAccessor) GetObjectDesc(
	ctx context.Context,
	txn *kv.Txn,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	db, schema, object string,
	flags tree.ObjectLookupFlags,
) (catalog.Descriptor, error) {
	switch flags.DesiredObjectKind {
	case tree.TypeObject:
		a.typeName = tree.MakeNewQualifiedTypeName(db, schema, object)
		if flags.RequireMutable {
			typ, err := a.tc.GetMutableTypeDescriptor(ctx, txn, &a.typeName, flags)
			if typ == nil {
				return nil, err
			}
			return typ, err
		}
		typ, err := a.tc.GetTypeVersion(ctx, txn, &a.typeName, flags)
		if typ == nil {
			return nil, err
		}
		return typ, err
	case tree.TableObject:
		a.tableName = tree.MakeTableNameWithSchema(tree.Name(db), tree.Name(schema), tree.Name(object))
		if flags.RequireMutable {
			table, err := a.tc.GetMutableTableDescriptor(ctx, txn, &a.tableName, flags)
			if table == nil {
				// return nil interface.
				return nil, err
			}
			return table, err
		}
		table, err := a.tc.GetTableVersion(ctx, txn, &a.tableName, flags)
		if table == nil {
			// return nil interface.
			return nil, err
		}
		return table, err
	default:
		return nil, errors.AssertionFailedf("unknown desired object kind %d", flags.DesiredObjectKind)
	}
}
