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
//   - uses a `*TableCollection` (table.go) as cache.
//

// CachedPhysicalAccessor adds a cache on top of any Accessor.
type CachedPhysicalAccessor struct {
	catalog.Accessor
	tc *TableCollection
	// Used to avoid allocations.
	tn TableName
}

var _ catalog.Accessor = &CachedPhysicalAccessor{}

// GetDatabaseDesc implements the Accessor interface.
func (a *CachedPhysicalAccessor) GetDatabaseDesc(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	name string,
	flags tree.DatabaseLookupFlags,
) (desc *DatabaseDescriptor, err error) {
	isSystemDB := name == sqlbase.SystemDB.Name
	if !(flags.AvoidCached || isSystemDB || testDisableTableLeases) {
		refuseFurtherLookup, dbID, err := a.tc.getUncommittedDatabaseID(name, flags.Required)
		if refuseFurtherLookup || err != nil {
			return nil, err
		}

		if dbID != sqlbase.InvalidID {
			// Some database ID was found in the list of uncommitted DB changes.
			// Use that to get the descriptor.
			desc, err := a.tc.databaseCache.GetDatabaseDescByID(ctx, txn, dbID)
			if desc == nil && flags.Required {
				return nil, sqlbase.NewUndefinedDatabaseError(name)
			}
			return desc, err
		}

		// The database was not known in the uncommitted list. Have the db
		// cache look it up by name for us.
		return a.tc.databaseCache.GetDatabaseDesc(ctx, a.tc.leaseMgr.db.Txn, name, flags.Required)
	}

	// We avoided the cache. Go lower.
	return a.Accessor.GetDatabaseDesc(ctx, txn, codec, name, flags)
}

// IsValidSchema implements the Accessor interface.
func (a *CachedPhysicalAccessor) IsValidSchema(
	ctx context.Context, txn *kv.Txn, _ keys.SQLCodec, dbID sqlbase.ID, scName string,
) (bool, sqlbase.ID, error) {
	return a.tc.resolveSchemaID(ctx, txn, dbID, scName)
}

// GetObjectDesc implements the Accessor interface.
func (a *CachedPhysicalAccessor) GetObjectDesc(
	ctx context.Context,
	txn *kv.Txn,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	db, schema, object string,
	flags tree.ObjectLookupFlags,
) (catalog.ObjectDescriptor, error) {
	switch flags.DesiredObjectKind {
	case tree.TypeObject:
		return nil, errors.AssertionFailedf("accesses to type descriptors aren't cached")
	case tree.TableObject:
		a.tn = tree.MakeTableNameWithSchema(tree.Name(db), tree.Name(schema), tree.Name(object))
		if flags.RequireMutable {
			table, err := a.tc.getMutableTableDescriptor(ctx, txn, &a.tn, flags)
			if table == nil {
				// return nil interface.
				return nil, err
			}
			return table, err
		}
		table, err := a.tc.getTableVersion(ctx, txn, &a.tn, flags)
		if table == nil {
			// return nil interface.
			return nil, err
		}
		return table, err
	default:
		return nil, errors.AssertionFailedf("unknown desired object kind %d", flags.DesiredObjectKind)
	}
}
