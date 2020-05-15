// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// CachedPhysicalAccessor adds a cache on top of any SchemaAccessor.
type CachedPhysicalAccessor struct {
	catalog.SchemaAccessor
	tc *TableCollection

	// Used to avoid allocations.
	tn tree.TableName
}

func NewCachedPhysicalAccessor(tc *TableCollection) *CachedPhysicalAccessor {
	return &CachedPhysicalAccessor{
		SchemaAccessor: catalogkv.UncachedPhysicalAccessor{},
		tc:             tc,
	}
}

var _ catalog.SchemaAccessor = &CachedPhysicalAccessor{}

// GetDatabaseDesc implements the SchemaAccessor interface.
func (a *CachedPhysicalAccessor) GetDatabaseDesc(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	name string,
	flags tree.DatabaseLookupFlags,
) (desc *catalog.DatabaseDescriptor, err error) {
	isSystemDB := name == sqlbase.SystemDB.Name
	if !(flags.AvoidCached || isSystemDB /* || sql.testDisableTableLeases */) {
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
		return a.tc.databaseCache.GetDatabaseDesc(ctx, a.tc.LeaseManager().DB().Txn, name, flags.Required)
	}

	// We avoided the cache. Go lower.
	return a.SchemaAccessor.GetDatabaseDesc(ctx, txn, codec, name, flags)
}

// IsValidSchema implements the SchemaAccessor interface.
func (a *CachedPhysicalAccessor) IsValidSchema(
	ctx context.Context, txn *kv.Txn, _ keys.SQLCodec, dbID sqlbase.ID, scName string,
) (bool, sqlbase.ID, error) {
	return a.tc.resolveSchemaID(ctx, txn, dbID, scName)
}

// GetObjectDesc implements the SchemaAccessor interface.
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
