// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package database primarily provides the incoherent database cache and
// related interfaces.
//
// TODO(ajwerner): Lease database descriptors like all other schema objects and
// eliminate this package.
package database

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Cache holds a cache from database name to database ID. It is
// populated as database IDs are requested and a new cache is created whenever
// the system config changes. As such, no attempt is made to limit its size
// which is naturally limited by the number of database descriptors in the
// system the periodic reset whenever the system config is gossiped.
type Cache struct {
	// databases is really a map of string -> sqlbase.ID
	databases sync.Map

	// codec is used to encode and decode sql keys.
	codec keys.SQLCodec

	// systemConfig holds a copy of the latest system config since the last
	// call to resetForBatch.
	systemConfig *config.SystemConfig
}

// NewCache constructs a new Cache.
func NewCache(codec keys.SQLCodec, cfg *config.SystemConfig) *Cache {
	return &Cache{
		codec:        codec,
		systemConfig: cfg,
	}
}

func (dc *Cache) getID(name string) sqlbase.ID {
	val, ok := dc.databases.Load(name)
	if !ok {
		return sqlbase.InvalidID
	}
	return val.(sqlbase.ID)
}

func (dc *Cache) setID(name string, id sqlbase.ID) {
	dc.databases.Store(name, id)
}

// getCachedDatabaseDesc looks up the database descriptor from the descriptor cache,
// given its name. Returns nil and no error if the name is not present in the
// cache.
func (dc *Cache) getCachedDatabaseDesc(name string) (*sqlbase.ImmutableDatabaseDescriptor, error) {
	dbID, err := dc.GetCachedDatabaseID(name)
	if dbID == sqlbase.InvalidID || err != nil {
		return nil, err
	}

	return dc.getCachedDatabaseDescByID(dbID)
}

// getCachedDatabaseDescByID looks up the database descriptor from the descriptor cache,
// given its ID.
func (dc *Cache) getCachedDatabaseDescByID(
	id sqlbase.ID,
) (*sqlbase.ImmutableDatabaseDescriptor, error) {
	if id == keys.SystemDatabaseID {
		// We can't return a direct reference to SystemDB, because the
		// caller expects a private object that can be modified in-place.
		sysDB := sqlbase.MakeSystemDatabaseDesc()
		return sysDB, nil
	}

	descKey := sqlbase.MakeDescMetadataKey(dc.codec, id)
	descVal := dc.systemConfig.GetValue(descKey)
	if descVal == nil {
		return nil, nil
	}

	desc := &sqlbase.Descriptor{}
	if err := descVal.GetProto(desc); err != nil {
		return nil, err
	}

	dbDesc := desc.GetDatabase()
	if dbDesc == nil {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "[%d] is not a database", id)
	}
	database := sqlbase.NewImmutableDatabaseDescriptor(*dbDesc)
	if err := database.Validate(); err != nil {
		return nil, err
	}
	// TODO(ajwerner): Set ModificationTime.
	return database, nil
}

// GetDatabaseDesc returns the database descriptor given its name
// if it exists in the cache, otherwise falls back to KV operations.
func (dc *Cache) GetDatabaseDesc(
	ctx context.Context,
	txnRunner func(context.Context, func(context.Context, *kv.Txn) error) error,
	name string,
	required bool,
) (*sqlbase.ImmutableDatabaseDescriptor, error) {
	// Lookup the database in the cache first, falling back to the KV store if it
	// isn't present. The cache might cause the usage of a recently renamed
	// database, but that's a race that could occur anyways.
	// The cache lookup may fail.
	desc, err := dc.getCachedDatabaseDesc(name)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		if err := txnRunner(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// Run the descriptor read as high-priority, thereby pushing any intents out
			// of its way. We don't want schema changes to prevent database lookup;
			// we'd rather force them to refresh. Also this prevents deadlocks in cases
			// where the name resolution is triggered by the transaction doing the
			// schema change itself.
			if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
				return err
			}
			a := catalogkv.UncachedPhysicalAccessor{}
			descI, err := a.GetDatabaseDesc(ctx, txn, dc.codec, name,
				tree.DatabaseLookupFlags{Required: required})
			if err != nil {
				return err
			}
			if descI != nil {
				desc = descI.(*sqlbase.ImmutableDatabaseDescriptor)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if desc != nil {
		dc.setID(name, desc.GetID())
	}
	return desc, err
}

// GetDatabaseDescByID returns the database descriptor given its ID
// if it exists in the cache, otherwise falls back to KV operations.
func (dc *Cache) GetDatabaseDescByID(
	ctx context.Context, txn *kv.Txn, id sqlbase.ID,
) (*sqlbase.ImmutableDatabaseDescriptor, error) {
	desc, err := dc.getCachedDatabaseDescByID(id)
	if desc == nil || err != nil {
		if err != nil {
			log.VEventf(ctx, 3, "error getting database descriptor from cache: %s", err)
		}
		desc, err = catalogkv.MustGetDatabaseDescByID(ctx, txn, dc.codec, id)
	}
	return desc, err
}

// GetDatabaseID returns the ID of a database given its name. It
// uses the descriptor cache if possible, otherwise falls back to KV
// operations.
func (dc *Cache) GetDatabaseID(
	ctx context.Context,
	txnRunner func(context.Context, func(context.Context, *kv.Txn) error) error,
	name string,
	required bool,
) (sqlbase.ID, error) {
	dbID, err := dc.GetCachedDatabaseID(name)
	if err != nil {
		return dbID, err
	}
	if dbID == sqlbase.InvalidID {
		if err := txnRunner(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// Run the namespace read as high-priority, thereby pushing any intents out
			// of its way. We don't want schema changes to prevent database acquisitions;
			// we'd rather force them to refresh. Also this prevents deadlocks in cases
			// where the name resolution is triggered by the transaction doing the
			// schema change itself.
			if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
				return err
			}
			var err error
			dbID, err = catalogkv.GetDatabaseID(ctx, txn, dc.codec, name, required)
			return err
		}); err != nil {
			return sqlbase.InvalidID, err
		}
	}
	dc.setID(name, dbID)
	return dbID, nil
}

// GetCachedDatabaseID returns the ID of a database given its name
// from the cache. This method never goes to the store to resolve
// the name to id mapping. Returns InvalidID if the name to id mapping or
// the database descriptor are not in the cache.
func (dc *Cache) GetCachedDatabaseID(name string) (sqlbase.ID, error) {
	if id := dc.getID(name); id != sqlbase.InvalidID {
		return id, nil
	}

	if name == sqlbase.SystemDB.GetName() {
		return sqlbase.SystemDB.GetID(), nil
	}

	var nameKey sqlbase.DescriptorKey = sqlbase.NewDatabaseKey(name)
	nameVal := dc.systemConfig.GetValue(nameKey.Key(dc.codec))
	if nameVal == nil {
		// Try the deprecated system.namespace before returning InvalidID.
		// TODO(solon): This can be removed in 20.2.
		nameKey = sqlbase.NewDeprecatedDatabaseKey(name)
		nameVal = dc.systemConfig.GetValue(nameKey.Key(dc.codec))
		if nameVal == nil {
			return sqlbase.InvalidID, nil
		}
	}

	id, err := nameVal.GetInt()
	return sqlbase.ID(id), err
}

// Codec returns the cache's codec.
func (dc *Cache) Codec() keys.SQLCodec {
	return dc.codec
}
