// Copyright 2015 The Cockroach Authors.
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
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

//
// This file contains routines for low-level access to stored database
// descriptors, as well as accessors for the database cache.
//
// For higher levels in the SQL layer, these interface are likely not
// suitable; consider instead schema_accessors.go and resolver.go.
//

// databaseCache holds a cache from database name to database ID. It is
// populated as database IDs are requested and a new cache is created whenever
// the system config changes. As such, no attempt is made to limit its size
// which is naturally limited by the number of database descriptors in the
// system the periodic reset whenever the system config is gossiped.
type databaseCache struct {
	// databases is really a map of string -> sqlbase.ID
	databases sync.Map

	// systemConfig holds a copy of the latest system config since the last
	// call to resetForBatch.
	systemConfig *config.SystemConfig
}

func newDatabaseCache(cfg *config.SystemConfig) *databaseCache {
	return &databaseCache{
		systemConfig: cfg,
	}
}

func (dc *databaseCache) getID(name string) sqlbase.ID {
	val, ok := dc.databases.Load(name)
	if !ok {
		return sqlbase.InvalidID
	}
	return val.(sqlbase.ID)
}

func (dc *databaseCache) setID(name string, id sqlbase.ID) {
	dc.databases.Store(name, id)
}

func makeDatabaseDesc(p *tree.CreateDatabase) sqlbase.DatabaseDescriptor {
	return sqlbase.DatabaseDescriptor{
		Name:       string(p.Name),
		Privileges: sqlbase.NewDefaultPrivilegeDescriptor(),
	}
}

// getKeysForDatabaseDescriptor retrieves the KV keys corresponding to
// the zone, name and descriptor of a database.
func getKeysForDatabaseDescriptor(
	dbDesc *sqlbase.DatabaseDescriptor,
) (zoneKey roachpb.Key, nameKey roachpb.Key, descKey roachpb.Key) {
	zoneKey = config.MakeZoneKey(uint32(dbDesc.ID))
	nameKey = sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, dbDesc.GetName())
	descKey = sqlbase.MakeDescMetadataKey(dbDesc.ID)
	return
}

// getDatabaseID resolves a database name into a database ID.
// Returns InvalidID on failure.
func getDatabaseID(
	ctx context.Context, txn *client.Txn, name string, required bool,
) (sqlbase.ID, error) {
	if name == sqlbase.SystemDB.Name {
		return sqlbase.SystemDB.ID, nil
	}
	dbID, err := getDescriptorID(ctx, txn, sqlbase.NewDatabaseKey(name))
	if err != nil {
		return sqlbase.InvalidID, err
	}
	if dbID == sqlbase.InvalidID && required {
		return dbID, sqlbase.NewUndefinedDatabaseError(name)
	}
	return dbID, nil
}

// getDatabaseDescByID looks up the database descriptor given its ID,
// returning nil if the descriptor is not found. If you want the "not
// found" condition to return an error, use mustGetDatabaseDescByID() instead.
func getDatabaseDescByID(
	ctx context.Context, txn *client.Txn, id sqlbase.ID,
) (*sqlbase.DatabaseDescriptor, error) {
	log.Eventf(ctx, "fetching descriptor with ID %d", id)
	descKey := sqlbase.MakeDescMetadataKey(id)
	desc := &sqlbase.Descriptor{}
	if err := txn.GetProto(ctx, descKey, desc); err != nil {
		return nil, err
	}

	database := desc.GetDatabase()
	if database == nil {
		return nil, pgerror.Newf(pgcode.WrongObjectType,
			"%q is not a database", desc.String())
	}

	if err := database.Validate(); err != nil {
		return nil, err
	}
	return database, nil
}

// MustGetDatabaseDescByID looks up the database descriptor given its ID,
// returning an error if the descriptor is not found.
func MustGetDatabaseDescByID(
	ctx context.Context, txn *client.Txn, id sqlbase.ID,
) (*sqlbase.DatabaseDescriptor, error) {
	desc, err := getDatabaseDescByID(ctx, txn, id)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return nil, sqlbase.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", id))
	}
	return desc, nil
}

// getCachedDatabaseDesc looks up the database descriptor from the descriptor cache,
// given its name. Returns nil and no error if the name is not present in the
// cache.
func (dc *databaseCache) getCachedDatabaseDesc(name string) (*sqlbase.DatabaseDescriptor, error) {
	dbID, err := dc.getCachedDatabaseID(name)
	if dbID == sqlbase.InvalidID || err != nil {
		return nil, err
	}

	return dc.getCachedDatabaseDescByID(dbID)
}

// getCachedDatabaseDescByID looks up the database descriptor from the descriptor cache,
// given its ID.
func (dc *databaseCache) getCachedDatabaseDescByID(
	id sqlbase.ID,
) (*sqlbase.DatabaseDescriptor, error) {
	if id == sqlbase.SystemDB.ID {
		// We can't return a direct reference to SystemDB, because the
		// caller expects a private object that can be modified in-place.
		sysDB := sqlbase.MakeSystemDatabaseDesc()
		return &sysDB, nil
	}

	descKey := sqlbase.MakeDescMetadataKey(id)
	descVal := dc.systemConfig.GetValue(descKey)
	if descVal == nil {
		return nil, nil
	}

	desc := &sqlbase.Descriptor{}
	if err := descVal.GetProto(desc); err != nil {
		return nil, err
	}

	database := desc.GetDatabase()
	if database == nil {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "[%d] is not a database", id)
	}

	return database, database.Validate()
}

// getDatabaseDesc returns the database descriptor given its name
// if it exists in the cache, otherwise falls back to KV operations.
func (dc *databaseCache) getDatabaseDesc(
	ctx context.Context,
	txnRunner func(context.Context, func(context.Context, *client.Txn) error) error,
	name string,
	required bool,
) (*sqlbase.DatabaseDescriptor, error) {
	// Lookup the database in the cache first, falling back to the KV store if it
	// isn't present. The cache might cause the usage of a recently renamed
	// database, but that's a race that could occur anyways.
	// The cache lookup may fail.
	desc, err := dc.getCachedDatabaseDesc(name)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		if err := txnRunner(ctx, func(ctx context.Context, txn *client.Txn) error {
			a := UncachedPhysicalAccessor{}
			desc, err = a.GetDatabaseDesc(ctx, txn, name,
				DatabaseLookupFlags{required: required})
			return err
		}); err != nil {
			return nil, err
		}
	}
	if desc != nil {
		dc.setID(name, desc.ID)
	}
	return desc, err
}

// getDatabaseDescByID returns the database descriptor given its ID
// if it exists in the cache, otherwise falls back to KV operations.
func (dc *databaseCache) getDatabaseDescByID(
	ctx context.Context, txn *client.Txn, id sqlbase.ID,
) (*sqlbase.DatabaseDescriptor, error) {
	desc, err := dc.getCachedDatabaseDescByID(id)
	if err != nil {
		log.VEventf(ctx, 3, "error getting database descriptor from cache: %s", err)
		desc, err = MustGetDatabaseDescByID(ctx, txn, id)
	}
	return desc, err
}

// getDatabaseID returns the ID of a database given its name. It
// uses the descriptor cache if possible, otherwise falls back to KV
// operations.
func (dc *databaseCache) getDatabaseID(
	ctx context.Context,
	txnRunner func(context.Context, func(context.Context, *client.Txn) error) error,
	name string,
	required bool,
) (sqlbase.ID, error) {
	dbID, err := dc.getCachedDatabaseID(name)
	if err != nil {
		return dbID, err
	}
	if dbID == sqlbase.InvalidID {
		if err := txnRunner(ctx, func(ctx context.Context, txn *client.Txn) error {
			var err error
			dbID, err = getDatabaseID(ctx, txn, name, required)
			return err
		}); err != nil {
			return sqlbase.InvalidID, err
		}
	}
	dc.setID(name, dbID)
	return dbID, nil
}

// getCachedDatabaseID returns the ID of a database given its name
// from the cache. This method never goes to the store to resolve
// the name to id mapping. Returns InvalidID if the name to id mapping or
// the database descriptor are not in the cache.
func (dc *databaseCache) getCachedDatabaseID(name string) (sqlbase.ID, error) {
	if id := dc.getID(name); id != sqlbase.InvalidID {
		return id, nil
	}

	if name == sqlbase.SystemDB.Name {
		return sqlbase.SystemDB.ID, nil
	}

	nameKey := sqlbase.NewDatabaseKey(name)
	nameVal := dc.systemConfig.GetValue(nameKey.Key())
	if nameVal == nil {
		return sqlbase.InvalidID, nil
	}

	id, err := nameVal.GetInt()
	return sqlbase.ID(id), err
}

// renameDatabase implements the DatabaseDescEditor interface.
func (p *planner) renameDatabase(
	ctx context.Context, oldDesc *sqlbase.DatabaseDescriptor, newName string,
) error {
	oldName := oldDesc.Name
	oldDesc.SetName(newName)
	if err := oldDesc.Validate(); err != nil {
		return err
	}

	oldKey := sqlbase.NewDatabaseKey(oldName).Key()
	newKey := sqlbase.NewDatabaseKey(newName).Key()
	descID := oldDesc.GetID()
	descKey := sqlbase.MakeDescMetadataKey(descID)
	descDesc := sqlbase.WrapDescriptor(oldDesc)

	b := &client.Batch{}
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", newKey, descID)
		log.VEventf(ctx, 2, "Put %s -> %s", descKey, descDesc)
		log.VEventf(ctx, 2, "Del %s", oldKey)
	}
	b.CPut(newKey, descID, nil)
	b.Put(descKey, descDesc)
	b.Del(oldKey)

	p.Tables().addUncommittedDatabase(oldName, descID, dbDropped)
	p.Tables().addUncommittedDatabase(newName, descID, dbCreated)

	if err := p.txn.Run(ctx, b); err != nil {
		if _, ok := err.(*roachpb.ConditionFailedError); ok {
			return pgerror.Newf(pgcode.DuplicateDatabase,
				"the new database name %q already exists", newName)
		}
		return err
	}

	return nil
}
