// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql

import (
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// databaseKey implements sqlbase.DescriptorKey.
type databaseKey struct {
	name string
}

func (dk databaseKey) Key() roachpb.Key {
	return sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, dk.name)
}

func (dk databaseKey) Name() string {
	return dk.name
}

// databaseCache holds a cache from database name to database ID. It is
// populated as database IDs are requested and a new cache is created whenever
// the system config changes. As such, no attempt is made to limit its size
// which is naturally limited by the number of database descriptors in the
// system the periodic reset whenever the system config is gossiped.
type databaseCache struct {
	mu        syncutil.Mutex
	databases map[string]sqlbase.ID

	// systemConfig holds a copy of the latest system config since the last
	// call to resetForBatch.
	systemConfig config.SystemConfig
}

func newDatabaseCache(cfg config.SystemConfig) *databaseCache {
	return &databaseCache{
		databases:    map[string]sqlbase.ID{},
		systemConfig: cfg,
	}
}

func (dc *databaseCache) getID(name string) sqlbase.ID {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.databases[name]
}

func (dc *databaseCache) setID(name string, id sqlbase.ID) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.databases[name] = id
}

func makeDatabaseDesc(p *parser.CreateDatabase) sqlbase.DatabaseDescriptor {
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
	zoneKey = sqlbase.MakeZoneKey(dbDesc.ID)
	nameKey = sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, dbDesc.GetName())
	descKey = sqlbase.MakeDescMetadataKey(dbDesc.ID)
	return
}

// DatabaseAccessor provides helper methods for using SQL database descriptors.
type DatabaseAccessor interface {
	// createDatabase attempts to create a database with the provided DatabaseDescriptor.
	// Returns true if the database is actually created, false if it already existed,
	// or an error if one was encountered. The ifNotExists flag is used to declare
	// if the "already existed" state should be an error (false) or a no-op (true).
	createDatabase(
		ctx context.Context, desc *sqlbase.DatabaseDescriptor, ifNotExists bool,
	) (bool, error)

	// renameDatabase attempts to rename the database with the provided DatabaseDescriptor
	// to a new name. The method will mutate the provided DatabaseDescriptor, updating its
	// name with the new name.
	renameDatabase(ctx context.Context, oldDesc *sqlbase.DatabaseDescriptor, newName string) error
}

var _ DatabaseAccessor = &planner{}

// getDatabaseDesc looks up the database descriptor given its name,
// returning nil if the descriptor is not found. If you want the "not
// found" condition to return an error, use mustGetDatabaseDesc() instead.
func getDatabaseDesc(
	ctx context.Context, txn *client.Txn, vt VirtualTabler, name string,
) (*sqlbase.DatabaseDescriptor, error) {
	if virtual := vt.getVirtualDatabaseDesc(name); virtual != nil {
		return virtual, nil
	}
	desc := &sqlbase.DatabaseDescriptor{}
	found, err := getDescriptor(ctx, txn, databaseKey{name}, desc)
	if !found {
		return nil, err
	}
	return desc, err
}

// MustGetDatabaseDesc looks up the database descriptor given its name,
// returning an error if the descriptor is not found.
func MustGetDatabaseDesc(
	ctx context.Context, txn *client.Txn, vt VirtualTabler, name string,
) (*sqlbase.DatabaseDescriptor, error) {
	desc, err := getDatabaseDesc(ctx, txn, vt, name)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return nil, sqlbase.NewUndefinedDatabaseError(name)
	}
	return desc, nil
}

// getCachedDatabaseDesc looks up the database descriptor from the descriptor cache,
// given its name.
func (dc *databaseCache) getCachedDatabaseDesc(name string) (*sqlbase.DatabaseDescriptor, error) {
	if name == sqlbase.SystemDB.Name {
		return &sqlbase.SystemDB, nil
	}

	nameKey := databaseKey{name}
	nameVal := dc.systemConfig.GetValue(nameKey.Key())
	if nameVal == nil {
		return nil, fmt.Errorf("database %q does not exist in system cache", name)
	}

	id, err := nameVal.GetInt()
	if err != nil {
		return nil, err
	}

	descKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(id))
	descVal := dc.systemConfig.GetValue(descKey)
	if descVal == nil {
		return nil, fmt.Errorf("database %q has name entry, but no descriptor in system cache", name)
	}

	desc := &sqlbase.Descriptor{}
	if err := descVal.GetProto(desc); err != nil {
		return nil, err
	}

	database := desc.GetDatabase()
	if database == nil {
		return nil, errors.Errorf("%q is not a database", name)
	}

	return database, database.Validate()
}

// getAllDatabaseDescs looks up and returns all available database
// descriptors.
func getAllDatabaseDescs(
	ctx context.Context, txn *client.Txn,
) ([]*sqlbase.DatabaseDescriptor, error) {
	descs, err := getAllDescriptors(ctx, txn)
	if err != nil {
		return nil, err
	}

	var dbDescs []*sqlbase.DatabaseDescriptor
	for _, desc := range descs {
		if dbDesc, ok := desc.(*sqlbase.DatabaseDescriptor); ok {
			dbDescs = append(dbDescs, dbDesc)
		}
	}
	return dbDescs, nil
}

// getDatabaseID returns the ID of a database given its name. It
// uses the descriptor cache if possible, otherwise falls back to KV
// operations.
func (dc *databaseCache) getDatabaseID(
	ctx context.Context, txn *client.Txn, vt VirtualTabler, name string,
) (sqlbase.ID, error) {
	if virtual := vt.getVirtualDatabaseDesc(name); virtual != nil {
		return virtual.GetID(), nil
	}

	if id := dc.getID(name); id != 0 {
		return id, nil
	}

	// Lookup the database in the cache first, falling back to the KV store if it
	// isn't present. The cache might cause the usage of a recently renamed
	// database, but that's a race that could occur anyways.
	desc, err := dc.getCachedDatabaseDesc(name)
	if err != nil {
		if log.V(3) {
			log.Infof(ctx, "error getting database descriptor: %s", err)
		}
		var err error
		desc, err = MustGetDatabaseDesc(ctx, txn, vt, name)
		if err != nil {
			return 0, err
		}
	}

	dc.setID(name, desc.ID)
	return desc.ID, nil
}

// createDatabase implements the DatabaseAccessor interface.
func (p *planner) createDatabase(
	ctx context.Context, desc *sqlbase.DatabaseDescriptor, ifNotExists bool,
) (bool, error) {
	if p.session.virtualSchemas.isVirtualDatabase(desc.Name) {
		if ifNotExists {
			// Noop.
			return false, nil
		}
		return false, descriptorAlreadyExistsErr{desc, desc.Name}
	}
	return p.createDescriptor(ctx, databaseKey{desc.Name}, desc, ifNotExists)
}

// renameDatabase implements the DatabaseAccessor interface.
func (p *planner) renameDatabase(
	ctx context.Context, oldDesc *sqlbase.DatabaseDescriptor, newName string,
) error {
	onAlreadyExists := func() error {
		return fmt.Errorf("the new database name %q already exists", newName)
	}

	if p.session.virtualSchemas.isVirtualDatabase(newName) {
		return onAlreadyExists()
	}

	oldName := oldDesc.Name
	oldDesc.SetName(newName)
	if err := oldDesc.Validate(); err != nil {
		return err
	}

	oldKey := databaseKey{oldName}.Key()
	newKey := databaseKey{newName}.Key()
	descID := oldDesc.GetID()
	descKey := sqlbase.MakeDescMetadataKey(descID)
	descDesc := sqlbase.WrapDescriptor(oldDesc)

	b := &client.Batch{}
	b.CPut(newKey, descID, nil)
	b.Put(descKey, descDesc)
	b.Del(oldKey)

	if err := p.txn.Run(ctx, b); err != nil {
		if _, ok := err.(*roachpb.ConditionFailedError); ok {
			return onAlreadyExists()
		}
		return err
	}

	p.session.setTestingVerifyMetadata(func(systemConfig config.SystemConfig) error {
		if err := expectDescriptorID(systemConfig, newKey, descID); err != nil {
			return err
		}
		if err := expectDescriptor(systemConfig, descKey, descDesc); err != nil {
			return err
		}
		return expectDeleted(systemConfig, oldKey)
	})
	return nil
}
