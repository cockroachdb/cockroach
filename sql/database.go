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
	"sync"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
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
	mu        sync.Mutex
	databases map[string]sqlbase.ID
}

func (s *databaseCache) getID(name string) sqlbase.ID {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.databases[name]
}

func (s *databaseCache) setID(name string, id sqlbase.ID) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.databases[name] = id
}

func makeDatabaseDesc(p *parser.CreateDatabase) sqlbase.DatabaseDescriptor {
	return sqlbase.DatabaseDescriptor{
		Name:       p.Name.String(),
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
	// getDatabaseDesc looks up the database descriptor given its name.
	// Returns nil if the descriptor is not found. If you want to turn the "not
	// found" condition into an error, use newUndefinedDatabaseError().
	getDatabaseDesc(name string) (*sqlbase.DatabaseDescriptor, error)

	// getCachedDatabaseDesc looks up the database descriptor from
	// the descriptor cache, given its name.
	getCachedDatabaseDesc(name string) (*sqlbase.DatabaseDescriptor, error)

	// getDatabaseID returns the ID of a database given its name.  It
	// uses the descriptor cache if possible, otherwise falls back to KV
	// operations.
	getDatabaseID(name string) (sqlbase.ID, error)
}

var _ DatabaseAccessor = &planner{}

// getDatabaseDesc implements the DatabaseAccessor interface.
func (p *planner) getDatabaseDesc(name string) (*sqlbase.DatabaseDescriptor, error) {
	desc := &sqlbase.DatabaseDescriptor{}
	found, err := p.getDescriptor(databaseKey{name}, desc)
	if !found {
		return nil, err
	}
	return desc, err
}

// getCachedDatabaseDesc implements the DatabaseAccessor interface.
func (p *planner) getCachedDatabaseDesc(name string) (*sqlbase.DatabaseDescriptor, error) {
	if name == sqlbase.SystemDB.Name {
		return &sqlbase.SystemDB, nil
	}

	nameKey := databaseKey{name}
	nameVal := p.systemConfig.GetValue(nameKey.Key())
	if nameVal == nil {
		return nil, fmt.Errorf("database %q does not exist in system cache", name)
	}

	id, err := nameVal.GetInt()
	if err != nil {
		return nil, err
	}

	descKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(id))
	descVal := p.systemConfig.GetValue(descKey)
	if descVal == nil {
		return nil, fmt.Errorf("database %q has name entry, but no descriptor in system cache", name)
	}

	desc := &sqlbase.Descriptor{}
	if err := descVal.GetProto(desc); err != nil {
		return nil, err
	}

	database := desc.GetDatabase()
	if database == nil {
		return nil, util.Errorf("%q is not a database", name)
	}

	return database, database.Validate()
}

// getDatabaseID implements the DatabaseAccessor interface.
func (p *planner) getDatabaseID(name string) (sqlbase.ID, error) {
	if id := p.databaseCache.getID(name); id != 0 {
		return id, nil
	}

	// Lookup the database in the cache first, falling back to the KV store if it
	// isn't present. The cache might cause the usage of a recently renamed
	// database, but that's a race that could occur anyways.
	desc, err := p.getCachedDatabaseDesc(name)
	if err != nil {
		if log.V(3) {
			log.Infof("%v", err)
		}
		var err error
		desc, err = p.getDatabaseDesc(name)
		if err != nil {
			return 0, err
		}
		if desc == nil {
			return 0, newUndefinedDatabaseError(name)
		}
	}

	p.databaseCache.setID(name, desc.ID)
	return desc.ID, nil
}
