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
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
)

// databaseKey implements descriptorKey.
type databaseKey struct {
	name string
}

func (dk databaseKey) Key() roachpb.Key {
	return MakeNameMetadataKey(keys.RootNamespaceID, dk.name)
}

func (dk databaseKey) Name() string {
	return dk.name
}

func makeDatabaseDesc(p *parser.CreateDatabase) DatabaseDescriptor {
	return DatabaseDescriptor{
		Name:       p.Name.String(),
		Privileges: NewDefaultPrivilegeDescriptor(),
	}
}

// getDatabaseDesc looks up the database descriptor given its name.
func (p *planner) getDatabaseDesc(name string) (*DatabaseDescriptor, *roachpb.Error) {
	desc := &DatabaseDescriptor{}
	if pErr := p.getDescriptor(databaseKey{name}, desc); pErr != nil {
		return nil, pErr
	}
	return desc, nil
}

// getCachedDatabaseDesc looks up the database descriptor given its name in the
// descriptor cache.
func (p *planner) getCachedDatabaseDesc(name string) (*DatabaseDescriptor, *roachpb.Error) {
	if name == SystemDB.Name {
		return &SystemDB, nil
	}

	nameKey := databaseKey{name}
	nameVal := p.systemConfig.GetValue(nameKey.Key())
	if nameVal == nil {
		return nil, roachpb.NewUErrorf("database %q does not exist in system cache", name)
	}

	id, err := nameVal.GetInt()
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	descKey := MakeDescMetadataKey(ID(id))
	descVal := p.systemConfig.GetValue(descKey)
	if descVal == nil {
		return nil, roachpb.NewUErrorf("database %q has name entry, but no descriptor in system cache", name)
	}

	desc := &Descriptor{}
	if err := descVal.GetProto(desc); err != nil {
		return nil, roachpb.NewError(err)
	}

	database := desc.GetDatabase()
	if database == nil {
		return nil, roachpb.NewErrorf("%q is not a database", name)
	}
	return database, roachpb.NewError(database.Validate())
}
