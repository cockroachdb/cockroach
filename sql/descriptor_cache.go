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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
	gogoproto "github.com/gogo/protobuf/proto"
)

// TestingDisableDescriptorCache is used by tests and benchmarks to disable
// the descriptor cache functionality. All lookups will bypass the cache
// and use the DB client.
var TestingDisableDescriptorCache bool

// getCachedTableDesc looks for a table descriptor in the descriptor cache.
// Looks up the database descriptor, followed by the table descriptor.
//
// Returns: the table descriptor, whether it comes from the cache, and an error.
func (p *planner) getCachedTableDesc(qname *parser.QualifiedName) (*TableDescriptor, bool, error) {
	if err := qname.NormalizeTableName(p.session.Database); err != nil {
		return nil, false, err
	}
	dbDesc, cached, err := p.getCachedDatabaseDesc(qname.Database())
	if err != nil {
		return nil, false, err
	}

	desc := &TableDescriptor{}
	// Only attempt cached lookup if the database descriptor was found in the cache.
	if cached {
		if err := p.getCachedDescriptor(tableKey{dbDesc.ID, qname.Table()}, desc); err == nil {
			return desc, true, nil
		}
		// Problem, or not found in the cache: fall back on KV lookup.
	}

	if err := p.getDescriptor(tableKey{dbDesc.ID, qname.Table()}, desc); err != nil {
		return nil, false, err
	}
	return desc, false, nil
}

// getCachedDatabaseDesc looks for a database descriptor in the descriptor cache.
// If there is an error looking up in the descriptor cache, or if not found, falls
// back on kv lookup.
//
// The system DB is never looked up in the cache as we may be performing operations
// on it that really need to know the current values.
// Returns: the database descriptor, whether it comes from the cache, and an error.
func (p *planner) getCachedDatabaseDesc(name string) (*DatabaseDescriptor, bool, error) {
	if !TestingDisableDescriptorCache && name != SystemDB.Name {
		desc := &DatabaseDescriptor{}
		if err := p.getCachedDescriptor(databaseKey{name}, desc); err == nil {
			return desc, true, nil
		}
		// Problem looking up in the cache: fall back on non-cached version.
	}
	d, err := p.getDatabaseDesc(name)
	return d, false, err
}

// getCachedDescriptor looks for a database descriptor in the descriptor cache.
func (p *planner) getCachedDescriptor(plainKey descriptorKey, descriptor descriptorProto) error {
	kv, found := p.systemConfig.Get(plainKey.Key())
	if !found {
		return fmt.Errorf("%s %q does not exist in system cache", descriptor.TypeName(), plainKey.Name())
	}

	id, err := kv.Value.GetInt()
	if err != nil {
		return err
	}

	descKey := MakeDescMetadataKey(ID(id))
	kv, found = p.systemConfig.Get(descKey)
	if !found {
		return fmt.Errorf("%s %q has name entry, but no descriptor in system cache",
			descriptor.TypeName(), plainKey.Name())
	}

	if err := gogoproto.Unmarshal(kv.Value.Bytes, descriptor); err != nil {
		return err
	}

	return descriptor.Validate()
}
