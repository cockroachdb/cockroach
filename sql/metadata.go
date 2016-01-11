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
// Author: Matt Tracy (matt@cockroachlabs.com)

package sql

import (
	"sort"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
)

// MetadataSchema is used to construct the initial sql schema for a new
// CockroachDB cluster being bootstrapped. Tables and databases must be
// installed on the underlying persistent storage before a cockroach store can
// start running correctly, thus requiring this special initialization.
type MetadataSchema struct {
	systemObjects []systemObject
	tables        []metadataTable
}

type systemObject struct {
	parentID ID
	desc     descriptorProto
}

type metadataTable struct {
	definition string
	privileges *PrivilegeDescriptor
}

// MakeMetadataSchema constructs a new MetadataSchema value which contains the
// sql SystemDB.
func MakeMetadataSchema() MetadataSchema {
	ms := MetadataSchema{}
	addSystemDatabaseToSchema(&ms)
	return ms
}

// AddSystemDescriptor adds a new system descriptor to the schema. System
// descriptors have well-known, static descriptors; however, the MetadataSchema
// is used to generate the KeyValue objects needed to install them on a new
// cockroach store.
func (ms *MetadataSchema) AddSystemDescriptor(parentID ID, desc descriptorProto) {
	ms.systemObjects = append(ms.systemObjects, systemObject{parentID, desc})
}

// AddTable adds a new table to the system database.
func (ms *MetadataSchema) AddTable(definition string, privileges *PrivilegeDescriptor) {
	ms.tables = append(ms.tables, metadataTable{
		definition: definition,
		privileges: privileges,
	})
}

// DescriptorCount returns the number of descriptors that will be created by
// this schema. This value is needed to automate certain tests.
func (ms MetadataSchema) DescriptorCount() int {
	count := len(ms.systemObjects)
	count += len(ms.tables)
	return count
}

// GetInitialValues returns the set of initial K/V values which should be added to
// a bootstrapping CockroachDB cluster in order to create the tables contained
// in the schema.
func (ms MetadataSchema) GetInitialValues() []roachpb.KeyValue {
	var ret []roachpb.KeyValue

	// Save the ID generator value, which will generate descriptor IDs for user
	// objects.
	value := roachpb.Value{}
	value.SetInt(int64(keys.MaxReservedDescID + 1))
	ret = append(ret, roachpb.KeyValue{
		Key:   keys.DescIDGenerator,
		Value: value,
	})

	// addDescriptor generates the needed KeyValue objects to install a
	// descriptor on a new cluster.
	addDescriptor := func(parentID ID, desc descriptorProto) {
		// Create name metadata key.
		value := roachpb.Value{}
		value.SetInt(int64(desc.GetID()))
		ret = append(ret, roachpb.KeyValue{
			Key:   MakeNameMetadataKey(parentID, desc.GetName()),
			Value: value,
		})

		// Create descriptor metadata key.
		value = roachpb.Value{}
		wrappedDesc := wrapDescriptor(desc)
		if err := value.SetProto(wrappedDesc); err != nil {
			log.Fatalf("could not marshal %v", desc)
		}
		ret = append(ret, roachpb.KeyValue{
			Key:   MakeDescMetadataKey(desc.GetID()),
			Value: value,
		})
	}

	// Generate initial values for system databases and tables, which have
	// static descriptors that were generated elsewhere.
	for _, sysObj := range ms.systemObjects {
		addDescriptor(sysObj.parentID, sysObj.desc)
	}

	// Descriptor IDs for non-system databases and objects will be generated
	// sequentially within the non-system reserved range.
	initialDescID := keys.MaxSystemConfigDescID + 1
	nextID := func() ID {
		next := initialDescID
		initialDescID++
		return ID(next)
	}

	for _, tbl := range ms.tables {
		dbID := ID(keys.SystemDatabaseID)
		desc := createTableDescriptor(nextID(), dbID, tbl.definition, tbl.privileges)
		addDescriptor(dbID, &desc)
	}

	// Sort returned key values; this is valuable because it matches the way the
	// objects would be sorted if read from the engine.
	sort.Sort(roachpb.KeyValueByKey(ret))
	return ret
}
