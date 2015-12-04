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
	databases     []*MetadataDatabase
}

type systemObject struct {
	parentID ID
	desc     descriptorProto
}

// MetadataDatabase represents a database to be created on a bootstrapped
// cockroach cluster. This structure should only be created by calling the
// "AddDatabase()" method of a MetadataSchema object.
type MetadataDatabase struct {
	name       string
	privileges *PrivilegeDescriptor
	tables     []*metadataTable
}

type metadataTable struct {
	definition string
	privileges *PrivilegeDescriptor
}

// NewMetadataSchema constructs a new MetadataSchema object which contains no
// tables.
func NewMetadataSchema() *MetadataSchema {
	return &MetadataSchema{}
}

// AddSystemDescriptor adds a new system descriptor to the schema. System
// descriptors have well-known, static descriptors; however, the MetadataSchema
// is used to generate the KeyValue objects needed install them on a new
// cockroach store.
func (ms *MetadataSchema) AddSystemDescriptor(parentID ID, desc descriptorProto) {
	ms.systemObjects = append(ms.systemObjects, systemObject{parentID, desc})
}

// AddDatabase adds a new database to the initial schema. The record returned by
// this function can be used to add tables to the new database.
func (ms *MetadataSchema) AddDatabase(name string, privileges *PrivilegeDescriptor) *MetadataDatabase {
	md := &MetadataDatabase{
		name:       name,
		privileges: privileges,
	}
	ms.databases = append(ms.databases, md)
	return md
}

// AddTable adds a new table to this database record.
func (md *MetadataDatabase) AddTable(definition string, privileges *PrivilegeDescriptor) {
	md.tables = append(md.tables, &metadataTable{
		definition: definition,
		privileges: privileges,
	})
}

// GetInitialValues returns the set of initial K/V values which should be added to
// a bootstrapping CockroachDB cluster in order to create the tables contained
// in the schema.
func (ms *MetadataSchema) GetInitialValues() []roachpb.KeyValue {
	var ret []roachpb.KeyValue

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

	// Initial value of descriptor ID generator. Descriptor IDs will be generated
	// sequentially for non-system tables created as part of the initial schema.
	initialDescID := keys.MaxReservedDescID + 1
	nextID := func() ID {
		next := initialDescID
		initialDescID++
		return ID(next)
	}

	// Generate initial values for non-system metadata tables, which do not need
	// well-known IDs.
	for _, db := range ms.databases {
		dbID := nextID()
		addDescriptor(keys.RootNamespaceID, &DatabaseDescriptor{
			Name:       db.name,
			ID:         dbID,
			Privileges: db.privileges,
		})

		for _, tbl := range db.tables {
			desc := createTableDescriptor(nextID(), dbID, tbl.definition, tbl.privileges)
			addDescriptor(dbID, &desc)
		}
	}

	// Save ID generator value.
	value := roachpb.Value{}
	value.SetInt(int64(initialDescID))
	ret = append(ret, roachpb.KeyValue{
		Key:   keys.DescIDGenerator,
		Value: value,
	})

	// Sort returned keys; this
	sort.Sort(roachpb.KeyValueByKey(ret))
	return ret
}
