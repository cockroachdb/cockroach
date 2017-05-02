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

package sqlbase

import (
	"fmt"
	"sort"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/gogo/protobuf/proto"
)

var _ DescriptorProto = &DatabaseDescriptor{}
var _ DescriptorProto = &TableDescriptor{}

// DescriptorKey is the interface implemented by both
// databaseKey and tableKey. It is used to easily get the
// descriptor key and plain name.
type DescriptorKey interface {
	Key() roachpb.Key
	Name() string
}

// DescriptorProto is the interface implemented by both DatabaseDescriptor
// and TableDescriptor.
// TODO(marc): this is getting rather large.
type DescriptorProto interface {
	proto.Message
	GetPrivileges() *PrivilegeDescriptor
	GetID() ID
	SetID(ID)
	TypeName() string
	GetName() string
	SetName(string)
}

// WrapDescriptor fills in a Descriptor.
func WrapDescriptor(descriptor DescriptorProto) *Descriptor {
	desc := &Descriptor{}
	switch t := descriptor.(type) {
	case *TableDescriptor:
		desc.Union = &Descriptor_Table{Table: t}
	case *DatabaseDescriptor:
		desc.Union = &Descriptor_Database{Database: t}
	default:
		panic(fmt.Sprintf("unknown descriptor type: %s", descriptor.TypeName()))
	}
	return desc
}

// MetadataSchema is used to construct the initial sql schema for a new
// CockroachDB cluster being bootstrapped. Tables and databases must be
// installed on the underlying persistent storage before a cockroach store can
// start running correctly, thus requiring this special initialization.
type MetadataSchema struct {
	descs   []metadataDescriptor
	configs int
	otherKV []roachpb.KeyValue
}

type metadataDescriptor struct {
	parentID ID
	desc     DescriptorProto
}

// MakeMetadataSchema constructs a new MetadataSchema value which constructs
// the "system" database.
func MakeMetadataSchema() MetadataSchema {
	ms := MetadataSchema{}
	addSystemDatabaseToSchema(&ms)
	return ms
}

// AddDescriptor adds a new non-config descriptor to the system schema.
func (ms *MetadataSchema) AddDescriptor(parentID ID, desc DescriptorProto) {
	if id := desc.GetID(); id > keys.MaxReservedDescID {
		panic(fmt.Sprintf("invalid reserved table ID: %d > %d", id, keys.MaxReservedDescID))
	}
	for _, d := range ms.descs {
		if d.desc.GetID() == desc.GetID() {
			log.Errorf(context.TODO(), "adding descriptor with duplicate ID: %v", desc)
			return
		}
	}
	ms.descs = append(ms.descs, metadataDescriptor{parentID, desc})
}

// AddConfigDescriptor adds a new descriptor to the system schema. Used only for
// SystemConfig tables and databases.
func (ms *MetadataSchema) AddConfigDescriptor(parentID ID, desc DescriptorProto) {
	ms.AddDescriptor(parentID, desc)
	ms.configs++
}

// SystemDescriptorCount returns the number of descriptors that will be created by
// this schema. This value is needed to automate certain tests.
func (ms MetadataSchema) SystemDescriptorCount() int {
	return len(ms.descs)
}

// SystemConfigDescriptorCount returns the number of config descriptors that
// will be created by this schema. This value is needed to automate certain
// tests.
func (ms MetadataSchema) SystemConfigDescriptorCount() int {
	return ms.configs
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
	addDescriptor := func(parentID ID, desc DescriptorProto) {
		// Create name metadata key.
		value := roachpb.Value{}
		value.SetInt(int64(desc.GetID()))
		ret = append(ret, roachpb.KeyValue{
			Key:   MakeNameMetadataKey(parentID, desc.GetName()),
			Value: value,
		})

		// Create descriptor metadata key.
		value = roachpb.Value{}
		wrappedDesc := WrapDescriptor(desc)
		if err := value.SetProto(wrappedDesc); err != nil {
			log.Fatalf(context.TODO(), "could not marshal %v", desc)
		}
		ret = append(ret, roachpb.KeyValue{
			Key:   MakeDescMetadataKey(desc.GetID()),
			Value: value,
		})
	}

	// Generate initial values for system databases and tables, which have
	// static descriptors that were generated elsewhere.
	for _, sysObj := range ms.descs {
		addDescriptor(sysObj.parentID, sysObj.desc)
	}

	// Other key/value generation that doesn't fit into databases and
	// tables. This can be used to add initial entries to a table.
	ret = append(ret, ms.otherKV...)

	// Sort returned key values; this is valuable because it matches the way the
	// objects would be sorted if read from the engine.
	sort.Sort(roachpb.KeyValueByKey(ret))
	return ret
}

// InitialRangeCount returns the number of ranges that would be installed if
// this metadata schema were installed on a fresh cluster and nothing else. Most
// clusters will have additional ranges installed by migrations, so this
// function should be used when only a lower bound, and not an exact count, is
// needed. See server.ExpectedInitialRangeCount() for a count that includes
// migrations.
func (ms MetadataSchema) InitialRangeCount() int {
	// The number of fixed ranges is determined by the pre-defined split points
	// in SystemConfig.ComputeSplitKey. The early keyspace is split up in order
	// to support separate zone configs for different parts of the system ranges.
	// There are 4 pre-defined split points, so 5 fixed ranges.
	const fixedRanges = 5
	return len(ms.descs) - ms.configs + fixedRanges
}
