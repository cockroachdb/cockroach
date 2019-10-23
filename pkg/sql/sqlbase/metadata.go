// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
	protoutil.Message
	GetPrivileges() *PrivilegeDescriptor
	GetID() ID
	SetID(ID)
	TypeName() string
	GetName() string
	SetName(string)
	GetAuditMode() TableDescriptor_AuditMode
}

// WrapDescriptor fills in a Descriptor.
func WrapDescriptor(descriptor DescriptorProto) *Descriptor {
	desc := &Descriptor{}
	switch t := descriptor.(type) {
	case *MutableTableDescriptor:
		desc.Union = &Descriptor_Table{Table: &t.TableDescriptor}
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
	descs         []metadataDescriptor
	otherSplitIDs []uint32
	otherKV       []roachpb.KeyValue
}

type metadataDescriptor struct {
	parentID ID
	desc     DescriptorProto
}

// MakeMetadataSchema constructs a new MetadataSchema value which constructs
// the "system" database.
func MakeMetadataSchema(
	defaultZoneConfig *zonepb.ZoneConfig, defaultSystemZoneConfig *zonepb.ZoneConfig,
) MetadataSchema {
	ms := MetadataSchema{}
	addSystemDatabaseToSchema(&ms, defaultZoneConfig, defaultSystemZoneConfig)
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

// AddSplitIDs adds some "table ids" to the MetadataSchema such that
// corresponding keys are returned as split points by GetInitialValues().
// AddDescriptor() has the same effect for the table descriptors that are passed
// to it, but we also have a couple of "fake tables" that don't have descriptors
// but need splits just the same.
func (ms *MetadataSchema) AddSplitIDs(id ...uint32) {
	ms.otherSplitIDs = append(ms.otherSplitIDs, id...)
}

// SystemDescriptorCount returns the number of descriptors that will be created by
// this schema. This value is needed to automate certain tests.
func (ms MetadataSchema) SystemDescriptorCount() int {
	return len(ms.descs)
}

// GetInitialValues returns the set of initial K/V values which should be added to
// a bootstrapping cluster in order to create the tables contained
// in the schema. Also returns a list of split points (a split for each SQL
// table descriptor part of the initial values). Both returned sets are sorted.
func (ms MetadataSchema) GetInitialValues() ([]roachpb.KeyValue, []roachpb.RKey) {
	var ret []roachpb.KeyValue
	var splits []roachpb.RKey

	// Save the ID generator value, which will generate descriptor IDs for user
	// objects.
	value := roachpb.Value{}
	value.SetInt(int64(keys.MinUserDescID))
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
			Key:   NewTableKey(parentID, desc.GetName()).Key(),
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
		if desc.GetID() > keys.MaxSystemConfigDescID {
			splits = append(splits, roachpb.RKey(keys.MakeTablePrefix(uint32(desc.GetID()))))
		}
	}

	// Generate initial values for system databases and tables, which have
	// static descriptors that were generated elsewhere.
	for _, sysObj := range ms.descs {
		addDescriptor(sysObj.parentID, sysObj.desc)
	}

	for _, id := range ms.otherSplitIDs {
		splits = append(splits, roachpb.RKey(keys.MakeTablePrefix(id)))
	}

	// Other key/value generation that doesn't fit into databases and
	// tables. This can be used to add initial entries to a table.
	ret = append(ret, ms.otherKV...)

	// Sort returned key values; this is valuable because it matches the way the
	// objects would be sorted if read from the engine.
	sort.Sort(roachpb.KeyValueByKey(ret))
	sort.Slice(splits, func(i, j int) bool {
		return splits[i].Less(splits[j])
	})

	return ret, splits
}

// DescriptorIDs returns the descriptor IDs present in the metadata schema in
// sorted order.
func (ms MetadataSchema) DescriptorIDs() IDs {
	descriptorIDs := IDs{}
	for _, md := range ms.descs {
		descriptorIDs = append(descriptorIDs, md.desc.GetID())
	}
	sort.Sort(descriptorIDs)
	return descriptorIDs
}

// systemTableIDCache is used to accelerate name lookups
// on table descriptors. It relies on the fact that
// table IDs under MaxReservedDescID are fixed.
var systemTableIDCache = func() map[string]ID {
	cache := make(map[string]ID)

	ms := MetadataSchema{}
	addSystemDescriptorsToSchema(&ms)
	for _, d := range ms.descs {
		t, ok := d.desc.(*TableDescriptor)
		if !ok || t.ParentID != SystemDB.ID || t.ID > keys.MaxReservedDescID {
			// We only cache table descriptors under 'system' with a reserved table ID.
			continue
		}
		cache[t.Name] = t.ID
	}

	return cache
}()

// LookupSystemTableDescriptorID uses the lookup cache above
// to bypass a KV lookup when resolving the name of system tables.
func LookupSystemTableDescriptorID(dbID ID, tableName string) ID {
	if dbID != SystemDB.ID {
		return InvalidID
	}
	dbID, ok := systemTableIDCache[tableName]
	if !ok {
		return InvalidID
	}
	return dbID
}
