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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// DescriptorKey is the interface implemented by both
// databaseKey and tableKey. It is used to easily get the
// descriptor key and plain name.
type DescriptorKey interface {
	Key(codec keys.SQLCodec) roachpb.Key
	Name() string
}

// wrapDescriptor fills in a Descriptor from a given member of its union.
//
// TODO(ajwerner): Replace this with the relevant type-specific DescriptorProto
// methods.
func wrapDescriptor(descriptor protoutil.Message) *Descriptor {
	desc := &Descriptor{}
	switch t := descriptor.(type) {
	case *MutableTableDescriptor:
		desc.Union = &Descriptor_Table{Table: &t.TableDescriptor}
	case *TableDescriptor:
		desc.Union = &Descriptor_Table{Table: t}
	case *DatabaseDescriptor:
		desc.Union = &Descriptor_Database{Database: t}
	case *MutableTypeDescriptor:
		desc.Union = &Descriptor_Type{Type: &t.TypeDescriptor}
	case *TypeDescriptor:
		desc.Union = &Descriptor_Type{Type: t}
	case *SchemaDescriptor:
		desc.Union = &Descriptor_Schema{Schema: t}
	default:
		panic(fmt.Sprintf("unknown descriptor type: %T", descriptor))
	}
	return desc
}

// MetadataSchema is used to construct the initial sql schema for a new
// CockroachDB cluster being bootstrapped. Tables and databases must be
// installed on the underlying persistent storage before a cockroach store can
// start running correctly, thus requiring this special initialization.
type MetadataSchema struct {
	codec         keys.SQLCodec
	descs         []metadataDescriptor
	otherSplitIDs []uint32
	otherKV       []roachpb.KeyValue
}

type metadataDescriptor struct {
	parentID ID
	desc     DescriptorInterface
}

// MakeMetadataSchema constructs a new MetadataSchema value which constructs
// the "system" database. Default zone configurations are required to create
// a MetadataSchema for the system tenant, but do not need to be supplied for
// any other tenant.
func MakeMetadataSchema(
	codec keys.SQLCodec,
	defaultZoneConfig *zonepb.ZoneConfig,
	defaultSystemZoneConfig *zonepb.ZoneConfig,
) MetadataSchema {
	ms := MetadataSchema{codec: codec}
	addSystemDatabaseToSchema(&ms, defaultZoneConfig, defaultSystemZoneConfig)
	return ms
}

// AddDescriptor adds a new non-config descriptor to the system schema.
func (ms *MetadataSchema) AddDescriptor(parentID ID, desc DescriptorInterface) {
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
		Key:   ms.codec.DescIDSequenceKey(),
		Value: value,
	})

	// addDescriptor generates the needed KeyValue objects to install a
	// descriptor on a new cluster.
	addDescriptor := func(parentID ID, desc DescriptorInterface) {
		// Create name metadata key.
		value := roachpb.Value{}
		value.SetInt(int64(desc.GetID()))
		if parentID != keys.RootNamespaceID {
			ret = append(ret, roachpb.KeyValue{
				Key:   NewPublicTableKey(parentID, desc.GetName()).Key(ms.codec),
				Value: value,
			})
		} else {
			// Initializing a database. Databases must be initialized with
			// the public schema, as all tables are scoped under the public schema.
			publicSchemaValue := roachpb.Value{}
			publicSchemaValue.SetInt(int64(keys.PublicSchemaID))
			ret = append(
				ret,
				roachpb.KeyValue{
					Key:   NewDatabaseKey(desc.GetName()).Key(ms.codec),
					Value: value,
				},
				roachpb.KeyValue{
					Key:   NewPublicSchemaKey(desc.GetID()).Key(ms.codec),
					Value: publicSchemaValue,
				})
		}

		// Create descriptor metadata key.
		value = roachpb.Value{}
		descDesc := desc.DescriptorProto()
		if err := value.SetProto(descDesc); err != nil {
			log.Fatalf(context.TODO(), "could not marshal %v", desc)
		}
		ret = append(ret, roachpb.KeyValue{
			Key:   MakeDescMetadataKey(ms.codec, desc.GetID()),
			Value: value,
		})
		if desc.GetID() > keys.MaxSystemConfigDescID {
			splits = append(splits, roachpb.RKey(ms.codec.TablePrefix(uint32(desc.GetID()))))
		}
	}

	// Generate initial values for system databases and tables, which have
	// static descriptors that were generated elsewhere.
	for _, sysObj := range ms.descs {
		addDescriptor(sysObj.parentID, sysObj.desc)
	}

	// The splits slice currently has a split point for each of the object
	// descriptors in ms.descs. If we're fetching the initial values for the
	// system tenant, add any additional split point, which correspond to
	// "pseudo" tables that don't have real descriptors.
	//
	// If we're fetching the initial values for a secondary tenant, things are
	// different. Secondary tenants do not enforce split points at table
	// boundaries. In fact, if we tried to split at table boundaries, those
	// splits would quickly be merged away. The only enforced split points are
	// between secondary tenants (e.g. between /tenant/<id> and /tenant/<id+1>).
	// So we drop all descriptor split points and replace it with a single split
	// point at the beginning of this tenant's keyspace.
	if ms.codec.ForSystemTenant() {
		for _, id := range ms.otherSplitIDs {
			splits = append(splits, roachpb.RKey(ms.codec.TablePrefix(id)))
		}
	} else {
		splits = []roachpb.RKey{roachpb.RKey(ms.codec.TenantPrefix())}
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

// systemTableIDCache is used to accelerate name lookups on table descriptors.
// It relies on the fact that table IDs under MaxReservedDescID are fixed.
//
// Mapping: [systemTenant][tableName] => tableID
var systemTableIDCache = func() [2]map[string]ID {
	cacheForTenant := func(systemTenant bool) map[string]ID {
		cache := make(map[string]ID)

		codec := keys.SystemSQLCodec
		if !systemTenant {
			codec = keys.MakeSQLCodec(roachpb.MinTenantID)
		}

		ms := MetadataSchema{codec: codec}
		addSystemDescriptorsToSchema(&ms)
		for _, d := range ms.descs {
			t := d.desc.TableDesc()
			if t == nil || t.ParentID != keys.SystemDatabaseID || t.ID > keys.MaxReservedDescID {
				// We only cache table descriptors under 'system' with a
				// reserved table ID.
				continue
			}
			cache[t.Name] = t.ID
		}

		// This special case exists so that we resolve "namespace" to the new
		// namespace table ID (30) in 20.1, while the Name in the "namespace"
		// descriptor is still set to "namespace2" during the 20.1 cycle. We
		// couldn't set the new namespace table's Name to "namespace" in 20.1,
		// because it had to co-exist with the old namespace table, whose name
		// must *remain* "namespace" - and you can't have duplicate descriptor
		// Name fields.
		//
		// This can be removed in 20.2, when we add a migration to change the
		// new namespace table's Name to "namespace" again.
		// TODO(solon): remove this in 20.2.
		cache[NamespaceTableName] = keys.NamespaceTableID

		return cache
	}

	var cache [2]map[string]ID
	for _, b := range []bool{false, true} {
		cache[boolToInt(b)] = cacheForTenant(b)
	}
	return cache
}()

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// LookupSystemTableDescriptorID uses the lookup cache above
// to bypass a KV lookup when resolving the name of system tables.
func LookupSystemTableDescriptorID(
	ctx context.Context, settings *cluster.Settings, codec keys.SQLCodec, dbID ID, tableName string,
) ID {
	if dbID != SystemDB.GetID() {
		return InvalidID
	}

	if settings != nil &&
		!settings.Version.IsActive(ctx, clusterversion.VersionNamespaceTableWithSchemas) &&
		tableName == NamespaceTableName {
		return DeprecatedNamespaceTable.ID
	}
	systemTenant := boolToInt(codec.ForSystemTenant())
	dbID, ok := systemTableIDCache[systemTenant][tableName]
	if !ok {
		return InvalidID
	}
	return dbID
}
