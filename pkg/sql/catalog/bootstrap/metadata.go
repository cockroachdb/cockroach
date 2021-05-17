// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package bootstrap contains the metadata required to bootstrap the sql
// schema for a fresh cockroach cluster.
package bootstrap

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

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
	parentID descpb.ID
	desc     catalog.Descriptor
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
func (ms *MetadataSchema) AddDescriptor(parentID descpb.ID, desc catalog.Descriptor) {
	if id := desc.GetID(); id > keys.MaxReservedDescID {
		panic(errors.AssertionFailedf("invalid reserved table ID: %d > %d", id, keys.MaxReservedDescID))
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
	addDescriptor := func(parentID descpb.ID, desc catalog.Descriptor) {
		// Create name metadata key.
		value := roachpb.Value{}
		value.SetInt(int64(desc.GetID()))
		if parentID != keys.RootNamespaceID {
			ret = append(ret, roachpb.KeyValue{
				Key:   catalogkeys.MakePublicObjectNameKey(ms.codec, parentID, desc.GetName()),
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
					Key:   catalogkeys.MakeDatabaseNameKey(ms.codec, desc.GetName()),
					Value: value,
				},
				roachpb.KeyValue{
					Key:   catalogkeys.MakePublicSchemaNameKey(ms.codec, desc.GetID()),
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
			Key:   catalogkeys.MakeDescMetadataKey(ms.codec, desc.GetID()),
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
func (ms MetadataSchema) DescriptorIDs() descpb.IDs {
	descriptorIDs := descpb.IDs{}
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
var systemTableIDCache = func() [2]map[string]descpb.ID {
	cacheForTenant := func(systemTenant bool) map[string]descpb.ID {
		cache := make(map[string]descpb.ID)

		codec := keys.SystemSQLCodec
		if !systemTenant {
			codec = keys.MakeSQLCodec(roachpb.MinTenantID)
		}

		ms := MetadataSchema{codec: codec}
		addSystemDescriptorsToSchema(&ms)
		for _, d := range ms.descs {
			t, ok := d.desc.(catalog.TableDescriptor)
			if !ok || t.GetParentID() != keys.SystemDatabaseID || t.GetID() > keys.MaxReservedDescID {
				// We only cache table descriptors under 'system' with a
				// reserved table ID.
				continue
			}
			cache[t.GetName()] = t.GetID()
		}

		return cache
	}

	var cache [2]map[string]descpb.ID
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
	codec keys.SQLCodec, dbID descpb.ID, tableName string,
) descpb.ID {
	if dbID != systemschema.SystemDB.GetID() {
		return descpb.InvalidID
	}

	systemTenant := boolToInt(codec.ForSystemTenant())
	dbID, ok := systemTableIDCache[systemTenant][tableName]
	if !ok {
		return descpb.InvalidID
	}
	return dbID
}

// addSystemDescriptorsToSchema populates the supplied MetadataSchema
// with the system database and table descriptors. The descriptors for
// these objects exist statically in this file, but a MetadataSchema
// can be used to persist these descriptors to the cockroach store.
func addSystemDescriptorsToSchema(target *MetadataSchema) {
	// Add system database.
	target.AddDescriptor(keys.RootNamespaceID, systemschema.SystemDB)

	// Add system config tables.
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.NamespaceTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.DescriptorTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.UsersTable)
	if target.codec.ForSystemTenant() {
		target.AddDescriptor(keys.SystemDatabaseID, systemschema.ZonesTable)
	}
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.SettingsTable)
	if !target.codec.ForSystemTenant() {
		// Only add the descriptor ID sequence if this is a non-system tenant.
		// System tenants use the global descIDGenerator key. See #48513.
		target.AddDescriptor(keys.SystemDatabaseID, systemschema.DescIDSequence)
	}
	if target.codec.ForSystemTenant() {
		// Only add the tenant table if this is the system tenant.
		target.AddDescriptor(keys.SystemDatabaseID, systemschema.TenantsTable)
	}

	// Add all the other system tables.
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.LeaseTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.EventLogTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.RangeEventTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.UITable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.JobsTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.WebSessionsTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.RoleOptionsTable)

	// Tables introduced in 2.0, added here for 2.1.
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.TableStatisticsTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.LocationsTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.RoleMembersTable)

	// The CommentsTable has been introduced in 2.2. It was added here since it
	// was introduced, but it's also created as a migration for older clusters.
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.CommentsTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.ReportsMetaTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.ReplicationConstraintStatsTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.ReplicationStatsTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.ReplicationCriticalLocalitiesTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.ProtectedTimestampsMetaTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.ProtectedTimestampsRecordsTable)

	// Tables introduced in 20.1.

	target.AddDescriptor(keys.SystemDatabaseID, systemschema.StatementBundleChunksTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.StatementDiagnosticsRequestsTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.StatementDiagnosticsTable)

	// Tables introduced in 20.2.

	target.AddDescriptor(keys.SystemDatabaseID, systemschema.ScheduledJobsTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.SqllivenessTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.MigrationsTable)

	// Tables introduced in 21.1.

	target.AddDescriptor(keys.SystemDatabaseID, systemschema.JoinTokensTable)

	// Tables introduced in 21.2.

	target.AddDescriptor(keys.SystemDatabaseID, systemschema.StatementStatisticsTable)
	target.AddDescriptor(keys.SystemDatabaseID, systemschema.TransactionStatisticsTable)
}

// addSplitIDs adds a split point for each of the PseudoTableIDs to the supplied
// MetadataSchema.
func addSplitIDs(target *MetadataSchema) {
	target.AddSplitIDs(keys.PseudoTableIDs...)
}

// Create a kv pair for the zone config for the given key and config value.
func createZoneConfigKV(
	keyID int, codec keys.SQLCodec, zoneConfig *zonepb.ZoneConfig,
) roachpb.KeyValue {
	value := roachpb.Value{}
	if err := value.SetProto(zoneConfig); err != nil {
		panic(errors.AssertionFailedf("could not marshal ZoneConfig for ID: %d: %s", keyID, err))
	}
	return roachpb.KeyValue{
		Key:   codec.ZoneKey(uint32(keyID)),
		Value: value,
	}
}

// addZoneConfigKVsToSchema adds a kv pair for each of the statically defined
// zone configurations that should be populated in a newly bootstrapped cluster.
func addZoneConfigKVsToSchema(
	target *MetadataSchema,
	defaultZoneConfig *zonepb.ZoneConfig,
	defaultSystemZoneConfig *zonepb.ZoneConfig,
) {
	// If this isn't the system tenant, don't add any zone configuration keys.
	// Only the system tenant has a zone table.
	if !target.codec.ForSystemTenant() {
		return
	}

	// Adding a new system table? It should be added here to the metadata schema,
	// and also created as a migration for older cluster. The includedInBootstrap
	// field should be set on the migration.

	target.otherKV = append(target.otherKV,
		createZoneConfigKV(keys.RootNamespaceID, target.codec, defaultZoneConfig))

	systemZoneConf := defaultSystemZoneConfig
	metaRangeZoneConf := protoutil.Clone(defaultSystemZoneConfig).(*zonepb.ZoneConfig)
	livenessZoneConf := protoutil.Clone(defaultSystemZoneConfig).(*zonepb.ZoneConfig)

	// .meta zone config entry with a shorter GC time.
	metaRangeZoneConf.GC.TTLSeconds = 60 * 60 // 1h
	target.otherKV = append(target.otherKV,
		createZoneConfigKV(keys.MetaRangesID, target.codec, metaRangeZoneConf))

	// Some reporting tables have shorter GC times.
	replicationConstraintStatsZoneConf := &zonepb.ZoneConfig{
		GC: &zonepb.GCPolicy{TTLSeconds: int32(systemschema.ReplicationConstraintStatsTableTTL.Seconds())},
	}
	replicationStatsZoneConf := &zonepb.ZoneConfig{
		GC: &zonepb.GCPolicy{TTLSeconds: int32(systemschema.ReplicationStatsTableTTL.Seconds())},
	}

	// Liveness zone config entry with a shorter GC time.
	livenessZoneConf.GC.TTLSeconds = 10 * 60 // 10m
	target.otherKV = append(target.otherKV,
		createZoneConfigKV(keys.LivenessRangesID, target.codec, livenessZoneConf))
	target.otherKV = append(target.otherKV,
		createZoneConfigKV(keys.SystemRangesID, target.codec, systemZoneConf))
	target.otherKV = append(target.otherKV,
		createZoneConfigKV(keys.SystemDatabaseID, target.codec, systemZoneConf))
	target.otherKV = append(target.otherKV,
		createZoneConfigKV(keys.ReplicationConstraintStatsTableID, target.codec, replicationConstraintStatsZoneConf))
	target.otherKV = append(target.otherKV,
		createZoneConfigKV(keys.ReplicationStatsTableID, target.codec, replicationStatsZoneConf))
}

// addSystemDatabaseToSchema populates the supplied MetadataSchema with the
// System database, its tables and zone configurations.
func addSystemDatabaseToSchema(
	target *MetadataSchema,
	defaultZoneConfig *zonepb.ZoneConfig,
	defaultSystemZoneConfig *zonepb.ZoneConfig,
) {
	addSystemDescriptorsToSchema(target)
	addSplitIDs(target)
	addZoneConfigKVsToSchema(target, defaultZoneConfig, defaultSystemZoneConfig)
}
