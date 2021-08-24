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
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
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
	descs         []catalog.Descriptor
	otherSplitIDs []uint32
	otherKV       []roachpb.KeyValue
}

// MakeMetadataSchema constructs a new MetadataSchema value which constructs
// the "system" database.
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
func (ms *MetadataSchema) AddDescriptor(desc catalog.Descriptor) {
	for _, d := range ms.descs {
		if d.GetID() == desc.GetID() {
			log.Fatalf(context.TODO(), "adding descriptor with duplicate ID: %v", desc)
		}
	}
	ms.descs = append(ms.descs, desc)
}

// AddDescriptorForSystemTenant is like AddDescriptor but only for the system
// tenant.
func (ms *MetadataSchema) AddDescriptorForSystemTenant(desc catalog.Descriptor) {
	if !ms.codec.ForSystemTenant() {
		return
	}
	ms.AddDescriptor(desc)
}

// AddDescriptorForNonSystemTenant is like AddDescriptor but only for non-system
// tenants.
func (ms *MetadataSchema) AddDescriptorForNonSystemTenant(desc catalog.Descriptor) {
	if ms.codec.ForSystemTenant() {
		return
	}
	ms.AddDescriptor(desc)
}

// ForEachCatalogDescriptor iterates through each catalog.Descriptor object in
// this schema.
// iterutil.StopIteration is supported.
func (ms MetadataSchema) ForEachCatalogDescriptor(fn func(desc catalog.Descriptor) error) error {
	for _, desc := range ms.descs {
		if err := fn(desc); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
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
	add := func(key roachpb.Key, value roachpb.Value) {
		ret = append(ret, roachpb.KeyValue{Key: key, Value: value})
	}

	// Save the ID generator value, which will generate descriptor IDs for user
	// objects.
	{
		value := roachpb.Value{}
		value.SetInt(keys.MinUserDescID)
		add(ms.codec.DescIDSequenceKey(), value)
	}

	// Generate initial values for system databases and tables, which have
	// static descriptors that were generated elsewhere.
	for _, desc := range ms.descs {
		// Create name metadata key.
		nameValue := roachpb.Value{}
		nameValue.SetInt(int64(desc.GetID()))
		if desc.GetParentID() != keys.RootNamespaceID {
			add(catalogkeys.MakePublicObjectNameKey(ms.codec, desc.GetParentID(), desc.GetName()), nameValue)
		} else {
			// Initializing a database. Databases must be initialized with
			// the public schema, as all tables are scoped under the public schema.
			add(catalogkeys.MakeDatabaseNameKey(ms.codec, desc.GetName()), nameValue)
			publicSchemaValue := roachpb.Value{}
			publicSchemaValue.SetInt(int64(keys.PublicSchemaID))
			add(catalogkeys.MakePublicSchemaNameKey(ms.codec, desc.GetID()), publicSchemaValue)
		}

		// Create descriptor metadata key.
		descValue := roachpb.Value{}
		if err := descValue.SetProto(desc.DescriptorProto()); err != nil {
			log.Fatalf(context.TODO(), "could not marshal %v", desc)
		}
		add(catalogkeys.MakeDescMetadataKey(ms.codec, desc.GetID()), descValue)
		if desc.GetID() > keys.MaxSystemConfigDescID {
			splits = append(splits, roachpb.RKey(ms.codec.TablePrefix(uint32(desc.GetID()))))
		}
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
	for _, d := range ms.descs {
		descriptorIDs = append(descriptorIDs, d.GetID())
	}
	sort.Sort(descriptorIDs)
	return descriptorIDs
}

// addSystemDescriptorsToSchema populates the supplied MetadataSchema
// with the system database and table descriptors. The descriptors for
// these objects exist statically in this file, but a MetadataSchema
// can be used to persist these descriptors to the cockroach store.
func addSystemDescriptorsToSchema(target *MetadataSchema) {
	// Add system database.
	target.AddDescriptor(systemschema.SystemDB)

	// Add system config tables.
	target.AddDescriptor(systemschema.NamespaceTable)
	target.AddDescriptor(systemschema.DescriptorTable)
	target.AddDescriptor(systemschema.UsersTable)
	target.AddDescriptor(systemschema.ZonesTable)
	target.AddDescriptor(systemschema.SettingsTable)
	// Only add the descriptor ID sequence if this is a non-system tenant.
	// System tenants use the global descIDGenerator key. See #48513.
	target.AddDescriptorForNonSystemTenant(systemschema.DescIDSequence)
	target.AddDescriptorForSystemTenant(systemschema.TenantsTable)

	// Add all the other system tables.
	target.AddDescriptor(systemschema.LeaseTable)
	target.AddDescriptor(systemschema.EventLogTable)
	target.AddDescriptor(systemschema.RangeEventTable)
	target.AddDescriptor(systemschema.UITable)
	target.AddDescriptor(systemschema.JobsTable)
	target.AddDescriptor(systemschema.WebSessionsTable)
	target.AddDescriptor(systemschema.RoleOptionsTable)

	// Tables introduced in 2.0, added here for 2.1.
	target.AddDescriptor(systemschema.TableStatisticsTable)
	target.AddDescriptor(systemschema.LocationsTable)
	target.AddDescriptor(systemschema.RoleMembersTable)

	// The CommentsTable has been introduced in 2.2. It was added here since it
	// was introduced, but it's also created as a migration for older clusters.
	target.AddDescriptor(systemschema.CommentsTable)
	target.AddDescriptor(systemschema.ReportsMetaTable)
	target.AddDescriptor(systemschema.ReplicationConstraintStatsTable)
	target.AddDescriptor(systemschema.ReplicationStatsTable)
	target.AddDescriptor(systemschema.ReplicationCriticalLocalitiesTable)
	target.AddDescriptor(systemschema.ProtectedTimestampsMetaTable)
	target.AddDescriptor(systemschema.ProtectedTimestampsRecordsTable)

	// Tables introduced in 20.1.

	target.AddDescriptor(systemschema.StatementBundleChunksTable)
	target.AddDescriptor(systemschema.StatementDiagnosticsRequestsTable)
	target.AddDescriptor(systemschema.StatementDiagnosticsTable)

	// Tables introduced in 20.2.

	target.AddDescriptor(systemschema.ScheduledJobsTable)
	target.AddDescriptor(systemschema.SqllivenessTable)
	target.AddDescriptor(systemschema.MigrationsTable)

	// Tables introduced in 21.1.

	target.AddDescriptor(systemschema.JoinTokensTable)

	// Tables introduced in 21.2.

	target.AddDescriptor(systemschema.StatementStatisticsTable)
	target.AddDescriptor(systemschema.TransactionStatisticsTable)
	target.AddDescriptor(systemschema.DatabaseRoleSettingsTable)
	target.AddDescriptorForSystemTenant(systemschema.TenantUsageTable)
	target.AddDescriptor(systemschema.SQLInstancesTable)
	target.AddDescriptorForSystemTenant(systemschema.SpanConfigurationsTable)

	// Adding a new system table? It should be added here to the metadata schema,
	// and also created as a migration for older clusters. The includedInBootstrap
	// field should be set on the migration.
}

// addSplitIDs adds a split point for each of the PseudoTableIDs to the supplied
// MetadataSchema.
func addSplitIDs(target *MetadataSchema) {
	target.AddSplitIDs(keys.PseudoTableIDs...)
}

// createZoneConfigKV creates a kv pair for the zone config for the given key
// and config value.
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

// InitialZoneConfigKVs returns a list of KV pairs to seed `system.zones`. The
// list contains extra entries for the system tenant.
func InitialZoneConfigKVs(
	codec keys.SQLCodec,
	defaultZoneConfig *zonepb.ZoneConfig,
	defaultSystemZoneConfig *zonepb.ZoneConfig,
) (ret []roachpb.KeyValue) {
	// Both the system tenant and secondary tenants get their own RANGE DEFAULT
	// zone configuration.
	ret = append(ret,
		createZoneConfigKV(keys.RootNamespaceID, codec, defaultZoneConfig))

	if !codec.ForSystemTenant() {
		return ret
	}

	// Only the system tenant has zone configs over {META, LIVENESS, SYSTEM}
	// ranges. Additionally, some reporting tables have custom zone configs set,
	// but only for the system tenant.
	systemZoneConf := defaultSystemZoneConfig
	metaRangeZoneConf := protoutil.Clone(defaultSystemZoneConfig).(*zonepb.ZoneConfig)
	livenessZoneConf := protoutil.Clone(defaultSystemZoneConfig).(*zonepb.ZoneConfig)

	// .meta zone config entry with a shorter GC time.
	metaRangeZoneConf.GC.TTLSeconds = 60 * 60 // 1h
	ret = append(ret,
		createZoneConfigKV(keys.MetaRangesID, codec, metaRangeZoneConf))

	// Some reporting tables have shorter GC times.
	replicationConstraintStatsZoneConf := &zonepb.ZoneConfig{
		GC: &zonepb.GCPolicy{TTLSeconds: int32(systemschema.ReplicationConstraintStatsTableTTL.Seconds())},
	}
	replicationStatsZoneConf := &zonepb.ZoneConfig{
		GC: &zonepb.GCPolicy{TTLSeconds: int32(systemschema.ReplicationStatsTableTTL.Seconds())},
	}

	// Liveness zone config entry with a shorter GC time.
	livenessZoneConf.GC.TTLSeconds = 10 * 60 // 10m
	ret = append(ret,
		createZoneConfigKV(keys.LivenessRangesID, codec, livenessZoneConf))
	ret = append(ret,
		createZoneConfigKV(keys.SystemRangesID, codec, systemZoneConf))
	ret = append(ret,
		createZoneConfigKV(keys.SystemDatabaseID, codec, systemZoneConf))
	ret = append(ret,
		createZoneConfigKV(keys.ReplicationConstraintStatsTableID, codec, replicationConstraintStatsZoneConf))
	ret = append(ret,
		createZoneConfigKV(keys.ReplicationStatsTableID, codec, replicationStatsZoneConf))

	return ret
}

// addZoneConfigKVsToSchema adds a kv pair for each of the statically defined
// zone configurations that should be populated in a newly bootstrapped cluster.
func addZoneConfigKVsToSchema(
	target *MetadataSchema,
	defaultZoneConfig *zonepb.ZoneConfig,
	defaultSystemZoneConfig *zonepb.ZoneConfig,
) {
	kvs := InitialZoneConfigKVs(target.codec, defaultZoneConfig, defaultSystemZoneConfig)
	target.otherKV = append(target.otherKV, kvs...)
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
