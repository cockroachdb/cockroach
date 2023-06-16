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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	ids           catalog.DescriptorIDSet
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

// firstNonSystemDescriptorID is the initial value of the descriptor ID generator
// and thus is the value at which we generate the first non-system descriptor
// ID. In clusters which have been upgraded, there may be non-system
// descriptors with IDs smaller that this. This is unexported very
// intentionally; we may change this value and it should not be relied upon.
// Note that this is in the bootstrap package because it's the value at
// bootstrap time only.
//
// This value is chosen in partly for its aesthetics and partly because it
// still fits in the smaller varint representation. Nothing should break
// other than datadriven tests if this number changes. Entropy will surely
// lead to more tests implicitly relying on this number, but hopefully not
// many. Even once the number of system tables surpasses this number, things
// would be okay, but the number of tests that'd need to be rewritten per new
// system table would go up a bunch.
const firstNonSystemDescriptorID = 100

// AddDescriptor adds a new non-config descriptor to the system schema.
func (ms *MetadataSchema) AddDescriptor(desc catalog.Descriptor) {
	switch id := desc.GetID(); id {
	case descpb.InvalidID:
		if _, isTable := desc.(catalog.TableDescriptor); !isTable {
			log.Fatalf(context.TODO(), "only system tables may have dynamic IDs, got %T for %s",
				desc, desc.GetName())
		}
		mut := desc.NewBuilder().BuildCreatedMutable().(*tabledesc.Mutable)
		mut.ID = ms.allocateID()
		desc = mut.ImmutableCopy()
	default:
		if ms.ids.Contains(id) {
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
			return iterutil.Map(err)
		}
	}
	return nil
}

// FindDescriptorByName retrieves the descriptor with the specified
// name. It returns nil if no descriptor with this name was found.
func (ms MetadataSchema) FindDescriptorByName(name string) catalog.Descriptor {
	for _, desc := range ms.descs {
		if desc.GetName() == name {
			return desc
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
		value.SetInt(int64(ms.FirstNonSystemDescriptorID()))
		add(ms.codec.SequenceKey(keys.DescIDSequenceID), value)
		if ms.codec.ForSystemTenant() {
			// We need to also set the value of the legacy descriptor ID generator
			// until clusterversion.V23_1DescIDSequenceForSystemTenant is removed.
			legacyValue := roachpb.Value{}
			legacyValue.SetInt(int64(ms.FirstNonSystemDescriptorID()))
			add(keys.LegacyDescIDGenerator, legacyValue)
		}
	}
	// Generate initial values for the system database's public schema, which
	// doesn't have a descriptor.
	{
		publicSchemaValue := roachpb.Value{}
		publicSchemaValue.SetInt(int64(keys.SystemPublicSchemaID))
		nameInfo := descpb.NameInfo{ParentID: keys.SystemDatabaseID, Name: tree.PublicSchema}
		add(catalogkeys.EncodeNameKey(ms.codec, &nameInfo), publicSchemaValue)
	}

	// Generate initial values for system databases and tables, which have
	// static descriptors that were generated elsewhere.
	for _, desc := range ms.descs {
		// Create name metadata key.
		nameValue := roachpb.Value{}
		nameValue.SetInt(int64(desc.GetID()))
		add(catalogkeys.EncodeNameKey(ms.codec, desc), nameValue)

		// Set initial sequence values.
		if tbl, ok := desc.(catalog.TableDescriptor); ok && tbl.IsSequence() && tbl.GetID() != keys.DescIDSequenceID {
			// Note that we skip over the DescIDSequence here,
			// the value is initialized earlier in this function.
			// DescIDSequence is special cased such that there
			// is a special "descIDGenerator" for the system tenant.
			// Because of this, there is no DescIDSequence for
			// the system tenant and thus this loop over descriptors
			// will not initialize the value for the system tenant.
			value := roachpb.Value{}
			value.SetInt(tbl.GetSequenceOpts().Start)
			add(ms.codec.SequenceKey(uint32(tbl.GetID())), value)
		}

		// Create descriptor metadata key.
		descValue := roachpb.Value{}
		if err := descValue.SetProto(desc.DescriptorProto()); err != nil {
			log.Fatalf(context.TODO(), "could not marshal %v", desc)
		}
		add(catalogkeys.MakeDescMetadataKey(ms.codec, desc.GetID()), descValue)

		// Split on each system table.
		if desc.GetID() != keys.SystemDatabaseID {
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
	// So we drop all descriptor split points and replace them with split points
	// at the beginning and end of this tenant's keyspace.
	if ms.codec.ForSystemTenant() {
		for _, id := range ms.otherSplitIDs {
			splits = append(splits, roachpb.RKey(ms.codec.TablePrefix(id)))
		}
	} else {
		tenantSpan := ms.codec.TenantSpan()
		tenantStartKey := roachpb.RKey(tenantSpan.Key)
		tenantEndKey := roachpb.RKey(tenantSpan.EndKey)
		splits = []roachpb.RKey{tenantStartKey, tenantEndKey}
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

type initialValueStrings struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// InitialValuesToString returns a string representation of the return values
// of MetadataSchema.GetInitialValues. The tenant prefix is stripped.
func InitialValuesToString(ms MetadataSchema) string {
	kvs, splits := ms.GetInitialValues()
	// Collect the records.
	type record struct {
		k, v []byte
	}
	records := make([]record, 0, len(kvs)+len(splits))
	for _, kv := range kvs {
		records = append(records, record{k: kv.Key, v: kv.Value.TagAndDataBytes()})
	}
	p := ms.codec.TenantPrefix()
	pNext := p.PrefixEnd()
	for _, s := range splits {
		// Filter out the tenant end key because it does not have the same prefix.
		if bytes.HasPrefix(s, pNext) {
			continue
		}
		records = append(records, record{k: s})
	}
	// Strip the tenant prefix if there is one.
	for i, r := range records {
		if !bytes.Equal(p, r.k[:len(p)]) {
			panic("unexpected prefix")
		}
		records[i].k = r.k[len(p):]
	}
	// Build the string representation.
	s := make([]initialValueStrings, len(records))
	for i, r := range records {
		s[i].Key = hex.EncodeToString(r.k)
		s[i].Value = hex.EncodeToString(r.v)
	}
	// Sort the records by key.
	sort.Slice(s, func(i, j int) bool {
		return s[i].Key < s[j].Key
	})
	// Build the string representation.
	var sb strings.Builder
	sb.WriteRune('[')
	for i, r := range s {
		if i > 0 {
			sb.WriteRune(',')
		}
		b, err := json.Marshal(r)
		if err != nil {
			panic(err)
		}
		sb.Write(b)
		sb.WriteRune('\n')
	}
	sb.WriteRune(']')
	return sb.String()
}

// InitialValuesFromString is the reciprocal to InitialValuesToString and
// appends the tenant prefix from the given codec.
func InitialValuesFromString(
	codec keys.SQLCodec, str string,
) (kvs []roachpb.KeyValue, splits []roachpb.RKey, _ error) {
	p := codec.TenantPrefix()
	var s []initialValueStrings
	if err := json.Unmarshal([]byte(str), &s); err != nil {
		return nil, nil, err
	}
	for i, r := range s {
		k, err := hex.DecodeString(r.Key)
		if err != nil {
			return nil, nil, errors.Errorf("failed to decode hex key %s for record #%d", r.Key, i+1)
		}
		v, err := hex.DecodeString(r.Value)
		if err != nil {
			return nil, nil, errors.Errorf("failed to decode hex value %s fo record #%d", r.Value, i+1)
		}
		k = append(p[:len(p):len(p)], k...)
		if len(v) == 0 {
			splits = append(splits, k)
		} else {
			kv := roachpb.KeyValue{Key: k}
			kv.Value.SetTagAndData(v)
			kvs = append(kvs, kv)
		}
	}
	// Add back the filtered out tenant end key.
	if !codec.ForSystemTenant() {
		splits = append(splits, roachpb.RKey(p.PrefixEnd()))
	}
	return kvs, splits, nil
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

// FirstNonSystemDescriptorID returns the largest system descriptor ID in this
// schema.
func (ms MetadataSchema) FirstNonSystemDescriptorID() descpb.ID {
	if next := ms.allocateID(); next > firstNonSystemDescriptorID {
		return next
	}
	return firstNonSystemDescriptorID
}

func (ms MetadataSchema) allocateID() (nextID descpb.ID) {
	maxID := descpb.ID(keys.MaxReservedDescID)
	for _, d := range ms.descs {
		if d.GetID() > maxID {
			maxID = d.GetID()
		}
	}
	return maxID + 1
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
	target.AddDescriptor(systemschema.DescIDSequence)
	target.AddDescriptorForSystemTenant(systemschema.TenantsTable)

	// Add all the other system tables.
	target.AddDescriptor(systemschema.LeaseTable())
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
	target.AddDescriptor(systemschema.SqllivenessTable())
	target.AddDescriptor(systemschema.MigrationsTable)

	// Tables introduced in 21.1.

	target.AddDescriptor(systemschema.JoinTokensTable)

	// Tables introduced in 21.2.

	target.AddDescriptor(systemschema.StatementStatisticsTable)
	target.AddDescriptor(systemschema.TransactionStatisticsTable)
	target.AddDescriptor(systemschema.DatabaseRoleSettingsTable)
	target.AddDescriptorForSystemTenant(systemschema.TenantUsageTable)
	target.AddDescriptor(systemschema.SQLInstancesTable())
	target.AddDescriptorForSystemTenant(systemschema.SpanConfigurationsTable)

	// Tables introduced in 22.1.

	target.AddDescriptorForSystemTenant(systemschema.TenantSettingsTable)
	target.AddDescriptorForNonSystemTenant(systemschema.SpanCountTable)

	// Tables introduced in 22.2.
	target.AddDescriptor(systemschema.SystemPrivilegeTable)
	target.AddDescriptor(systemschema.SystemExternalConnectionsTable)
	target.AddDescriptor(systemschema.RoleIDSequence)

	// Tables introduced in 23.1.
	target.AddDescriptor(systemschema.SystemJobInfoTable)
	target.AddDescriptor(systemschema.SpanStatsUniqueKeysTable)
	target.AddDescriptor(systemschema.SpanStatsBucketsTable)
	target.AddDescriptor(systemschema.SpanStatsSamplesTable)
	target.AddDescriptor(systemschema.SpanStatsTenantBoundariesTable)
	target.AddDescriptorForSystemTenant(systemschema.SystemTaskPayloadsTable)
	target.AddDescriptorForSystemTenant(systemschema.SystemTenantTasksTable)
	target.AddDescriptor(systemschema.StatementActivityTable)
	target.AddDescriptor(systemschema.TransactionActivityTable)
	target.AddDescriptorForSystemTenant(systemschema.TenantIDSequence)

	// Adding a new system table? It should be added here to the metadata schema,
	// and also created as a migration for older clusters.
	// If adding a call to AddDescriptor or AddDescriptorForSystemTenant, please
	// bump the value of NumSystemTablesForSystemTenant below. This constant is
	// just used for testing purposes.
}

// NumSystemTablesForSystemTenant is the number of system tables defined on
// the system tenant. This constant is only defined to avoid having to manually
// update auto stats tests every time a new system table is added.
const NumSystemTablesForSystemTenant = 51

// addSplitIDs adds a split point for each of the PseudoTableIDs to the supplied
// MetadataSchema.
func addSplitIDs(target *MetadataSchema) {
	target.AddSplitIDs(keys.PseudoTableIDs...)
}

// InitialZoneConfigKVs returns a list of KV pairs to seed `system.zones`. The
// list contains extra entries for the system tenant.
func InitialZoneConfigKVs(
	codec keys.SQLCodec,
	defaultZoneConfig *zonepb.ZoneConfig,
	defaultSystemZoneConfig *zonepb.ZoneConfig,
) (ret []roachpb.KeyValue) {
	const skippedColumnFamilyID = 0
	w := MakeKVWriter(codec, systemschema.ZonesTable, skippedColumnFamilyID)
	add := func(id uint32, zc *zonepb.ZoneConfig) {
		bytes, err := protoutil.Marshal(zc)
		if err != nil {
			panic(errors.NewAssertionErrorWithWrappedErrf(err, "could not marshal ZoneConfig for ID: %d", id))
		}
		kvs, err := w.RecordToKeyValues(tree.NewDInt(tree.DInt(id)), tree.NewDBytes(tree.DBytes(bytes)))
		if err != nil {
			panic(err)
		}
		ret = append(ret, kvs...)
	}

	// Both the system tenant and secondary tenants get their own RANGE DEFAULT
	// zone configuration.
	add(keys.RootNamespaceID, defaultZoneConfig)

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

	// Some reporting tables have shorter GC times.
	replicationConstraintStatsZoneConf := &zonepb.ZoneConfig{
		GC: &zonepb.GCPolicy{TTLSeconds: int32(systemschema.ReplicationConstraintStatsTableTTL.Seconds())},
	}
	replicationStatsZoneConf := &zonepb.ZoneConfig{
		GC: &zonepb.GCPolicy{TTLSeconds: int32(systemschema.ReplicationStatsTableTTL.Seconds())},
	}
	tenantUsageZoneConf := &zonepb.ZoneConfig{
		GC: &zonepb.GCPolicy{TTLSeconds: int32(systemschema.TenantUsageTableTTL.Seconds())},
	}

	// Liveness zone config entry with a shorter GC time.
	livenessZoneConf.GC.TTLSeconds = 10 * 60 // 10m

	add(keys.MetaRangesID, metaRangeZoneConf)
	add(keys.LivenessRangesID, livenessZoneConf)
	add(keys.SystemRangesID, systemZoneConf)
	add(keys.SystemDatabaseID, systemZoneConf)
	add(keys.ReplicationConstraintStatsTableID, replicationConstraintStatsZoneConf)
	add(keys.ReplicationStatsTableID, replicationStatsZoneConf)
	add(keys.TenantUsageTableID, tenantUsageZoneConf)

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
	addSystemTenantEntry(target)
}

// addSystemTenantEntry adds a kv pair to system.tenants to define the initial
// system tenant entry.
func addSystemTenantEntry(target *MetadataSchema) {
	info := mtinfopb.ProtoInfo{
		DeprecatedID:        roachpb.SystemTenantID.ToUint64(),
		DeprecatedDataState: mtinfopb.ProtoInfo_READY,
	}
	infoBytes, err := protoutil.Marshal(&info)
	if err != nil {
		panic(err)
	}

	// Find the system.tenant descriptor in the newly created catalog.
	desc := target.FindDescriptorByName(string(catconstants.TenantsTableName))
	if desc == nil {
		// No system.tenant table (we're likely in a secondary
		// tenant). Nothing to do.
		return
	}
	tenantsTableWriter := MakeKVWriter(target.codec, desc.(catalog.TableDescriptor))
	kvs, err := tenantsTableWriter.RecordToKeyValues(
		// ID
		tree.NewDInt(tree.DInt(roachpb.SystemTenantID.ToUint64())),
		// active -- deprecated.
		tree.MakeDBool(true),
		// info.
		tree.NewDBytes(tree.DBytes(infoBytes)),
		// name.
		tree.NewDString(catconstants.SystemTenantName),
		// data_state.
		tree.NewDInt(tree.DInt(mtinfopb.DataStateReady)),
		// service_mode.
		tree.NewDInt(tree.DInt(mtinfopb.ServiceModeShared)),
	)
	if err != nil {
		panic(err)
	}
	target.otherKV = append(target.otherKV, kvs...)
}

// TestingMinUserDescID returns the smallest user-created descriptor ID in a
// bootstrapped cluster.
func TestingMinUserDescID() uint32 {
	ms := MakeMetadataSchema(keys.SystemSQLCodec, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef())
	return uint32(ms.FirstNonSystemDescriptorID())
}

// TestingMinNonDefaultUserDescID returns the smallest user-creatable descriptor
// ID in a bootstrapped cluster.
func TestingMinNonDefaultUserDescID() uint32 {
	// Each default DB comes with a public schema descriptor.
	return TestingMinUserDescID() + uint32(len(catalogkeys.DefaultUserDBs))*2
}

// TestingUserDescID is a convenience function which returns a user ID offset
// from the minimum value allowed in a simple unit test setting.
func TestingUserDescID(offset uint32) uint32 {
	return TestingMinUserDescID() + offset
}

// TestingUserTableDataMin is a convenience function which returns the first
// user table data key in a simple unit test setting.
func TestingUserTableDataMin() roachpb.Key {
	return keys.SystemSQLCodec.TablePrefix(TestingUserDescID(0))
}
