// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keys

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// For a high-level overview of the keyspace layout, see the package comment in
// doc.go.

// These constants are single bytes for performance. They allow single-byte
// comparisons which are considerably faster than bytes.HasPrefix.
const (
	meta1PrefixByte  = roachpb.LocalMaxByte
	meta2PrefixByte  = '\x03'
	metaMaxByte      = '\x04'
	systemPrefixByte = metaMaxByte
	systemMaxByte    = '\x05'
	tenantPrefixByte = '\xfe'
)

// Constants to subdivide unsafe loss of quorum recovery data into groups.
// Currently we only store keys as they are applied, but might benefit from
// archiving them to make them more "durable".
const (
	appliedUnsafeReplicaRecoveryPrefix = "applied"
)

// Constants for system-reserved keys in the KV map.
//
// Note: Preserve group-wise ordering when adding new constants.
// Note: Update `keymap` in doc.go when adding new constants.
var (
	// MinKey is a minimum key value which sorts before all other keys.
	MinKey = roachpb.KeyMin
	// MaxKey is the infinity marker which is larger than any other key.
	MaxKey = roachpb.KeyMax

	// LocalPrefix is the prefix for all local keys.
	LocalPrefix = roachpb.LocalPrefix
	// LocalMax is the end of the local key range. It is itself a global key.
	LocalMax = roachpb.LocalMax

	// localSuffixLength specifies the length in bytes of all local
	// key suffixes.
	localSuffixLength = 4

	// There are five types of local key data enumerated below: replicated
	// range-ID, unreplicated range-ID, range local, store-local, and range lock
	// keys.

	// 1. Replicated Range-ID keys
	//
	// LocalRangeIDPrefix is the prefix identifying per-range data
	// indexed by Range ID. The Range ID is appended to this prefix,
	// encoded using EncodeUvarint. The specific sort of per-range
	// metadata is identified by one of the suffixes listed below, along
	// with potentially additional encoded key info, for instance in the
	// case of AbortSpan entry.
	LocalRangeIDPrefix = roachpb.RKey(makeKey(LocalPrefix, roachpb.Key("i")))
	// LocalRangeIDReplicatedInfix is the post-Range ID specifier for all Raft
	// replicated per-range data. By appending this after the Range ID, these
	// keys will be sorted directly before the local unreplicated keys for the
	// same Range ID, so they can be manipulated either together or individually
	// in a single scan.
	LocalRangeIDReplicatedInfix = []byte("r")
	// LocalAbortSpanSuffix is the suffix for AbortSpan entries. The
	// AbortSpan protects a transaction from re-reading its own intents
	// after it's been aborted.
	LocalAbortSpanSuffix = []byte("abc-")
	// localRangeFrozenStatusSuffix is DEPRECATED and remains to prevent reuse.
	localRangeFrozenStatusSuffix = []byte("fzn-")
	// LocalRangeGCThresholdSuffix is the suffix for the GC threshold. It keeps
	// the lgc- ("last GC") representation for backwards compatibility.
	LocalRangeGCThresholdSuffix = []byte("lgc-")
	// LocalRangeAppliedStateSuffix is the suffix for the range applied state
	// key.
	LocalRangeAppliedStateSuffix = []byte("rask")
	// This was previously used for the replicated RaftTruncatedState. It is no
	// longer used and this key has been removed via a migration. See
	// LocalRaftTruncatedStateSuffix for the corresponding unreplicated
	// RaftTruncatedState.
	_ = []byte("rftt")
	// LocalRangeLeaseSuffix is the suffix for a range lease.
	LocalRangeLeaseSuffix = []byte("rll-")
	// LocalRangePriorReadSummarySuffix is the suffix for a range's prior read
	// summary.
	LocalRangePriorReadSummarySuffix = []byte("rprs")
	// LocalRangeVersionSuffix is the suffix for the range version.
	LocalRangeVersionSuffix = []byte("rver")
	// LocalRangeStatsLegacySuffix is the suffix for range statistics.
	LocalRangeStatsLegacySuffix = []byte("stat")
	// localTxnSpanGCThresholdSuffix is DEPRECATED and remains to prevent reuse.
	localTxnSpanGCThresholdSuffix = []byte("tst-")

	// 2. Unreplicated Range-ID keys
	//
	// localRangeIDUnreplicatedInfix is the post-Range ID specifier for all
	// per-range data that is not fully Raft replicated. By appending this
	// after the Range ID, these keys will be sorted directly after the local
	// replicated keys for the same Range ID, so they can be manipulated either
	// together or individually in a single scan.
	localRangeIDUnreplicatedInfix = []byte("u")
	// LocalRangeTombstoneSuffix is the suffix for the range tombstone.
	//
	// NB: This suffix was originally named LocalRaftTombstoneSuffix, which is
	// why it starts off with "rft" as opposed to "rl".
	LocalRangeTombstoneSuffix = []byte("rftb")
	// LocalRaftHardStateSuffix is the Suffix for the raft HardState.
	LocalRaftHardStateSuffix = []byte("rfth")
	// localRaftLastIndexSuffix is DEPRECATED and remains to prevent reuse.
	localRaftLastIndexSuffix = []byte("rfti")
	// LocalRaftLogSuffix is the suffix for the raft log.
	LocalRaftLogSuffix = []byte("rftl")
	// LocalRaftReplicaIDSuffix is the suffix for the RaftReplicaID. This is
	// written when a replica is created.
	LocalRaftReplicaIDSuffix = []byte("rftr")
	// LocalRaftTruncatedStateSuffix is the suffix for the unreplicated
	// RaftTruncatedState.
	LocalRaftTruncatedStateSuffix = []byte("rftt")

	// LocalRangeLastReplicaGCTimestampSuffix is the suffix for a range's last
	// replica GC timestamp (for GC of old replicas).
	LocalRangeLastReplicaGCTimestampSuffix = []byte("rlrt")
	// localRangeLastVerificationTimestampSuffix is DEPRECATED and remains to
	// prevent reuse.
	localRangeLastVerificationTimestampSuffix = []byte("rlvt")

	// 3. Range local keys
	//
	// LocalRangePrefix is the prefix identifying per-range data indexed
	// by range key (either start key, or some key in the range). The
	// key is appended to this prefix, encoded using EncodeBytes. The
	// specific sort of per-range metadata is identified by one of the
	// suffixes listed below, along with potentially additional encoded
	// key info, such as the txn ID in the case of a transaction record.
	LocalRangePrefix = roachpb.Key(makeKey(LocalPrefix, roachpb.RKey("k")))
	LocalRangeMax    = LocalRangePrefix.PrefixEnd()
	// LocalRangeProbeSuffix is the suffix for keys for probing.
	LocalRangeProbeSuffix = roachpb.RKey("prbe")
	// LocalQueueLastProcessedSuffix is the suffix for replica queue state keys.
	LocalQueueLastProcessedSuffix = roachpb.RKey("qlpt")
	// LocalRangeDescriptorSuffix is the suffix for keys storing
	// range descriptors. The value is a struct of type RangeDescriptor.
	LocalRangeDescriptorSuffix = roachpb.RKey("rdsc")
	// LocalTransactionSuffix specifies the key suffix for
	// transaction records. The additional detail is the transaction id.
	LocalTransactionSuffix = roachpb.RKey("txn-")

	// 4. Store local keys
	//
	// LocalStorePrefix is the prefix identifying per-store data.
	LocalStorePrefix = makeKey(LocalPrefix, roachpb.Key("s"))
	// localStoreClusterVersionSuffix stores the cluster-wide version
	// information for this store, updated any time the operator
	// updates the minimum cluster version.
	localStoreClusterVersionSuffix = []byte("cver")
	// localStoreGossipSuffix stores gossip bootstrap metadata for this
	// store, updated any time new gossip hosts are encountered.
	localStoreGossipSuffix = []byte("goss")
	// localStoreHLCUpperBoundSuffix stores an upper bound to the wall time used by
	// the HLC.
	localStoreHLCUpperBoundSuffix = []byte("hlcu")
	// localStoreIdentSuffix stores an immutable identifier for this
	// store, created when the store is first bootstrapped.
	localStoreIdentSuffix = []byte("iden")
	// LocalStoreUnsafeReplicaRecoverySuffix is a suffix for temporary record
	// entries put when loss of quorum recovery operations are performed offline
	// on the store.
	// See StoreUnsafeReplicaRecoveryKey for details.
	localStoreUnsafeReplicaRecoverySuffix = makeKey([]byte("loqr"),
		[]byte(appliedUnsafeReplicaRecoveryPrefix))
	// LocalStoreUnsafeReplicaRecoveryKeyMin is the start of keyspace used to store
	// loss of quorum recovery record entries.
	LocalStoreUnsafeReplicaRecoveryKeyMin = MakeStoreKey(localStoreUnsafeReplicaRecoverySuffix, nil)
	// LocalStoreUnsafeReplicaRecoveryKeyMax is the end of keyspace used to store
	// loss of quorum recovery record entries.
	LocalStoreUnsafeReplicaRecoveryKeyMax = LocalStoreUnsafeReplicaRecoveryKeyMin.PrefixEnd()
	// localStoreNodeTombstoneSuffix stores key value pairs that map
	// nodeIDs to time of removal from cluster.
	localStoreNodeTombstoneSuffix = []byte("ntmb")
	// localStoreCachedSettingsSuffix stores the cached settings for node.
	localStoreCachedSettingsSuffix = []byte("stng")
	// LocalStoreCachedSettingsKeyMin is the start of span of possible cached settings keys.
	LocalStoreCachedSettingsKeyMin = MakeStoreKey(localStoreCachedSettingsSuffix, nil)
	// LocalStoreCachedSettingsKeyMax is the end of span of possible cached settings keys.
	LocalStoreCachedSettingsKeyMax = LocalStoreCachedSettingsKeyMin.PrefixEnd()
	// localStoreLastUpSuffix stores the last timestamp that a store's node
	// acknowledged that it was still running. This value will be regularly
	// refreshed on all stores for a running node; the intention of this value
	// is to allow a restarting node to discover approximately how long it has
	// been down without needing to retrieve liveness records from the cluster.
	localStoreLastUpSuffix = []byte("uptm")
	// localRemovedLeakedRaftEntriesSuffix is DEPRECATED and remains to prevent
	// reuse.
	localRemovedLeakedRaftEntriesSuffix = []byte("dlre")

	// 5. Lock table keys
	//
	// LocalRangeLockTablePrefix specifies the key prefix for the lock
	// table. It is immediately followed by the LockTableSingleKeyInfix,
	// and then the key being locked.
	//
	// The lock strength and txn UUID are not in the part of the key that
	// the keys package deals with. They are in the versioned part of the
	// key (see EngineKey.Version). This permits the storage engine to use
	// bloom filters when searching for all locks for a lockable key.
	//
	// Different lock strengths may use different value types. The exclusive
	// lock strength uses MVCCMetadata as the value type, since it does
	// double duty as a reference to a provisional MVCC value.
	// TODO(sumeer): remember to adjust this comment when adding locks of
	// other strengths, or range locks.
	LocalRangeLockTablePrefix = roachpb.Key(makeKey(LocalPrefix, roachpb.RKey("z")))
	LockTableSingleKeyInfix   = []byte("k")
	// LockTableSingleKeyStart is the inclusive start key of the key range
	// containing single key locks.
	LockTableSingleKeyStart = roachpb.Key(makeKey(LocalRangeLockTablePrefix, LockTableSingleKeyInfix))
	// LockTableSingleKeyEnd is the exclusive end key of the key range
	// containing single key locks.
	LockTableSingleKeyEnd = roachpb.Key(
		makeKey(LocalRangeLockTablePrefix, roachpb.Key(LockTableSingleKeyInfix).PrefixEnd()))

	// The global keyspace includes the meta{1,2}, system, system tenant SQL
	// keys, and non-system tenant SQL keys.

	// 1. Meta keys
	//
	// MetaMin is the start of the range of addressing keys.
	MetaMin = Meta1Prefix
	// MetaMax is the end of the range of addressing keys.
	MetaMax = roachpb.Key{metaMaxByte}
	// Meta1Prefix is the first level of key addressing. It is selected such that
	// all range addressing records sort before any system tables which they
	// might describe. The value is a RangeDescriptor struct.
	Meta1Prefix = roachpb.Key{meta1PrefixByte}
	// Meta1KeyMax is the end of the range of the first level of key addressing.
	// The value is a RangeDescriptor struct.
	Meta1KeyMax = roachpb.Key(makeKey(Meta1Prefix, roachpb.RKeyMax))
	// Meta2Prefix is the second level of key addressing. The value is a
	// RangeDescriptor struct.
	Meta2Prefix = roachpb.Key{meta2PrefixByte}
	// Meta2KeyMax is the end of the range of the second level of key addressing.
	// The value is a RangeDescriptor struct.
	Meta2KeyMax = roachpb.Key(makeKey(Meta2Prefix, roachpb.RKeyMax))

	// 2. System keys
	//
	// SystemPrefix indicates the beginning of the key range for
	// global, system data which are replicated across the cluster.
	SystemPrefix = roachpb.Key{systemPrefixByte}
	SystemMax    = roachpb.Key{systemMaxByte}
	// NodeLivenessPrefix specifies the key prefix for the node liveness
	// table.  Note that this should sort before the rest of the system
	// keyspace in order to limit the number of ranges which must use
	// expiration-based range leases instead of the more efficient
	// node-liveness epoch-based range leases (see
	// https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160210_range_leases.md)
	NodeLivenessPrefix = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("\x00liveness-")))
	// NodeLivenessKeyMax is the maximum value for any node liveness key.
	NodeLivenessKeyMax = NodeLivenessPrefix.PrefixEnd()
	//
	// BootstrapVersionKey is the key at which clusters bootstrapped with a version
	// > 1.0 persist the version at which they were bootstrapped.
	BootstrapVersionKey = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("bootstrap-version")))
	//
	// descIDGenerator is the global descriptor ID generator sequence used for
	// table and namespace IDs for the system tenant. All other tenants use a
	// SQL sequence for this purpose. See sqlEncoder.DescIDSequenceKey.
	descIDGenerator = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("desc-idgen")))
	// NodeIDGenerator is the global node ID generator sequence.
	NodeIDGenerator = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("node-idgen")))
	// RangeIDGenerator is the global range ID generator sequence.
	RangeIDGenerator = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("range-idgen")))
	// StoreIDGenerator is the global store ID generator sequence.
	StoreIDGenerator = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("store-idgen")))
	//
	// StatusPrefix specifies the key prefix to store all status details.
	StatusPrefix = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("status-")))
	// StatusNodePrefix stores all status info for nodes.
	StatusNodePrefix = roachpb.Key(makeKey(StatusPrefix, roachpb.RKey("node-")))
	//
	// MigrationPrefix specifies the key prefix to store all migration details.
	MigrationPrefix = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("system-version/")))
	// MigrationLease is the key that nodes must take a lease on in order to run
	// system migrations on the cluster.
	MigrationLease = roachpb.Key(makeKey(MigrationPrefix, roachpb.RKey("lease")))
	//
	// TimeseriesPrefix is the key prefix for all timeseries data.
	TimeseriesPrefix = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("tsd")))
	// TimeseriesKeyMax is the maximum value for any timeseries data.
	TimeseriesKeyMax = TimeseriesPrefix.PrefixEnd()
	//
	// SystemSpanConfigPrefix is the key prefix for all system span config data.
	//
	// We sort this at the end of the system keyspace to easily be able to exclude
	// it from the span configuration that applies over the system keyspace. This
	// is important because spans carved out from this range are used to store
	// system span configurations in the `system.span_configurations` table, and
	// as such, have special meaning associated with them; nothing is stored in
	// the range itself.
	SystemSpanConfigPrefix = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("\xffsys-scfg")))
	// SystemSpanConfigEntireKeyspace is the key prefix used to denote that the
	// associated system span configuration applies over the entire keyspace
	// (including all secondary tenants).
	SystemSpanConfigEntireKeyspace = roachpb.Key(makeKey(SystemSpanConfigPrefix, roachpb.RKey("host/all")))
	// SystemSpanConfigHostOnTenantKeyspace is the key prefix used to denote that
	// the associated system span configuration was applied by the host tenant
	// over the keyspace of a secondary tenant.
	SystemSpanConfigHostOnTenantKeyspace = roachpb.Key(makeKey(SystemSpanConfigPrefix, roachpb.RKey("host/ten/")))
	// SystemSpanConfigSecondaryTenantOnEntireKeyspace is the key prefix used to
	// denote that the associated system span configuration was applied by a
	// secondary tenant over its entire keyspace.
	SystemSpanConfigSecondaryTenantOnEntireKeyspace = roachpb.Key(makeKey(SystemSpanConfigPrefix, roachpb.RKey("ten/")))
	// SystemSpanConfigKeyMax is the maximum value for any system span config key.
	SystemSpanConfigKeyMax = SystemSpanConfigPrefix.PrefixEnd()

	// 3. System tenant SQL keys
	//
	// TODO(nvanbenschoten): Figure out what to do with all of these. At a
	// minimum, prefix them all with "System".

	// TableDataMin is the start of the range of table data keys.
	TableDataMin = SystemSQLCodec.TablePrefix(0)
	// TableDataMax is the end of the range of table data keys.
	TableDataMax = SystemSQLCodec.TablePrefix(math.MaxUint32).PrefixEnd()
	// ScratchRangeMin is a key used in tests to write arbitrary data without
	// overlapping with meta, system or tenant ranges.
	ScratchRangeMin = TableDataMax
	ScratchRangeMax = TenantPrefix
	//
	// SystemConfigSplitKey is the key to split at immediately prior to the
	// system config span. NB: Split keys need to be valid column keys.
	// TODO(bdarnell): this should be either roachpb.Key or RKey, not []byte.
	SystemConfigSplitKey = []byte(TableDataMin)
	// SystemConfigTableDataMax is the end key of system config span.
	SystemConfigTableDataMax = SystemSQLCodec.TablePrefix(MaxSystemConfigDescID + 1)
	//
	// NamespaceTableMin is the start key of system.namespace, which is a system
	// table that does not reside in the same range as other system tables.
	NamespaceTableMin = SystemSQLCodec.TablePrefix(NamespaceTableID)
	// NamespaceTableMax is the end key of system.namespace.
	NamespaceTableMax = SystemSQLCodec.TablePrefix(NamespaceTableID + 1)

	// 4. Non-system tenant SQL keys
	//
	// TenantPrefix is the prefix for all non-system tenant keys.
	TenantPrefix       = roachpb.Key{tenantPrefixByte}
	TenantTableDataMin = MakeTenantPrefix(roachpb.MinTenantID)
	TenantTableDataMax = MakeTenantPrefix(roachpb.MaxTenantID).PrefixEnd()
)

// Various IDs used by the structured data layer.
// NOTE: these must not change during the lifetime of a cluster.
const (
	// MaxSystemConfigDescID is the maximum system descriptor ID that will be
	// gossiped as part of the SystemConfig. Be careful adding new descriptors to
	// this ID range.
	MaxSystemConfigDescID = 10

	// MaxReservedDescID is the maximum descriptor ID in the reserved range.
	// In practice, what this means is that this is the highest-possible value
	// for a hard-coded descriptor ID.
	// Note that this is NO LONGER a higher bound on ALL POSSIBLE system
	// descriptor IDs.
	MaxReservedDescID = 49

	// RootNamespaceID is the ID of the root namespace.
	RootNamespaceID = 0

	// SystemDatabaseID and following are the database/table IDs for objects
	// in the system span.
	// NOTE: IDs must be <= MaxSystemConfigDescID.
	SystemDatabaseID = 1
	// DeprecatedNamespaceTableID was the tableID for the system.namespace table
	// for pre-20.1 clusters.
	DeprecatedNamespaceTableID = 2
	DescriptorTableID          = 3
	UsersTableID               = 4
	ZonesTableID               = 5
	SettingsTableID            = 6
	DescIDSequenceID           = 7
	TenantsTableID             = 8

	// IDs for the important columns and indexes in the zones table live here to
	// avoid introducing a dependency on sql/sqlbase throughout the codebase.
	ZonesTablePrimaryIndexID = 1
	ZonesTableConfigColumnID = 2
	ZonesTableConfigColFamID = 2

	DescriptorTablePrimaryKeyIndexID         = 1
	DescriptorTableDescriptorColID           = 2
	DescriptorTableDescriptorColFamID        = 2
	TenantsTablePrimaryKeyIndexID            = 1
	SpanConfigurationsTablePrimaryKeyIndexID = 1

	// Reserved IDs for other system tables. Note that some of these IDs refer
	// to "Ranges" instead of a Table - these IDs are needed to store custom
	// configuration for non-table ranges (e.g. Zone Configs).
	// NOTE: IDs must be <= MaxReservedDescID.
	LeaseTableID                         = 11
	EventLogTableID                      = 12
	RangeEventTableID                    = 13
	UITableID                            = 14
	JobsTableID                          = 15
	MetaRangesID                         = 16 // pseudo
	SystemRangesID                       = 17 // pseudo
	TimeseriesRangesID                   = 18 // pseudo
	WebSessionsTableID                   = 19
	TableStatisticsTableID               = 20
	LocationsTableID                     = 21
	LivenessRangesID                     = 22 // pseudo
	RoleMembersTableID                   = 23
	CommentsTableID                      = 24
	ReplicationConstraintStatsTableID    = 25
	ReplicationCriticalLocalitiesTableID = 26
	ReplicationStatsTableID              = 27
	ReportsMetaTableID                   = 28
	// PublicSchemaID refers to old references where Public schemas are
	// descriptorless.
	// TODO(richardjcai): This should be fully removed in 22.2.
	PublicSchemaID = 29 // pseudo
	// PublicSchemaIDForBackup is used temporarily to determine cases of
	// PublicSchemaID being used for backup.
	// We need to keep this around since backups created prior to 22.1 use 29
	// as the ID for a virtual public schema. In restores, we look for this 29
	// and synthesize a public schema with a descriptor when necessary.
	PublicSchemaIDForBackup = 29
	// SystemPublicSchemaID represents the ID used for the pseudo public
	// schema in the system database.
	SystemPublicSchemaID = 29 // pseudo
	// New NamespaceTableID for cluster version >= 20.1
	// Ensures that NamespaceTable does not get gossiped again
	NamespaceTableID                    = 30
	ProtectedTimestampsMetaTableID      = 31
	ProtectedTimestampsRecordsTableID   = 32
	RoleOptionsTableID                  = 33
	StatementBundleChunksTableID        = 34
	StatementDiagnosticsRequestsTableID = 35
	StatementDiagnosticsTableID         = 36
	ScheduledJobsTableID                = 37
	TenantsRangesID                     = 38 // pseudo
	SqllivenessID                       = 39
	MigrationsID                        = 40
	JoinTokensTableID                   = 41
	StatementStatisticsTableID          = 42
	TransactionStatisticsTableID        = 43
	DatabaseRoleSettingsTableID         = 44
	TenantUsageTableID                  = 45
	SQLInstancesTableID                 = 46
	SpanConfigurationsTableID           = 47
)

// CommentType the type of the schema object on which a comment has been
// applied.
type CommentType int

//go:generate stringer --type CommentType

const (
	// DatabaseCommentType comment on a database.
	DatabaseCommentType CommentType = 0
	// TableCommentType comment on a table/view/sequence.
	TableCommentType CommentType = 1
	// ColumnCommentType comment on a column.
	ColumnCommentType CommentType = 2
	// IndexCommentType comment on an index.
	IndexCommentType CommentType = 3
	// SchemaCommentType comment on a schema.
	SchemaCommentType CommentType = 4
	// ConstraintCommentType comment on a constraint.
	ConstraintCommentType CommentType = 5
)

const (
	// SequenceIndexID is the ID of the single index on each special single-column,
	// single-row sequence table.
	SequenceIndexID = 1
	// SequenceColumnFamilyID is the ID of the column family on each special single-column,
	// single-row sequence table.
	SequenceColumnFamilyID = 0
)

// PseudoTableIDs is the list of ids from above that are not real tables (i.e.
// there's no table descriptor). They're grouped here because the cluster
// bootstrap process needs to create splits for them; splits for the tables
// happen separately.
//
// TODO(ajwerner): There is no reason at all for these to have their own
// splits.
var PseudoTableIDs = []uint32{
	MetaRangesID,
	SystemRangesID,
	TimeseriesRangesID,
	LivenessRangesID,
	PublicSchemaID,
	TenantsRangesID,
}

// MaxPseudoTableID is the largest ID in PseudoTableIDs.
var MaxPseudoTableID = func() uint32 {
	var max uint32
	for _, id := range PseudoTableIDs {
		if max < id {
			max = id
		}
	}
	return max
}()
