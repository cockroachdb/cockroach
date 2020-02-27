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
	localPrefixByte  = '\x01'
	localMaxByte     = '\x02'
	meta1PrefixByte  = localMaxByte
	meta2PrefixByte  = '\x03'
	metaMaxByte      = '\x04'
	systemPrefixByte = metaMaxByte
	systemMaxByte    = '\x05'
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

	// localPrefix is the prefix for all local keys.
	localPrefix = roachpb.Key{localPrefixByte}
	// LocalMax is the end of the local key range. It is itself a global
	// key.
	LocalMax = roachpb.Key{localMaxByte}

	// localSuffixLength specifies the length in bytes of all local
	// key suffixes.
	localSuffixLength = 4

	// There are four types of local key data enumerated below: replicated
	// range-ID, unreplicated range-ID, range local, and store-local keys.

	// 1. Replicated Range-ID keys
	//
	// LocalRangeIDPrefix is the prefix identifying per-range data
	// indexed by Range ID. The Range ID is appended to this prefix,
	// encoded using EncodeUvarint. The specific sort of per-range
	// metadata is identified by one of the suffixes listed below, along
	// with potentially additional encoded key info, for instance in the
	// case of AbortSpan entry.
	//
	// NOTE: LocalRangeIDPrefix must be kept in sync with the value
	// in storage/engine/rocksdb/db.cc.
	LocalRangeIDPrefix = roachpb.RKey(makeKey(localPrefix, roachpb.Key("i")))
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
	// LocalRangeLastGCSuffix is the suffix for the last GC.
	LocalRangeLastGCSuffix = []byte("lgc-")
	// LocalRangeAppliedStateSuffix is the suffix for the range applied state
	// key.
	LocalRangeAppliedStateSuffix = []byte("rask")
	// LocalRaftAppliedIndexLegacySuffix is the suffix for the raft applied index.
	LocalRaftAppliedIndexLegacySuffix = []byte("rfta")
	// LocalRaftTruncatedStateLegacySuffix is the suffix for the legacy
	// RaftTruncatedState. See VersionUnreplicatedRaftTruncatedState.
	// Note: This suffix is also used for unreplicated Range-ID keys.
	LocalRaftTruncatedStateLegacySuffix = []byte("rftt")
	// LocalRangeLeaseSuffix is the suffix for a range lease.
	LocalRangeLeaseSuffix = []byte("rll-")
	// LocalLeaseAppliedIndexLegacySuffix is the suffix for the applied lease
	// index.
	LocalLeaseAppliedIndexLegacySuffix = []byte("rlla")
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
	//
	// NOTE: LocalRangePrefix must be kept in sync with the value in
	// storage/engine/rocksdb/db.cc.
	LocalRangePrefix = roachpb.Key(makeKey(localPrefix, roachpb.RKey("k")))
	LocalRangeMax    = LocalRangePrefix.PrefixEnd()
	// LocalQueueLastProcessedSuffix is the suffix for replica queue state keys.
	LocalQueueLastProcessedSuffix = roachpb.RKey("qlpt")
	// LocalRangeDescriptorJointSuffix is the suffix for keys storing
	// range descriptors. The value is a struct of type RangeDescriptor.
	//
	// TODO(tbg): decide what to actually store here. This is still unused.
	LocalRangeDescriptorJointSuffix = roachpb.RKey("rdjt")
	// LocalRangeDescriptorSuffix is the suffix for keys storing
	// range descriptors. The value is a struct of type RangeDescriptor.
	LocalRangeDescriptorSuffix = roachpb.RKey("rdsc")
	// LocalTransactionSuffix specifies the key suffix for
	// transaction records. The additional detail is the transaction id.
	// NOTE: if this value changes, it must be updated in C++
	// (storage/engine/rocksdb/db.cc).
	LocalTransactionSuffix = roachpb.RKey("txn-")

	// 4. Store local keys
	//
	// localStorePrefix is the prefix identifying per-store data.
	localStorePrefix = makeKey(localPrefix, roachpb.Key("s"))
	// localStoreSuggestedCompactionSuffix stores suggested compactions to
	// be aggregated and processed on the store.
	localStoreSuggestedCompactionSuffix = []byte("comp")
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
	// localStoreLastUpSuffix stores the last timestamp that a store's node
	// acknowledged that it was still running. This value will be regularly
	// refreshed on all stores for a running node; the intention of this value
	// is to allow a restarting node to discover approximately how long it has
	// been down without needing to retrieve liveness records from the cluster.
	localStoreLastUpSuffix = []byte("uptm")
	// localRemovedLeakedRaftEntriesSuffix is DEPRECATED and remains to prevent
	// reuse.
	localRemovedLeakedRaftEntriesSuffix = []byte("dlre")
	// LocalStoreSuggestedCompactionsMin is the start of the span of
	// possible suggested compaction keys for a store.
	LocalStoreSuggestedCompactionsMin = MakeStoreKey(localStoreSuggestedCompactionSuffix, nil)
	// LocalStoreSuggestedCompactionsMax is the end of the span of
	// possible suggested compaction keys for a store.
	LocalStoreSuggestedCompactionsMax = LocalStoreSuggestedCompactionsMin.PrefixEnd()

	// The global keyspace includes the meta{1,2}, system, and SQL keys.

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
	// BootstrapVersion is the key at which clusters bootstrapped with a version
	// > 1.0 persist the version at which they were bootstrapped.
	BootstrapVersionKey = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("bootstrap-version")))
	//
	// DescIDGenerator is the global descriptor ID generator sequence used for
	// table and namespace IDs.
	DescIDGenerator = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("desc-idgen")))
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
	// MigrationKeyMax is the maximum value for any system migration key.
	MigrationKeyMax = MigrationPrefix.PrefixEnd()
	//
	// TimeseriesPrefix is the key prefix for all timeseries data.
	TimeseriesPrefix = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("tsd")))
	// TimeseriesKeyMax is the maximum value for any timeseries data.
	TimeseriesKeyMax = TimeseriesPrefix.PrefixEnd()

	// 3. SQL keys
	//
	// TableDataMin is the start of the range of table data keys.
	TableDataMin = roachpb.Key(MakeTablePrefix(0))
	// TableDataMin is the end of the range of table data keys.
	TableDataMax = roachpb.Key(MakeTablePrefix(math.MaxUint32))
	//
	// SystemConfigSplitKey is the key to split at immediately prior to the
	// system config span. NB: Split keys need to be valid column keys.
	// TODO(bdarnell): this should be either roachpb.Key or RKey, not []byte.
	SystemConfigSplitKey = []byte(TableDataMin)
	// SystemConfigTableDataMax is the end key of system config span.
	SystemConfigTableDataMax = roachpb.Key(MakeTablePrefix(MaxSystemConfigDescID + 1))
	//
	// NamespaceTableMin is the start key of system.namespace, which is a system
	// table that does not reside in the same range as other system tables.
	NamespaceTableMin = roachpb.Key(MakeTablePrefix(NamespaceTableID))
	// NamespaceTableMax is the end key of system.namespace.
	NamespaceTableMax = roachpb.Key(MakeTablePrefix(NamespaceTableID + 1))
	//
	// UserTableDataMin is the start key of user structured data.
	UserTableDataMin = roachpb.Key(MakeTablePrefix(MinUserDescID))
)

// Various IDs used by the structured data layer.
// NOTE: these must not change during the lifetime of a cluster.
const (
	// MaxSystemConfigDescID is the maximum system descriptor ID that will be
	// gossiped as part of the SystemConfig. Be careful adding new descriptors to
	// this ID range.
	MaxSystemConfigDescID = 10

	// MaxReservedDescID is the maximum value of reserved descriptor
	// IDs. Reserved IDs are used by namespaces and tables used internally by
	// cockroach.
	MaxReservedDescID = 49

	// MinUserDescID is the first descriptor ID available for user
	// structured data.
	MinUserDescID = MaxReservedDescID + 1

	// MinNonPredefinedUserDescID is the first descriptor ID used by
	// user-level objects that are not created automatically on empty
	// clusters (default databases).
	MinNonPredefinedUserDescID = MinUserDescID + 2

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

	// IDs for the important columns and indexes in the zones table live here to
	// avoid introducing a dependency on sql/sqlbase throughout the codebase.
	ZonesTablePrimaryIndexID = 1
	ZonesTableConfigColumnID = 2
	ZonesTableConfigColFamID = 2

	DescriptorTablePrimaryKeyIndexID  = 1
	DescriptorTableDescriptorColID    = 2
	DescriptorTableDescriptorColFamID = 2

	// Reserved IDs for other system tables. Note that some of these IDs refer
	// to "Ranges" instead of a Table - these IDs are needed to store custom
	// configuration for non-table ranges (e.g. Zone Configs).
	// NOTE: IDs must be <= MaxReservedDescID.
	LeaseTableID                         = 11
	EventLogTableID                      = 12
	RangeEventTableID                    = 13
	UITableID                            = 14
	JobsTableID                          = 15
	MetaRangesID                         = 16
	SystemRangesID                       = 17
	TimeseriesRangesID                   = 18
	WebSessionsTableID                   = 19
	TableStatisticsTableID               = 20
	LocationsTableID                     = 21
	LivenessRangesID                     = 22
	RoleMembersTableID                   = 23
	CommentsTableID                      = 24
	ReplicationConstraintStatsTableID    = 25
	ReplicationCriticalLocalitiesTableID = 26
	ReplicationStatsTableID              = 27
	ReportsMetaTableID                   = 28
	PublicSchemaID                       = 29
	// New NamespaceTableID for cluster version >= 20.1
	// Ensures that NamespaceTable does not get gossiped again
	NamespaceTableID = 30

	ProtectedTimestampsMetaTableID    = 31
	ProtectedTimestampsRecordsTableID = 32

	RoleOptionsTableID = 33

	StatementBundleChunksTableID        = 34
	StatementDiagnosticsRequestsTableID = 35
	StatementDiagnosticsTableID         = 36

	// CommentType is type for system.comments
	DatabaseCommentType = 0
	TableCommentType    = 1
	ColumnCommentType   = 2
	IndexCommentType    = 3
)

// PseudoTableIDs is the list of ids from above that are not real tables (i.e.
// there's no table descriptor). They're grouped here because the cluster
// bootstrap process needs to create splits for them; splits for the tables
// happen separately.
var PseudoTableIDs = []uint32{MetaRangesID, SystemRangesID, TimeseriesRangesID, LivenessRangesID, PublicSchemaID}
