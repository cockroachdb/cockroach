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
// Author: Peter Mattis (peter@cockroachlabs.com)

package keys

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

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
// Note: preserve group-wise ordering when adding new constants.
var (
	// localPrefix is the prefix for keys which hold data local to a
	// RocksDB instance, such as store and range-specific metadata which
	// must not pollute the user key space, but must be collocated with
	// the store and/or ranges which they refer to. Storing this
	// information in the normal system keyspace would place the data on
	// an arbitrary set of stores, with no guarantee of collocation.
	// Local data includes store metadata, range metadata, abort
	// cache values, transaction records, range-spanning binary tree
	// node pointers, and message queues.
	//
	// The local key prefix has been deliberately chosen to sort before
	// the SystemPrefix, because these local keys are not addressable
	// via the meta range addressing indexes.
	//
	// Some local data are not replicated, such as the store's 'ident'
	// record. Most local data are replicated, such as abort cache
	// entries and transaction rows, but are not addressable as normal
	// MVCC values as part of transactions. Finally, some local data are
	// stored as MVCC values and are addressable as part of distributed
	// transactions, such as range metadata, range-spanning binary tree
	// node pointers, and message queues.
	localPrefix = roachpb.Key{localPrefixByte}
	// LocalMax is the end of the local key range. It is itself a global
	// key.
	LocalMax = roachpb.Key{localMaxByte}

	// localSuffixLength specifies the length in bytes of all local
	// key suffixes.
	localSuffixLength = 4

	// There are three types of local key data enumerated below:
	// store-local, range-local by ID, and range-local by key.

	// localStorePrefix is the prefix identifying per-store data.
	localStorePrefix = makeKey(localPrefix, roachpb.Key("s"))
	// localStoreIdentSuffix stores an immutable identifier for this
	// store, created when the store is first bootstrapped.
	localStoreIdentSuffix = []byte("iden")
	// localStoreGossipSuffix stores gossip bootstrap metadata for this
	// store, updated any time new gossip hosts are encountered.
	localStoreGossipSuffix = []byte("goss")
	// localStoreLastUpSuffix stores the last timestamp that a store's node
	// acknowledged that it was still running. This value will be regularly
	// refreshed on all stores for a running node; the intention of this value
	// is to allow a restarting node to discover approximately how long it has
	// been down without needing to retrieve liveness records from the cluster.
	localStoreLastUpSuffix = []byte("uptm")

	// LocalRangeIDPrefix is the prefix identifying per-range data
	// indexed by Range ID. The Range ID is appended to this prefix,
	// encoded using EncodeUvarint. The specific sort of per-range
	// metadata is identified by one of the suffixes listed below, along
	// with potentially additional encoded key info, for instance in the
	// case of abort cache entry.
	//
	// NOTE: LocalRangeIDPrefix must be kept in sync with the value
	// in storage/engine/rocksdb/db.cc.
	LocalRangeIDPrefix = roachpb.RKey(makeKey(localPrefix, roachpb.Key("i")))

	// localRangeIDReplicatedInfix is the post-Range ID specifier for all Raft
	// replicated per-range data. By appending this after the Range ID, these
	// keys will be sorted directly before the local unreplicated keys for the
	// same Range ID, so they can be manipulated either together or individually
	// in a single scan.
	localRangeIDReplicatedInfix = []byte("r")
	// LocalAbortCacheSuffix is the suffix for abort cache entries. The
	// abort cache protects a transaction from re-reading its own intents
	// after it's been aborted.
	LocalAbortCacheSuffix = []byte("abc-")
	// LocalRangeFrozenStatusSuffix is the suffix for a frozen status.
	// No longer used; exists only to reserve the key so we don't use it.
	LocalRangeFrozenStatusSuffix = []byte("fzn-")
	// LocalRangeLastGCSuffix is the suffix for the last GC.
	LocalRangeLastGCSuffix = []byte("lgc-")
	// LocalRaftAppliedIndexSuffix is the suffix for the raft applied index.
	LocalRaftAppliedIndexSuffix = []byte("rfta")
	// LocalRaftTombstoneSuffix is the suffix for the raft tombstone.
	LocalRaftTombstoneSuffix = []byte("rftb")
	// LocalRaftTruncatedStateSuffix is the suffix for the RaftTruncatedState.
	LocalRaftTruncatedStateSuffix = []byte("rftt")
	// LocalRangeLeaseSuffix is the suffix for a range lease.
	LocalRangeLeaseSuffix = []byte("rll-")
	// LocalLeaseAppliedIndexSuffix is the suffix for the applied lease index.
	LocalLeaseAppliedIndexSuffix = []byte("rlla")
	// LocalRangeStatsSuffix is the suffix for range statistics.
	LocalRangeStatsSuffix = []byte("stat")
	// LocalTxnSpanGCThresholdSuffix is the suffix for the last txn span GC's
	// threshold.
	LocalTxnSpanGCThresholdSuffix = []byte("tst-")

	// localRangeIDUnreplicatedInfix is the post-Range ID specifier for all
	// per-range data that is not fully Raft replicated. By appending this
	// after the Range ID, these keys will be sorted directly after the local
	// replicated keys for the same Range ID, so they can be manipulated either
	// together or individually in a single scan.
	localRangeIDUnreplicatedInfix = []byte("u")
	// LocalRaftHardStateSuffix is the Suffix for the raft HardState.
	LocalRaftHardStateSuffix = []byte("rfth")
	// LocalRaftLastIndexSuffix is the suffix for raft's last index.
	LocalRaftLastIndexSuffix = []byte("rfti")
	// LocalRaftLogSuffix is the suffix for the raft log.
	LocalRaftLogSuffix = []byte("rftl")
	// LocalRangeLastReplicaGCTimestampSuffix is the suffix for a range's
	// last replica GC timestamp (for GC of old replicas).
	LocalRangeLastReplicaGCTimestampSuffix = []byte("rlrt")
	// LocalRangeLastVerificationTimestampSuffixDeprecated is the suffix for a range's
	// last verification timestamp (for checking integrity of on-disk data).
	// Note: DEPRECATED.
	LocalRangeLastVerificationTimestampSuffixDeprecated = []byte("rlvt")
	// LocalRangeReplicaDestroyedErrorSuffix is the suffix for a range's replica
	// destroyed error (for marking replicas as dead).
	LocalRangeReplicaDestroyedErrorSuffix = []byte("rrde")

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
	// LocalRangeDescriptorSuffix is the suffix for keys storing
	// range descriptors. The value is a struct of type RangeDescriptor.
	LocalRangeDescriptorSuffix = roachpb.RKey("rdsc")
	// LocalTransactionSuffix specifies the key suffix for
	// transaction records. The additional detail is the transaction id.
	// NOTE: if this value changes, it must be updated in C++
	// (storage/engine/rocksdb/db.cc).
	LocalTransactionSuffix = roachpb.RKey("txn-")
	// LocalQueueLastProcessedSuffix is the suffix for replica queue state keys.
	LocalQueueLastProcessedSuffix = roachpb.RKey("qlpt")

	// Meta1Prefix is the first level of key addressing. It is selected such that
	// all range addressing records sort before any system tables which they
	// might describe. The value is a RangeDescriptor struct.
	Meta1Prefix = roachpb.Key{meta1PrefixByte}
	// Meta2Prefix is the second level of key addressing. The value is a
	// RangeDescriptor struct.
	Meta2Prefix = roachpb.Key{meta2PrefixByte}
	// Meta1KeyMax is the end of the range of the first level of key addressing.
	// The value is a RangeDescriptor struct.
	Meta1KeyMax = roachpb.Key(makeKey(Meta1Prefix, roachpb.RKeyMax))
	// Meta2KeyMax is the end of the range of the second level of key addressing.
	// The value is a RangeDescriptor struct.
	Meta2KeyMax = roachpb.Key(makeKey(Meta2Prefix, roachpb.RKeyMax))

	// MetaMin is the start of the range of addressing keys.
	MetaMin = Meta1Prefix
	// MetaMax is the end of the range of addressing keys.
	MetaMax = roachpb.Key{metaMaxByte}

	// SystemPrefix indicates the beginning of the key range for
	// global, system data which are replicated across the cluster.
	SystemPrefix = roachpb.Key{systemPrefixByte}
	SystemMax    = roachpb.Key{systemMaxByte}

	// MigrationPrefix specifies the key prefix to store all migration details.
	MigrationPrefix = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("system-version/")))

	// MigrationKeyMax is the maximum value for any system migration key.
	MigrationKeyMax = MigrationPrefix.PrefixEnd()

	// MigrationLease is the key that nodes must take a lease on in order to run
	// system migrations on the cluster.
	MigrationLease = roachpb.Key(makeKey(MigrationPrefix, roachpb.RKey("lease")))

	// NodeLivenessPrefix specifies the key prefix for the node liveness
	// table.  Note that this should sort before the rest of the system
	// keyspace in order to limit the number of ranges which must use
	// expiration-based range leases instead of the more efficient
	// node-liveness epoch-based range leases (see
	// https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/range_leases.md)
	NodeLivenessPrefix = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("\x00liveness-")))

	// NodeLivenessKeyMax is the maximum value for any node liveness key.
	NodeLivenessKeyMax = NodeLivenessPrefix.PrefixEnd()

	// DescIDGenerator is the global descriptor ID generator sequence used for
	// table and namespace IDs.
	DescIDGenerator = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("desc-idgen")))
	// NodeIDGenerator is the global node ID generator sequence.
	NodeIDGenerator = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("node-idgen")))
	// RangeIDGenerator is the global range ID generator sequence.
	RangeIDGenerator = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("range-idgen")))
	// StoreIDGenerator is the global store ID generator sequence.
	StoreIDGenerator = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("store-idgen")))

	// StatusPrefix specifies the key prefix to store all status details.
	StatusPrefix = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("status-")))
	// StatusNodePrefix stores all status info for nodes.
	StatusNodePrefix = roachpb.Key(makeKey(StatusPrefix, roachpb.RKey("node-")))

	// TimeseriesPrefix is the key prefix for all timeseries data.
	TimeseriesPrefix = roachpb.Key(makeKey(SystemPrefix, roachpb.RKey("tsd")))

	// TableDataMin is the start of the range of table data keys.
	TableDataMin = roachpb.Key(encoding.EncodeVarintAscending(nil, 0))
	// TableDataMin is the end of the range of table data keys.
	TableDataMax = roachpb.Key(encoding.EncodeVarintAscending(nil, math.MaxInt64))

	// SystemConfigSplitKey is the key to split at immediately prior to the
	// system config span. NB: Split keys need to be valid column keys.
	SystemConfigSplitKey = MakeRowSentinelKey(TableDataMin)
	// SystemConfigTableDataMax is the end key of system config span.
	SystemConfigTableDataMax = roachpb.Key(MakeTablePrefix(MaxSystemConfigDescID + 1))

	// UserTableDataMin is the start key of user structured data.
	UserTableDataMin = roachpb.Key(MakeTablePrefix(MaxReservedDescID + 1))

	// MaxKey is the infinity marker which is larger than any other key.
	MaxKey = roachpb.KeyMax
	// MinKey is a minimum key value which sorts before all other keys.
	MinKey = roachpb.KeyMin
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

	// VirtualDescriptorID is the ID used by all virtual descriptors.
	VirtualDescriptorID = math.MaxUint32

	// RootNamespaceID is the ID of the root namespace.
	RootNamespaceID = 0

	// SystemDatabaseID and following are the database/table IDs for objects
	// in the system span.
	// NOTE: IDs must be <= MaxSystemConfigDescID.
	SystemDatabaseID  = 1
	NamespaceTableID  = 2
	DescriptorTableID = 3
	UsersTableID      = 4
	ZonesTableID      = 5
	SettingsTableID   = 6

	// Reserved IDs for other system tables. If you're adding a new system table,
	// it probably belongs here.
	// NOTE: IDs must be <= MaxReservedDescID.
	LeaseTableID      = 11
	EventLogTableID   = 12
	RangeEventTableID = 13
	UITableID         = 14
	JobsTableID       = 15

	// Reserved IDs used to refer to certain parts of the system ranges that
	// come before the system config span and user table ranges.
	// NOTE: IDs must be <= MaxReservedDescID.
	MetaRangesID       = 16
	SystemRangesID     = 17
	TimeseriesRangesID = 18
)
