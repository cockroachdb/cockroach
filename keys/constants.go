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
// Author: Peter Mattis (peter@cockroachlabs.com)

package keys

import "github.com/cockroachdb/cockroach/roachpb"

// Constants for system-reserved keys in the KV map.
var (
	// localPrefix is the prefix for keys which hold data local to a
	// RocksDB instance, such as store and range-specific metadata which
	// must not pollute the user key space, but must be collocate with
	// the store and/or ranges which they refer to. Storing this
	// information in the normal system keyspace would place the data on
	// an arbitrary set of stores, with no guarantee of collocation.
	// Local data includes store metadata, range metadata, response
	// cache values, transaction records, range-spanning binary tree
	// node pointers, and message queues.
	//
	// The local key prefix has been deliberately chosen to sort before
	// the SystemPrefix, because these local keys are not addressable
	// via the meta range addressing indexes.
	//
	// Some local data are not replicated, such as the store's 'ident'
	// record. Most local data are replicated, such as response cache
	// entries and transaction rows, but are not addressable as normal
	// MVCC values as part of transactions. Finally, some local data are
	// stored as MVCC values and are addressable as part of distributed
	// transactions, such as range metadata, range-spanning binary tree
	// node pointers, and message queues.
	localPrefix = []byte("\x00\x00\x00")

	// localSuffixLength specifies the length in bytes of all local
	// key suffixes.
	localSuffixLength = 4

	// There are three types of local key data enumerated below:
	// store-local, range-local by ID, and range-local by key.

	// localStorePrefix is the prefix identifying per-store data.
	localStorePrefix = MakeKey(localPrefix, roachpb.Key("s"))
	// localStoreIdentSuffix stores an immutable identifier for this
	// store, created when the store is first bootstrapped.
	localStoreIdentSuffix = []byte("iden")

	// LocalRangeIDPrefix is the prefix identifying per-range data
	// indexed by Range ID. The Range ID is appended to this prefix,
	// encoded using EncodeUvarint. The specific sort of per-range
	// metadata is identified by one of the suffixes listed below, along
	// with potentially additional encoded key info, such as a command
	// ID in the case of response cache entry.
	//
	// NOTE: LocalRangeIDPrefix must be kept in sync with the value
	// in storage/engine/db.cc.
	LocalRangeIDPrefix = roachpb.RKey(MakeKey(localPrefix, roachpb.Key("i")))
	// LocalResponseCacheSuffix is the suffix for keys storing
	// command responses used to guarantee idempotency (see ResponseCache).
	// NOTE: if this value changes, it must be updated in C++
	// (storage/engine/db.cc).
	LocalResponseCacheSuffix = []byte("res-")
	// localRaftLeaderLeaseSuffix is the suffix for the raft leader lease.
	localRaftLeaderLeaseSuffix = []byte("rfll")
	// localRaftHardStateSuffix is the Suffix for the raft HardState.
	localRaftHardStateSuffix = []byte("rfth")
	// localRaftAppliedIndexSuffix is the suffix for the raft applied index.
	localRaftAppliedIndexSuffix = []byte("rfta")
	// localRaftLogSuffix is the suffix for the raft log.
	localRaftLogSuffix = []byte("rftl")
	// localRaftTruncatedStateSuffix is the suffix for the RaftTruncatedState.
	localRaftTruncatedStateSuffix = []byte("rftt")
	// localRaftLastIndexSuffix is the suffix for raft's last index.
	localRaftLastIndexSuffix = []byte("rfti")
	// localRangeGCMetadataSuffix is the suffix for a range's GC metadata.
	localRangeGCMetadataSuffix = []byte("rgcm")
	// localRangeLastVerificationTimestampSuffix is the suffix for a range's
	// last verification timestamp (for checking integrity of on-disk data).
	localRangeLastVerificationTimestampSuffix = []byte("rlvt")
	// localRangeStatsSuffix is the suffix for range statistics.
	localRangeStatsSuffix = []byte("stat")

	// LocalRangePrefix is the prefix identifying per-range data indexed
	// by range key (either start key, or some key in the range). The
	// key is appended to this prefix, encoded using EncodeBytes. The
	// specific sort of per-range metadata is identified by one of the
	// suffixes listed below, along with potentially additional encoded
	// key info, such as the txn ID in the case of a transaction record.
	//
	// NOTE: LocalRangePrefix must be kept in sync with the value in
	// storage/engine/db.cc.
	LocalRangePrefix = roachpb.Key(MakeKey(localPrefix, roachpb.RKey("k")))
	LocalRangeMax    = LocalRangePrefix.PrefixEnd()
	// LocalRangeDescriptorSuffix is the suffix for keys storing
	// range descriptors. The value is a struct of type RangeDescriptor.
	LocalRangeDescriptorSuffix = roachpb.RKey("rdsc")
	// localRangeTreeNodeSuffix is the suffix for keys storing
	// range tree nodes.  The value is a struct of type RangeTreeNode.
	localRangeTreeNodeSuffix = roachpb.RKey("rtn-")
	// localTransactionSuffix specifies the key suffix for
	// transaction records. The additional detail is the transaction id.
	// NOTE: if this value changes, it must be updated in C++
	// (storage/engine/db.cc).
	localTransactionSuffix = roachpb.RKey("txn-")

	// LocalMax is the end of the local key range.
	LocalMax = roachpb.Key(localPrefix).PrefixEnd()

	// SystemPrefix indicates the beginning of the key range for
	// global, system data which are replicated across the cluster.
	SystemPrefix = roachpb.Key("\x00")
	SystemMax    = roachpb.Key("\x01")

	// MetaPrefix is the prefix for range metadata keys. Notice that
	// an extra null character in the prefix causes all range addressing
	// records to sort before any system tables which they might describe.
	MetaPrefix = MakeKey(SystemPrefix, roachpb.RKey("\x00meta"))
	// Meta1Prefix is the first level of key addressing. The value is a
	// RangeDescriptor struct.
	Meta1Prefix = roachpb.Key(MakeKey(MetaPrefix, roachpb.RKey("1")))
	// Meta2Prefix is the second level of key addressing. The value is a
	// RangeDescriptor struct.
	Meta2Prefix = roachpb.Key(MakeKey(MetaPrefix, roachpb.RKey("2")))
	// Meta1KeyMax is the end of the range of the first level of key addressing.
	// The value is a RangeDescriptor struct.
	Meta1KeyMax = roachpb.Key(MakeKey(Meta1Prefix, roachpb.RKeyMax))
	// Meta2KeyMax is the end of the range of the second level of key addressing.
	// The value is a RangeDescriptor struct.
	Meta2KeyMax = roachpb.Key(MakeKey(Meta2Prefix, roachpb.RKeyMax))

	// MetaMax is the end of the range of addressing keys.
	MetaMax = roachpb.Key(MakeKey(SystemPrefix, roachpb.RKey("\x01")))

	// DescIDGenerator is the global descriptor ID generator sequence used for
	// table and namespace IDs.
	DescIDGenerator = roachpb.Key(MakeKey(SystemPrefix, roachpb.RKey("desc-idgen")))
	// NodeIDGenerator is the global node ID generator sequence.
	NodeIDGenerator = roachpb.Key(MakeKey(SystemPrefix, roachpb.RKey("node-idgen")))
	// RangeIDGenerator is the global range ID generator sequence.
	RangeIDGenerator = roachpb.Key(MakeKey(SystemPrefix, roachpb.RKey("range-idgen")))
	// StoreIDGenerator is the global store ID generator sequence.
	StoreIDGenerator = roachpb.Key(MakeKey(SystemPrefix, roachpb.RKey("store-idgen")))
	// RangeTreeRoot specifies the root range in the range tree.
	RangeTreeRoot = roachpb.Key(MakeKey(SystemPrefix, roachpb.RKey("range-tree-root")))

	// StatusPrefix specifies the key prefix to store all status details.
	StatusPrefix = roachpb.Key(MakeKey(SystemPrefix, roachpb.RKey("status-")))
	// StatusStorePrefix stores all status info for stores.
	StatusStorePrefix = roachpb.Key(MakeKey(StatusPrefix, roachpb.RKey("store-")))
	// StatusNodePrefix stores all status info for nodes.
	StatusNodePrefix = roachpb.Key(MakeKey(StatusPrefix, roachpb.RKey("node-")))

	// TableDataPrefix prefixes all table data. It is specifically chosen to
	// occur after the range of common user data prefixes so that tests which use
	// those prefixes will not see table data.
	TableDataPrefix = roachpb.Key("\xff")

	// UserTableDataMin is the start key of user structured data.
	UserTableDataMin = roachpb.Key(MakeTablePrefix(MaxReservedDescID + 1))
)

// Various IDs used by the structured data layer.
// NOTE: these must not change during the lifetime of a cluster.
const (
	// MaxReservedDescID is the maximum value of the system IDs
	// under 'TableDataPrefix'.
	// It is here only so that we may define keys/ranges using it.
	MaxReservedDescID = 999

	// RootNamespaceID is the ID of the root namespace.
	RootNamespaceID = 0

	// SystemDatabaseID and following are the database/table IDs for objects
	// in the system span.
	// NOTE: IDs should remain <= MaxReservedDescID.
	SystemDatabaseID  = 1
	NamespaceTableID  = 2
	DescriptorTableID = 3
	UsersTableID      = 4
	ZonesTableID      = 5
)
