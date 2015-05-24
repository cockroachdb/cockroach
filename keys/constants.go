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
// Author: Peter Mattis (peter.mattis@gmail.com)

package keys

import "github.com/cockroachdb/cockroach/proto"

// Constants for system-reserved keys in the KV map.
var (
	// KeyLocalPrefix is the prefix for keys which hold data local to a
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
	// the KeySystemPrefix, because these local keys are not addressable
	// via the meta range addressing indexes.
	//
	// Some local data are not replicated, such as the store's 'ident'
	// record. Most local data are replicated, such as response cache
	// entries and transaction rows, but are not addressable as normal
	// MVCC values as part of transactions. Finally, some local data are
	// stored as MVCC values and are addressable as part of distributed
	// transactions, such as range metadata, range-spanning binary tree
	// node pointers, and message queues.
	KeyLocalPrefix = proto.Key("\x00\x00\x00")

	// KeyLocalSuffixLength specifies the length in bytes of all local
	// key suffixes.
	KeyLocalSuffixLength = 4

	// There are three types of local key data enumerated below:
	// store-local, range-local by ID, and range-local by key.

	// KeyLocalStorePrefix is the prefix identifying per-store data.
	KeyLocalStorePrefix = MakeKey(KeyLocalPrefix, proto.Key("s"))
	// KeyLocalStoreIdentSuffix stores an immutable identifier for this
	// store, created when the store is first bootstrapped.
	KeyLocalStoreIdentSuffix = proto.Key("iden")

	// KeyLocalRangeIDPrefix is the prefix identifying per-range data
	// indexed by Raft ID. The Raft ID is appended to this prefix,
	// encoded using EncodeUvarint. The specific sort of per-range
	// metadata is identified by one of the suffixes listed below, along
	// with potentially additional encoded key info, such as a command
	// ID in the case of response cache entry.
	//
	// NOTE: KeyLocalRangeIDPrefix must be kept in sync with the value
	// in storage/engine/db.cc.
	KeyLocalRangeIDPrefix = MakeKey(KeyLocalPrefix, proto.Key("i"))
	// KeyLocalResponseCacheSuffix is the suffix for keys storing
	// command responses used to guarantee idempotency (see ResponseCache).
	// NOTE: if this value changes, it must be updated in C++
	// (storage/engine/db.cc).
	KeyLocalResponseCacheSuffix = proto.Key("res-")
	// KeyLocalRaftLeaderLeaseSuffix is the suffix for the raft leader lease.
	KeyLocalRaftLeaderLeaseSuffix = proto.Key("rfll")
	// KeyLocalRaftHardStateSuffix is the Suffix for the raft HardState.
	KeyLocalRaftHardStateSuffix = proto.Key("rfth")
	// KeyLocalRaftAppliedIndexSuffix is the suffix for the raft applied index.
	KeyLocalRaftAppliedIndexSuffix = proto.Key("rfta")
	// KeyLocalRaftLogSuffix is the suffix for the raft log.
	KeyLocalRaftLogSuffix = proto.Key("rftl")
	// KeyLocalRaftTruncatedStateSuffix is the suffix for the RaftTruncatedState.
	KeyLocalRaftTruncatedStateSuffix = proto.Key("rftt")
	// KeyLocalRaftLastIndexSuffix is the suffix for raft's last index.
	KeyLocalRaftLastIndexSuffix = proto.Key("rfti")
	// KeyLocalRangeGCMetadataSuffix is the suffix for a range's GC metadata.
	KeyLocalRangeGCMetadataSuffix = proto.Key("rgcm")
	// KeyLocalRangeLastVerificationTimestampSuffix is the suffix for a range's
	// last verification timestamp (for checking integrity of on-disk data).
	KeyLocalRangeLastVerificationTimestampSuffix = proto.Key("rlvt")
	// KeyLocalRangeStatsSuffix is the suffix for range statistics.
	KeyLocalRangeStatsSuffix = proto.Key("stat")

	// KeyLocalRangeKeyPrefix is the prefix identifying per-range data
	// indexed by range key (either start key, or some key in the
	// range). The key is appended to this prefix, encoded using
	// EncodeBytes. The specific sort of per-range metadata is
	// identified by one of the suffixes listed below, along with
	// potentially additional encoded key info, such as the txn ID in
	// the case of a transaction record.
	//
	// NOTE: KeyLocalRangeKeyPrefix must be kept in sync with the value
	// in storage/engine/db.cc.
	KeyLocalRangeKeyPrefix = MakeKey(KeyLocalPrefix, proto.Key("k"))
	// KeyLocalRangeDescriptorSuffix is the suffix for keys storing
	// range descriptors. The value is a struct of type RangeDescriptor.
	KeyLocalRangeDescriptorSuffix = proto.Key("rdsc")
	// KeyLocalRangeTreeNodeSuffix is the suffix for keys storing
	// range tree nodes.  The value is a struct of type RangeTreeNode.
	KeyLocalRangeTreeNodeSuffix = proto.Key("rtn-")
	// KeyLocalTransactionSuffix specifies the key suffix for
	// transaction records. The additional detail is the transaction id.
	// NOTE: if this value changes, it must be updated in C++
	// (storage/engine/db.cc).
	KeyLocalTransactionSuffix = proto.Key("txn-")

	// KeyLocalMax is the end of the local key range.
	KeyLocalMax = KeyLocalPrefix.PrefixEnd()

	// KeySystemPrefix indicates the beginning of the key range for
	// global, system data which are replicated across the cluster.
	KeySystemPrefix = proto.Key("\x00")
	KeySystemMax    = proto.Key("\x01")

	// KeyMetaPrefix is the prefix for range metadata keys. Notice that
	// an extra null character in the prefix causes all range addressing
	// records to sort before any system tables which they might describe.
	KeyMetaPrefix = MakeKey(KeySystemPrefix, proto.Key("\x00meta"))
	// KeyMeta1Prefix is the first level of key addressing. The value is a
	// RangeDescriptor struct.
	KeyMeta1Prefix = MakeKey(KeyMetaPrefix, proto.Key("1"))
	// KeyMeta2Prefix is the second level of key addressing. The value is a
	// RangeDescriptor struct.
	KeyMeta2Prefix = MakeKey(KeyMetaPrefix, proto.Key("2"))

	// KeyMetaMax is the end of the range of addressing keys.
	KeyMetaMax = MakeKey(KeySystemPrefix, proto.Key("\x01"))

	// KeyConfigAccountingPrefix specifies the key prefix for accounting
	// configurations. The suffix is the affected key prefix.
	KeyConfigAccountingPrefix = MakeKey(KeySystemPrefix, proto.Key("acct"))
	// KeyConfigPermissionPrefix specifies the key prefix for accounting
	// configurations. The suffix is the affected key prefix.
	KeyConfigPermissionPrefix = MakeKey(KeySystemPrefix, proto.Key("perm"))
	// KeyConfigZonePrefix specifies the key prefix for zone
	// configurations. The suffix is the affected key prefix.
	KeyConfigZonePrefix = MakeKey(KeySystemPrefix, proto.Key("zone"))
	// KeyNodeIDGenerator is the global node ID generator sequence.
	KeyNodeIDGenerator = MakeKey(KeySystemPrefix, proto.Key("node-idgen"))
	// KeyRaftIDGenerator is the global Raft consensus group ID generator sequence.
	KeyRaftIDGenerator = MakeKey(KeySystemPrefix, proto.Key("raft-idgen"))
	// KeySchemaPrefix specifies key prefixes for schema definitions.
	KeySchemaPrefix = MakeKey(KeySystemPrefix, proto.Key("schema"))
	// KeyStoreIDGenerator is the global store ID generator sequence.
	KeyStoreIDGenerator = MakeKey(KeySystemPrefix, proto.Key("store-idgen"))
	// KeyRangeTreeRoot specifies the root range in the range tree.
	KeyRangeTreeRoot = MakeKey(KeySystemPrefix, proto.Key("range-tree-root"))

	// KeyStatusPrefix specifies the key prefix to store all status details.
	KeyStatusPrefix = MakeKey(KeySystemPrefix, proto.Key("status-"))
	// KeyStatusStorePrefix stores all status info for stores.
	KeyStatusStorePrefix = MakeKey(KeyStatusPrefix, proto.Key("store-"))
	// KeyStatusNodePrefix stores all status info for nodes.
	KeyStatusNodePrefix = MakeKey(KeyStatusPrefix, proto.Key("node-"))
)
