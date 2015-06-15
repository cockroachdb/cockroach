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

import "github.com/cockroachdb/cockroach/proto"

// Constants for system-reserved keys in the KV map.
var (
	// LocalPrefix is the prefix for keys which hold data local to a
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
	LocalPrefix = proto.Key("\x00\x00\x00")

	// LocalSuffixLength specifies the length in bytes of all local
	// key suffixes.
	LocalSuffixLength = 4

	// There are three types of local key data enumerated below:
	// store-local, range-local by ID, and range-local by key.

	// LocalStorePrefix is the prefix identifying per-store data.
	LocalStorePrefix = MakeKey(LocalPrefix, proto.Key("s"))
	// LocalStoreIdentSuffix stores an immutable identifier for this
	// store, created when the store is first bootstrapped.
	LocalStoreIdentSuffix = proto.Key("iden")

	// LocalRangeIDPrefix is the prefix identifying per-range data
	// indexed by Raft ID. The Raft ID is appended to this prefix,
	// encoded using EncodeUvarint. The specific sort of per-range
	// metadata is identified by one of the suffixes listed below, along
	// with potentially additional encoded key info, such as a command
	// ID in the case of response cache entry.
	//
	// NOTE: LocalRangeIDPrefix must be kept in sync with the value
	// in storage/engine/db.cc.
	LocalRangeIDPrefix = MakeKey(LocalPrefix, proto.Key("i"))
	// LocalResponseCacheSuffix is the suffix for keys storing
	// command responses used to guarantee idempotency (see ResponseCache).
	// NOTE: if this value changes, it must be updated in C++
	// (storage/engine/db.cc).
	LocalResponseCacheSuffix = proto.Key("res-")
	// LocalRaftLeaderLeaseSuffix is the suffix for the raft leader lease.
	LocalRaftLeaderLeaseSuffix = proto.Key("rfll")
	// LocalRaftHardStateSuffix is the Suffix for the raft HardState.
	LocalRaftHardStateSuffix = proto.Key("rfth")
	// LocalRaftAppliedIndexSuffix is the suffix for the raft applied index.
	LocalRaftAppliedIndexSuffix = proto.Key("rfta")
	// LocalRaftLogSuffix is the suffix for the raft log.
	LocalRaftLogSuffix = proto.Key("rftl")
	// LocalRaftTruncatedStateSuffix is the suffix for the RaftTruncatedState.
	LocalRaftTruncatedStateSuffix = proto.Key("rftt")
	// LocalRaftLastIndexSuffix is the suffix for raft's last index.
	LocalRaftLastIndexSuffix = proto.Key("rfti")
	// LocalRangeGCMetadataSuffix is the suffix for a range's GC metadata.
	LocalRangeGCMetadataSuffix = proto.Key("rgcm")
	// LocalRangeLastVerificationTimestampSuffix is the suffix for a range's
	// last verification timestamp (for checking integrity of on-disk data).
	LocalRangeLastVerificationTimestampSuffix = proto.Key("rlvt")
	// LocalRangeStatsSuffix is the suffix for range statistics.
	LocalRangeStatsSuffix = proto.Key("stat")

	// LocalRangePrefix is the prefix identifying per-range data indexed
	// by range key (either start key, or some key in the range). The
	// key is appended to this prefix, encoded using EncodeBytes. The
	// specific sort of per-range metadata is identified by one of the
	// suffixes listed below, along with potentially additional encoded
	// key info, such as the txn ID in the case of a transaction record.
	//
	// NOTE: LocalRangePrefix must be kept in sync with the value in
	// storage/engine/db.cc.
	LocalRangePrefix = MakeKey(LocalPrefix, proto.Key("k"))
	// LocalRangeDescriptorSuffix is the suffix for keys storing
	// range descriptors. The value is a struct of type RangeDescriptor.
	LocalRangeDescriptorSuffix = proto.Key("rdsc")
	// LocalRangeTreeNodeSuffix is the suffix for keys storing
	// range tree nodes.  The value is a struct of type RangeTreeNode.
	LocalRangeTreeNodeSuffix = proto.Key("rtn-")
	// LocalTransactionSuffix specifies the key suffix for
	// transaction records. The additional detail is the transaction id.
	// NOTE: if this value changes, it must be updated in C++
	// (storage/engine/db.cc).
	LocalTransactionSuffix = proto.Key("txn-")

	// LocalMax is the end of the local key range.
	LocalMax = LocalPrefix.PrefixEnd()

	// SystemPrefix indicates the beginning of the key range for
	// global, system data which are replicated across the cluster.
	SystemPrefix = proto.Key("\x00")
	SystemMax    = proto.Key("\x01")

	// MetaPrefix is the prefix for range metadata keys. Notice that
	// an extra null character in the prefix causes all range addressing
	// records to sort before any system tables which they might describe.
	MetaPrefix = MakeKey(SystemPrefix, proto.Key("\x00meta"))
	// Meta1Prefix is the first level of key addressing. The value is a
	// RangeDescriptor struct.
	Meta1Prefix = MakeKey(MetaPrefix, proto.Key("1"))
	// Meta2Prefix is the second level of key addressing. The value is a
	// RangeDescriptor struct.
	Meta2Prefix = MakeKey(MetaPrefix, proto.Key("2"))
	// Meta1KeyMax is the end of the range of the first level of key addressing.
	// The value is a RangeDescriptor struct.
	Meta1KeyMax = MakeKey(Meta1Prefix, proto.KeyMax)
	// Meta2KeyMax is the end of the range of the second level of key addressing.
	// The value is a RangeDescriptor struct.
	Meta2KeyMax = MakeKey(Meta2Prefix, proto.KeyMax)

	// MetaMax is the end of the range of addressing keys.
	MetaMax = MakeKey(SystemPrefix, proto.Key("\x01"))

	// ConfigAccountingPrefix specifies the key prefix for accounting
	// configurations. The suffix is the affected key prefix.
	ConfigAccountingPrefix = MakeKey(SystemPrefix, proto.Key("acct"))
	// ConfigPermissionPrefix specifies the key prefix for accounting
	// configurations. The suffix is the affected key prefix.
	ConfigPermissionPrefix = MakeKey(SystemPrefix, proto.Key("perm"))
	// ConfigZonePrefix specifies the key prefix for zone
	// configurations. The suffix is the affected key prefix.
	ConfigZonePrefix = MakeKey(SystemPrefix, proto.Key("zone"))
	// DescIDGenerator is the global descriptor ID generator sequence used for
	// table and namespace IDs.
	DescIDGenerator = MakeKey(SystemPrefix, proto.Key("desc-idgen"))
	// DescMetadataPrefix is the key prefix for all descriptor metadata.
	DescMetadataPrefix = MakeKey(SystemPrefix, proto.Key("desc-"))
	// NodeIDGenerator is the global node ID generator sequence.
	NodeIDGenerator = MakeKey(SystemPrefix, proto.Key("node-idgen"))
	// RaftIDGenerator is the global Raft consensus group ID generator sequence.
	RaftIDGenerator = MakeKey(SystemPrefix, proto.Key("raft-idgen"))
	// SchemaPrefix specifies key prefixes for schema definitions.
	SchemaPrefix = MakeKey(SystemPrefix, proto.Key("schema"))
	// NameMetadataPrefix is the key prefix for all name metadata.
	NameMetadataPrefix = MakeKey(SystemPrefix, proto.Key("name-"))
	// StoreIDGenerator is the global store ID generator sequence.
	StoreIDGenerator = MakeKey(SystemPrefix, proto.Key("store-idgen"))
	// RangeTreeRoot specifies the root range in the range tree.
	RangeTreeRoot = MakeKey(SystemPrefix, proto.Key("range-tree-root"))

	// StatusPrefix specifies the key prefix to store all status details.
	StatusPrefix = MakeKey(SystemPrefix, proto.Key("status-"))
	// StatusStorePrefix stores all status info for stores.
	StatusStorePrefix = MakeKey(StatusPrefix, proto.Key("store-"))
	// StatusNodePrefix stores all status info for nodes.
	StatusNodePrefix = MakeKey(StatusPrefix, proto.Key("node-"))
)
