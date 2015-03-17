// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package engine

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

// MakeKey makes a new key which is the concatenation of the
// given inputs, in order.
func MakeKey(keys ...proto.Key) proto.Key {
	return proto.MakeKey(keys...)
}

// MakeStoreKey creates a store-local key based on the metadata key
// suffix, and optional detail.
func MakeStoreKey(suffix, detail proto.Key) proto.Key {
	return MakeKey(KeyLocalStorePrefix, suffix, detail)
}

// StoreIdentKey returns a store-local key for the store metadata.
func StoreIdentKey() proto.Key {
	return MakeStoreKey(KeyLocalStoreIdentSuffix, proto.Key{})
}

// StoreStatKey returns the key for accessing the named stat
// for the specified store ID.
func StoreStatKey(storeID int32, stat proto.Key) proto.Key {
	return MakeStoreKey(KeyLocalStoreStatSuffix, stat)
}

// MakeRangeIDKey creates a range-local key based on the range's
// Raft ID, metadata key suffix, and optional detail (e.g. the
// encoded command ID for a response cache entry, etc.).
func MakeRangeIDKey(raftID int64, suffix, detail proto.Key) proto.Key {
	if len(suffix) != KeyLocalSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, KeyLocalSuffixLength))
	}
	return MakeKey(KeyLocalRangeIDPrefix, encoding.EncodeUvarint(nil, uint64(raftID)), suffix, detail)
}

// RaftLogKey returns a system-local key for a Raft log entry.
func RaftLogKey(raftID int64, logIndex uint64) proto.Key {
	// The log is stored "backwards" so we can easily find the highest index stored.
	return MakeRangeIDKey(raftID, KeyLocalRaftLogSuffix, encoding.EncodeUint64Decreasing(nil, logIndex))
}

// RaftLogPrefix returns the system-local prefix shared by all entries in a Raft log.
func RaftLogPrefix(raftID int64) proto.Key {
	return MakeRangeIDKey(raftID, KeyLocalRaftLogSuffix, proto.Key{})
}

// RaftHardStateKey returns a system-local key for a Raft HardState.
func RaftHardStateKey(raftID int64) proto.Key {
	return MakeRangeIDKey(raftID, KeyLocalRaftHardStateSuffix, proto.Key{})
}

// DecodeRaftStateKey extracts the Raft ID from a RaftStateKey.
func DecodeRaftStateKey(key proto.Key) int64 {
	if !bytes.HasPrefix(key, KeyLocalRangeIDPrefix) {
		panic(fmt.Sprintf("key %q does not have %q prefix", key, KeyLocalRangeIDPrefix))
	}
	// Cut the prefix and the Raft ID.
	b := key[len(KeyLocalRangeIDPrefix):]
	_, raftID := encoding.DecodeUvarint(b)
	return int64(raftID)
}

// RaftTruncatedStateKey returns a system-local key for a RaftTruncatedState.
func RaftTruncatedStateKey(raftID int64) proto.Key {
	return MakeRangeIDKey(raftID, KeyLocalRaftTruncatedStateSuffix, proto.Key{})
}

// RangeStatKey returns the key for accessing the named stat
// for the specified Raft ID.
func RangeStatKey(raftID int64, stat proto.Key) proto.Key {
	return MakeRangeIDKey(raftID, KeyLocalRangeStatSuffix, stat)
}

// ResponseCacheKey returns a range-local key by Raft ID for a
// response cache entry, with detail specified by encoding the
// supplied client command ID.
func ResponseCacheKey(raftID int64, cmdID *proto.ClientCmdID) proto.Key {
	detail := proto.Key{}
	if cmdID != nil {
		detail = encoding.EncodeUvarint(nil, uint64(cmdID.WallTime)) // wall time helps sort for locality
		detail = encoding.EncodeUint64(detail, uint64(cmdID.Random))
	}
	return MakeRangeIDKey(raftID, KeyLocalResponseCacheSuffix, detail)
}

// MakeRangeKey creates a range-local key based on the range
// start key, metadata key suffix, and optional detail (e.g. the
// transaction UUID for a txn record, etc.).
func MakeRangeKey(key, suffix, detail proto.Key) proto.Key {
	if len(suffix) != KeyLocalSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, KeyLocalSuffixLength))
	}
	return MakeKey(KeyLocalRangeKeyPrefix, encoding.EncodeBytes(nil, key), suffix, detail)
}

// DecodeRangeKey decodes the range key into range start key,
// suffix and optional detail (may be nil).
func DecodeRangeKey(key proto.Key) (startKey, suffix, detail proto.Key) {
	if !bytes.HasPrefix(key, KeyLocalRangeKeyPrefix) {
		panic(fmt.Sprintf("key %q does not have %q prefix", key, KeyLocalRangeKeyPrefix))
	}
	// Cut the prefix and the Raft ID.
	b := key[len(KeyLocalRangeKeyPrefix):]
	b, startKey = encoding.DecodeBytes(b)
	if len(b) < KeyLocalSuffixLength {
		panic(fmt.Sprintf("key %q does not have suffix of length %d", key, KeyLocalSuffixLength))
	}
	// Cut the response cache suffix.
	suffix = b[:KeyLocalSuffixLength]
	detail = b[KeyLocalSuffixLength:]
	return
}

// RangeGCMetadataKey returns a range-local key for range garbage
// collection metadata.
func RangeGCMetadataKey(raftID int64) proto.Key {
	return MakeRangeIDKey(raftID, KeyLocalRangeGCMetadataSuffix, proto.Key{})
}

// RangeLastVerificationTimestampKey returns a range-local key for
// the range's last verification timestamp.
func RangeLastVerificationTimestampKey(raftID int64) proto.Key {
	return MakeRangeIDKey(raftID, KeyLocalRangeLastVerificationTimestampSuffix, proto.Key{})
}

// RangeDescriptorKey returns a range-local key for the descriptor
// for the range with specified key.
func RangeDescriptorKey(key proto.Key) proto.Key {
	return MakeRangeKey(key, KeyLocalRangeDescriptorSuffix, proto.Key{})
}

// TransactionKey returns a transaction key based on the provided
// transaction key and ID. The base key is encoded in order to
// guarantee that all transaction records for a range sort together.
func TransactionKey(key proto.Key, id []byte) proto.Key {
	return MakeRangeKey(key, KeyLocalTransactionSuffix, proto.Key(id))
}

// KeyAddress returns the address for the key, used to lookup the
// range containing the key. In the normal case, this is simply the
// key's value. However, for local keys, such as transaction records,
// range-spanning binary tree node pointers, and message queues, the
// address is the trailing suffix of the key, with the local key
// prefix removed. In this way, local keys address to the same range
// as non-local keys, but are stored separately so that they don't
// collide with user-space or global system keys.
//
// However, not all local keys are addressable in the global map. Only
// range local keys incorporating a range key (start key or transaction
// key) are addressable (e.g. range metadata and txn records). Range
// local keys incorporating the Raft ID are not (e.g. response cache
// entries, and range stats).
func KeyAddress(k proto.Key) proto.Key {
	if !bytes.HasPrefix(k, KeyLocalPrefix) {
		return k
	}
	if bytes.HasPrefix(k, KeyLocalRangeKeyPrefix) {
		k = k[len(KeyLocalRangeKeyPrefix):]
		_, k = encoding.DecodeBytes(k)
		return k
	}
	log.Fatalf("local key %q malformed; should contain prefix %q", k, KeyLocalRangeKeyPrefix)
	return nil
}

// RangeMetaKey returns a range metadata (meta1, meta2) indexing key
// for the given key. For ordinary keys this returns a level 2
// metadata key - for level 2 keys, it returns a level 1 key. For
// level 1 keys and local keys, KeyMin is returned.
func RangeMetaKey(key proto.Key) proto.Key {
	if len(key) == 0 {
		return KeyMin
	}
	addr := KeyAddress(key)
	if !bytes.HasPrefix(addr, KeyMetaPrefix) {
		return MakeKey(KeyMeta2Prefix, addr)
	}
	if bytes.HasPrefix(addr, KeyMeta2Prefix) {
		return MakeKey(KeyMeta1Prefix, addr[len(KeyMeta2Prefix):])
	}

	return KeyMin
}

// RangeMetaLookupKey returns the metadata key at which this range
// descriptor should be stored as a value.
func RangeMetaLookupKey(r *proto.RangeDescriptor) proto.Key {
	return RangeMetaKey(r.EndKey)
}

// ValidateRangeMetaKey validates that the given key is a valid Range Metadata
// key. It must have an appropriate metadata range prefix, and the original key
// value must be less than KeyMax. As a special case, KeyMin is considered a
// valid Range Metadata Key.
func ValidateRangeMetaKey(key proto.Key) error {
	// KeyMin is a valid key.
	if len(key) == 0 {
		return nil
	}
	// Key must be at least as long as KeyMeta1Prefix.
	if len(key) < len(KeyMeta1Prefix) {
		return NewInvalidRangeMetaKeyError("too short", key)
	}

	prefix, body := key[:len(KeyMeta1Prefix)], key[len(KeyMeta1Prefix):]

	// The prefix must be equal to KeyMeta1Prefix or KeyMeta2Prefix
	if !bytes.HasPrefix(key, KeyMetaPrefix) {
		return NewInvalidRangeMetaKeyError(fmt.Sprintf("does not have %q prefix", KeyMetaPrefix), key)
	}
	if lvl := string(prefix[len(KeyMetaPrefix)]); lvl != "1" && lvl != "2" {
		return NewInvalidRangeMetaKeyError("meta level is not 1 or 2", key)
	}
	// Body of the key must sort before KeyMax
	if !body.Less(KeyMax) {
		return NewInvalidRangeMetaKeyError("body of range lookup is >= KeyMax", key)
	}

	return nil
}

// Constants for stat key construction.
var (
	// StatLiveBytes counts how many bytes are "live", including bytes
	// from both keys and values. Live rows include only non-deleted
	// keys and only the most recent value.
	StatLiveBytes = proto.Key("live-bytes")
	// StatKeyBytes counts how many bytes are used to store all keys,
	// including bytes from deleted keys. Key bytes are re-counted for
	// each versioned value.
	StatKeyBytes = proto.Key("key-bytes")
	// StatValBytes counts how many bytes are used to store all values,
	// including all historical versions and deleted tombstones.
	StatValBytes = proto.Key("val-bytes")
	// StatIntentBytes counts how many bytes are used to store values
	// which are unresolved intents. Includes bytes used for both intent
	// keys and values.
	StatIntentBytes = proto.Key("intent-bytes")
	// StatLiveCount counts how many keys are "live". This includes only
	// non-deleted keys.
	StatLiveCount = proto.Key("live-count")
	// StatKeyCount counts the total number of keys, including both live
	// and deleted keys.
	StatKeyCount = proto.Key("key-count")
	// StatValCount counts the total number of values, including all
	// historical versions and deleted tombstones.
	StatValCount = proto.Key("val-count")
	// StatIntentCount counts the number of unresolved intents.
	StatIntentCount = proto.Key("intent-count")
	// StatIntentAge counts the total age of unresolved intents.
	StatIntentAge = proto.Key("intent-age")
	// StatGCBytesAge counts the total age of gc'able bytes.
	StatGCBytesAge = proto.Key("gc-age")
	// StatLastUpdateNanos counts nanoseconds since the unix epoch for
	// the last update to the intent / GC'able bytes ages. This really
	// is tracking the wall time as at last update, but is a merged
	// stat, with successive counts of elapsed nanos being added at each
	// stat computation.
	StatLastUpdateNanos = proto.Key("update-nanos")
)

// Constants for system-reserved keys in the KV map.
var (
	// KeyMaxLength is the maximum key length in bytes. This value is
	// somewhat arbitrary. It is chosen high enough to allow most
	// conceivable use cases while also still being comfortably short of
	// a limit which would affect the performance of the system, both
	// from performance of key comparisons and from memory usage for
	// things like the timestamp cache, lookup cache, and command queue.
	KeyMaxLength = proto.KeyMaxLength

	// KeyMin is a minimum key value which sorts before all other keys.
	KeyMin = proto.KeyMin
	// KeyMax is a maximum key value which sorts after all other keys.
	KeyMax = proto.KeyMax

	// MVCCKeyMax is a maximum mvcc-encoded key value which sorts after
	// all other keys.
	MVCCKeyMax = MVCCEncodeKey(KeyMax)

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
	// KeyLocalStoreStatSuffix is the suffix for store statistics.
	KeyLocalStoreStatSuffix = proto.Key("sst-")

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
	// KeyLocalRaftLogSuffix is the suffix for the raft log.
	KeyLocalRaftLogSuffix = proto.Key("rftl")
	// KeyLocalRaftHardStateSuffix is the Suffix for the raft HardState.
	KeyLocalRaftHardStateSuffix = proto.Key("rfth")
	// KeyLocalRaftTruncatedStateSuffix is the suffix for the RaftTruncatedState.
	KeyLocalRaftTruncatedStateSuffix = proto.Key("rftt")
	// KeyLocalRangeGCMetadataSuffix is the suffix for a range's GC metadata.
	KeyLocalRangeGCMetadataSuffix = proto.Key("rgcm")
	// KeyLocalRangeLastVerificationTimestampSuffix is the suffix for a range's
	// last verification timestamp (for checking integrity of on-disk data).
	KeyLocalRangeLastVerificationTimestampSuffix = proto.Key("rlvt")
	// KeyLocalRangeStatSuffix is the suffix for range statistics.
	KeyLocalRangeStatSuffix = proto.Key("rst-")
	// KeyLocalResponseCacheSuffix is the suffix for keys storing
	// command responses used to guarantee idempotency (see
	// ResponseCache).
	// NOTE: if this value changes, it must be updated in C++
	// (storage/engine/db.cc).
	KeyLocalResponseCacheSuffix = proto.Key("res-")

	// KeyLocalRangeKeyPrefix is the prefix identifying per-range data
	// indexed by range key (either start key, or some key in the
	// range). The key is appended to this prefix, encoded using
	// EncodeBytes. The specific sort of per-range metadata is
	// identified by one of the suffixes listed below, along with
	// potentially additional encoded key info, such as the txn UUID in
	// the case of a transaction record.
	//
	// NOTE: KeyLocalRangeKeyPrefix must be kept in sync with the value
	// in storage/engine/db.cc.
	KeyLocalRangeKeyPrefix = MakeKey(KeyLocalPrefix, proto.Key("k"))
	// KeyLocalRangeDescriptorSuffix is the suffix for keys storing
	// range descriptors. The value is a struct of type RangeDescriptor.
	KeyLocalRangeDescriptorSuffix = proto.Key("rdsc")
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
	// KeyStoreIDGeneratorPrefix specifies key prefixes for sequence
	// generators, one per node, for store IDs.
	KeyStoreIDGeneratorPrefix = MakeKey(KeySystemPrefix, proto.Key("store-idgen-"))
)
