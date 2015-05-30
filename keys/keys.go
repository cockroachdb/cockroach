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
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package keys

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
	return MakeKey(LocalStorePrefix, suffix, detail)
}

// StoreIdentKey returns a store-local key for the store metadata.
func StoreIdentKey() proto.Key {
	return MakeStoreKey(LocalStoreIdentSuffix, proto.Key{})
}

// StoreStatusKey returns the key for accessing the store status for the
// specified store ID.
func StoreStatusKey(storeID int32) proto.Key {
	return MakeKey(StatusStorePrefix, encoding.EncodeUvarint(nil, uint64(storeID)))
}

// NodeStatusKey returns the key for accessing the node status for the
// specified node ID.
func NodeStatusKey(nodeID int32) proto.Key {
	return MakeKey(StatusNodePrefix, encoding.EncodeUvarint(nil, uint64(nodeID)))
}

// MakeRangeIDKey creates a range-local key based on the range's
// Raft ID, metadata key suffix, and optional detail (e.g. the
// encoded command ID for a response cache entry, etc.).
func MakeRangeIDKey(raftID int64, suffix, detail proto.Key) proto.Key {
	if len(suffix) != LocalSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, LocalSuffixLength))
	}
	return MakeKey(LocalRangeIDPrefix,
		encoding.EncodeUvarint(nil, uint64(raftID)), suffix, detail)
}

// RaftLogKey returns a system-local key for a Raft log entry.
func RaftLogKey(raftID int64, logIndex uint64) proto.Key {
	return MakeRangeIDKey(raftID, LocalRaftLogSuffix,
		encoding.EncodeUint64(nil, logIndex))
}

// RaftLogPrefix returns the system-local prefix shared by all entries in a Raft log.
func RaftLogPrefix(raftID int64) proto.Key {
	return MakeRangeIDKey(raftID, LocalRaftLogSuffix, proto.Key{})
}

// RaftHardStateKey returns a system-local key for a Raft HardState.
func RaftHardStateKey(raftID int64) proto.Key {
	return MakeRangeIDKey(raftID, LocalRaftHardStateSuffix, proto.Key{})
}

// DecodeRaftStateKey extracts the Raft ID from a RaftStateKey.
func DecodeRaftStateKey(key proto.Key) int64 {
	if !bytes.HasPrefix(key, LocalRangeIDPrefix) {
		panic(fmt.Sprintf("key %q does not have %q prefix", key, LocalRangeIDPrefix))
	}
	// Cut the prefix and the Raft ID.
	b := key[len(LocalRangeIDPrefix):]
	_, raftID := encoding.DecodeUvarint(b)
	return int64(raftID)
}

// RaftTruncatedStateKey returns a system-local key for a RaftTruncatedState.
func RaftTruncatedStateKey(raftID int64) proto.Key {
	return MakeRangeIDKey(raftID, LocalRaftTruncatedStateSuffix, proto.Key{})
}

// RaftAppliedIndexKey returns a system-local key for a raft applied index.
func RaftAppliedIndexKey(raftID int64) proto.Key {
	return MakeRangeIDKey(raftID, LocalRaftAppliedIndexSuffix, proto.Key{})
}

// RaftLeaderLeaseKey returns a system-local key for a raft leader lease.
func RaftLeaderLeaseKey(raftID int64) proto.Key {
	return MakeRangeIDKey(raftID, LocalRaftLeaderLeaseSuffix, proto.Key{})
}

// RaftLastIndexKey returns a system-local key for a raft last index.
func RaftLastIndexKey(raftID int64) proto.Key {
	return MakeRangeIDKey(raftID, LocalRaftLastIndexSuffix, proto.Key{})
}

// RangeStatsKey returns the key for accessing the MVCCStats struct
// for the specified Raft ID.
func RangeStatsKey(raftID int64) proto.Key {
	return MakeRangeIDKey(raftID, LocalRangeStatsSuffix, nil)
}

// ResponseCacheKey returns a range-local key by Raft ID for a
// response cache entry, with detail specified by encoding the
// supplied client command ID.
func ResponseCacheKey(raftID int64, cmdID *proto.ClientCmdID) proto.Key {
	detail := proto.Key{}
	if cmdID != nil {
		// Wall time helps sort for locality.
		detail = encoding.EncodeUvarint(nil, uint64(cmdID.WallTime))
		detail = encoding.EncodeUint64(detail, uint64(cmdID.Random))
	}
	return MakeRangeIDKey(raftID, LocalResponseCacheSuffix, detail)
}

// MakeRangeKey creates a range-local key based on the range
// start key, metadata key suffix, and optional detail (e.g. the
// transaction ID for a txn record, etc.).
func MakeRangeKey(key, suffix, detail proto.Key) proto.Key {
	if len(suffix) != LocalSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, LocalSuffixLength))
	}
	return MakeKey(LocalRangePrefix,
		encoding.EncodeBytes(nil, key), suffix, detail)
}

// DecodeRangeKey decodes the range key into range start key,
// suffix and optional detail (may be nil).
func DecodeRangeKey(key proto.Key) (startKey, suffix, detail proto.Key) {
	if !bytes.HasPrefix(key, LocalRangePrefix) {
		panic(fmt.Sprintf("key %q does not have %q prefix",
			key, LocalRangePrefix))
	}
	// Cut the prefix and the Raft ID.
	b := key[len(LocalRangePrefix):]
	b, startKey = encoding.DecodeBytes(b, nil)
	if len(b) < LocalSuffixLength {
		panic(fmt.Sprintf("key %q does not have suffix of length %d",
			key, LocalSuffixLength))
	}
	// Cut the response cache suffix.
	suffix = b[:LocalSuffixLength]
	detail = b[LocalSuffixLength:]
	return
}

// RangeGCMetadataKey returns a range-local key for range garbage
// collection metadata.
func RangeGCMetadataKey(raftID int64) proto.Key {
	return MakeRangeIDKey(raftID, LocalRangeGCMetadataSuffix, proto.Key{})
}

// RangeLastVerificationTimestampKey returns a range-local key for
// the range's last verification timestamp.
func RangeLastVerificationTimestampKey(raftID int64) proto.Key {
	return MakeRangeIDKey(raftID, LocalRangeLastVerificationTimestampSuffix, proto.Key{})
}

// RangeTreeNodeKey returns a range-local key for the the range's
// node in the range tree.
func RangeTreeNodeKey(key proto.Key) proto.Key {
	return MakeRangeKey(key, LocalRangeTreeNodeSuffix, proto.Key{})
}

// RangeDescriptorKey returns a range-local key for the descriptor
// for the range with specified key.
func RangeDescriptorKey(key proto.Key) proto.Key {
	return MakeRangeKey(key, LocalRangeDescriptorSuffix, proto.Key{})
}

// TransactionKey returns a transaction key based on the provided
// transaction key and ID. The base key is encoded in order to
// guarantee that all transaction records for a range sort together.
func TransactionKey(key proto.Key, id []byte) proto.Key {
	return MakeRangeKey(key, LocalTransactionSuffix, proto.Key(id))
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
	if k == nil {
		return nil
	}

	if !bytes.HasPrefix(k, LocalPrefix) {
		return k
	}
	if bytes.HasPrefix(k, LocalRangePrefix) {
		k = k[len(LocalRangePrefix):]
		_, k = encoding.DecodeBytes(k, nil)
		return k
	}
	log.Fatalf("local key %q malformed; should contain prefix %q",
		k, LocalRangePrefix)
	return nil
}

// RangeMetaKey returns a range metadata (meta1, meta2) indexing key
// for the given key. For ordinary keys this returns a level 2
// metadata key - for level 2 keys, it returns a level 1 key. For
// level 1 keys and local keys, KeyMin is returned.
func RangeMetaKey(key proto.Key) proto.Key {
	if len(key) == 0 {
		return proto.KeyMin
	}
	addr := KeyAddress(key)
	if !bytes.HasPrefix(addr, MetaPrefix) {
		return MakeKey(Meta2Prefix, addr)
	}
	if bytes.HasPrefix(addr, Meta2Prefix) {
		return MakeKey(Meta1Prefix, addr[len(Meta2Prefix):])
	}

	return proto.KeyMin
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
	// Key must be at least as long as Meta1Prefix.
	if len(key) < len(Meta1Prefix) {
		return NewInvalidRangeMetaKeyError("too short", key)
	}

	prefix, body := key[:len(Meta1Prefix)], key[len(Meta1Prefix):]

	// The prefix must be equal to Meta1Prefix or Meta2Prefix
	if !bytes.HasPrefix(key, MetaPrefix) {
		return NewInvalidRangeMetaKeyError(
			fmt.Sprintf("does not have %q prefix", MetaPrefix), key)
	}
	if lvl := string(prefix[len(MetaPrefix)]); lvl != "1" && lvl != "2" {
		return NewInvalidRangeMetaKeyError("meta level is not 1 or 2", key)
	}
	// Body of the key must sort <= KeyMax. KeyMax might be the body in
	// the event of doing a lookup of the key "\x00\x00meta1\xff\xff" in
	// order to resolve an intent during a split.
	if proto.KeyMax.Less(body) {
		return NewInvalidRangeMetaKeyError("body of range lookup is > KeyMax", key)
	}

	return nil
}

// DecodeRangeMetaKey returns the bounds within which the desired meta
// record can be found. The given key must be a valid RangeMetaKey.
func DecodeRangeMetaKey(key proto.Key) (proto.Key, proto.Key) {
	if len(key) == 0 {
		// Special case KeyMin: find the first entry in meta1.
		return Meta1Prefix, Meta1Prefix.PrefixEnd()
	}
	// Otherwise find the first entry greater than the given key in the same meta prefix.
	metaPrefix := proto.Key(key[:len(Meta1Prefix)])
	// Edge case for "\x00\x00meta1\xff\xff", which cannot return
	// key.Next() as which itself is the last key in Meta1Prefix range.
	meta1KeyMax := proto.MakeKey(Meta1Prefix, proto.KeyMax)
	if key.Equal(meta1KeyMax) {
		return key, metaPrefix.PrefixEnd()
	}
	return key.Next(), metaPrefix.PrefixEnd()
}
