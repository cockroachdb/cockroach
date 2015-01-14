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
	"github.com/cockroachdb/cockroach/util/log"
)

// MakeKey makes a new key which is the concatenation of the
// given inputs, in order.
func MakeKey(keys ...proto.Key) proto.Key {
	return proto.MakeKey(keys...)
}

// MakeLocalKey is a simple passthrough to MakeKey, with verification
// that the first key has length KeyLocalPrefixLength.
func MakeLocalKey(keys ...proto.Key) proto.Key {
	if len(keys) == 0 {
		log.Fatal("no key components specified in call to MakeLocalKey")
	}
	if len(keys[0]) != KeyLocalPrefixLength {
		log.Fatalf("local key prefix length must be %d: %q", KeyLocalPrefixLength, keys[0])
	}
	return proto.MakeKey(keys...)
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
// However, not all local keys are addressable in the global map.
// Range metadata, response cache entries, and various other keys are
// strictly local, as the non-local suffix is not itself a key
// (e.g. in the case of range metadata, it's the encoded range ID) and
// so are not globally addressable.
func KeyAddress(k proto.Key) proto.Key {
	if !bytes.HasPrefix(k, KeyLocalPrefix) {
		return k
	}
	if len(k) < KeyLocalPrefixLength {
		log.Fatalf("local key %q malformed; should contain prefix %q and four-character designation", k, KeyLocalPrefix)
	}
	return k[KeyLocalPrefixLength:]
}

// RangeDescriptorKey returns a system-local key for the descriptor
// for the range with specified start key.
func RangeDescriptorKey(startKey proto.Key) proto.Key {
	return MakeLocalKey(KeyLocalRangeDescriptorPrefix, startKey)
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

func init() {
	if KeyLocalPrefixLength%7 != 0 {
		log.Fatalf("local key prefix is not a multiple of 7: %d", KeyLocalPrefixLength)
	}
}

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

	// KeyLocalPrefix is the prefix for keys which hold data local to a
	// RocksDB instance, such as range accounting information
	// (e.g. range metadata, range-spanning binary tree node pointers),
	// response cache values, transaction records, and message
	// queues. Some local data are replicated, such as transaction rows,
	// but are located in the local area so that they remain in
	// proximity to one or more keys which they affect, but without
	// unnecessarily polluting the key space. Further, some local data
	// are stored with MVCC and contribute to distributed transactions,
	// such as range metadata, range-spanning binary tree node pointers,
	// and message queues.
	//
	// The local key prefix has been deliberately chosen to sort before
	// the KeySystemPrefix, because these local keys are not addressable
	// via the meta range addressing indexes.
	KeyLocalPrefix = proto.Key("\x00\x00\x00")

	// KeyLocalPrefixLength is the maximum length of the local prefix.
	// It includes both the standard prefix and an additional four
	// characters to designate the type of local data.
	//
	// NOTE: this is very important! In order to support prefix matches
	//   (e.g. for garbage collection of transaction and response cache
	//   rows), the number of bytes in the key local prefix must be a
	//   multiple of 7. This provides an encoded binary string with no
	//   leftover bits to "bleed" into the next byte in the non-prefix
	//   part of the local key.
	KeyLocalPrefixLength = len(KeyLocalPrefix) + 4

	// KeyLocalIdent stores an immutable identifier for this store,
	// created when the store is first bootstrapped.
	KeyLocalIdent = MakeKey(KeyLocalPrefix, proto.Key("iden"))
	// KeyLocalRangeDescriptorPrefix is the prefix for keys storing
	// range descriptors. The value is a struct of type RangeDescriptor.
	KeyLocalRangeDescriptorPrefix = MakeKey(KeyLocalPrefix, proto.Key("rng-"))
	// KeyLocalRangeStatPrefix is the prefix for range statistics.
	KeyLocalRangeStatPrefix = MakeKey(KeyLocalPrefix, proto.Key("rst-"))
	// KeyLocalResponseCachePrefix is the prefix for keys storing command
	// responses used to guarantee idempotency (see ResponseCache).
	KeyLocalResponseCachePrefix = MakeKey(KeyLocalPrefix, proto.Key("res-"))
	// KeyLocalStoreStatPrefix is the prefix for store statistics.
	KeyLocalStoreStatPrefix = MakeKey(KeyLocalPrefix, proto.Key("sst-"))
	// KeyLocalTransactionPrefix specifies the key prefix for
	// transaction records. The suffix is the transaction id.
	KeyLocalTransactionPrefix = MakeKey(KeyLocalPrefix, proto.Key("txn-"))

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
