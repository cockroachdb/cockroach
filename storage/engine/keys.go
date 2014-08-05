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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package engine

import (
	"bytes"

	"github.com/cockroachdb/cockroach/hlc"
)

// Key defines the key in the key-value datastore.
type Key []byte

// Less implements the util.Ordered interface.
func (k Key) Less(l Key) bool {
	return bytes.Compare(k, l) == -1
}

// Value specifies the value at a key. Multiple values at the same key
// are supported based on timestamp.
type Value struct {
	// Bytes is the byte string value.
	Bytes []byte
	// Checksum is a CRC-32-IEEE checksum. A Value will only be used in
	// a write operation by the database if either its checksum is zero
	// or the CRC checksum of Bytes matches it.
	// Values returned by the database will contain a checksum of the
	// contained value.
	Checksum uint32
	// Timestamp of value.
	Timestamp hlc.HLTimestamp
}

// KeyValue is a pair of Key and Value for returned Key/Value pairs
// from ScanRequest/ScanResponse. It embeds a Key and a Value.
type KeyValue struct {
	Key
	Value
}

// MakeKey makes a new key which is the concatenation of the
// given inputs, in order.
func MakeKey(prefix Key, keys ...Key) Key {
	suffix := []byte(nil)
	for _, k := range keys {
		suffix = append(suffix, []byte(k)...)
	}
	return Key(bytes.Join([][]byte{prefix, suffix}, []byte{}))
}

// PrefixEndKey determines the end key given a start key as a prefix,
// that is the key that sorts precisely behind all keys starting with
// prefix: "1" is added to the final byte and the carry propagated.
// The special cases of nil and KeyMin ("") always returns KeyMax ("\xff").
func PrefixEndKey(prefix Key) Key {
	if bytes.Equal(prefix, KeyMin) || len(prefix) == 0 {
		return KeyMax
	}
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end
		}
	}
	// This statement will only be reached if the key is already a
	// maximal byte string (i.e. already \xff...).
	return prefix
}

// NextKey returns a new Key that sorts immediately after the given
// key. No special behaviour applies for KeyMax. nil is treated like
// the empty key.
func NextKey(k Key) Key {
	return MakeKey(k, Key{0})
}

// RangeMetaKey returns a range metadata key for the given key.  For ordinary
// keys this returns a level 2 metadata key - for level 2 keys, it returns a
// level 1 key.  For level 1 keys and local keys, KeyMin is returned.
func RangeMetaKey(key Key) Key {
	if len(key) == 0 {
		return KeyMin
	}
	if !bytes.HasPrefix(key, KeyReplicatedPrefix) {
		return MakeKey(KeyMeta2Prefix, key)
	}
	if bytes.HasPrefix(key, KeyMeta2Prefix) {
		return MakeKey(KeyMeta1Prefix, key[len(KeyMeta2Prefix):])
	}

	return KeyMin
}

// Constants for system-reserved keys in the KV map.
var (
	// KeyMin is a minimum key value which sorts before all other keys.
	KeyMin = Key("")
	// KeyMax is a maximum key value which sorts after all other
	// keys. Because keys are stored using an ordered encoding (see
	// storage/encoding.go), they will never start with \xff.
	KeyMax = Key("\xff")

	// KeyLocalPrefix is the prefix for keys which hold data local to
	// a RocksDB instance (and is not replicated), such as accounting
	// information relevant to the load of ranges. It is chosen to sort before
	// KeyReplicatedPrefix. Data at these keys is local to this store and is
	// not replicated via raft nor is it available via access to the global
	// key-value store.
	KeyLocalPrefix = Key("\x00\x00\x00")

	// KeyLocalIdent stores an immutable identifier for this store,
	// created when the store is first bootstrapped.
	KeyLocalIdent = MakeKey(KeyLocalPrefix, Key("store-ident"))
	// KeyLocalRangeIDGenerator is a range ID generator sequence. Range IDs
	// must be unique per node ID.
	KeyLocalRangeIDGenerator = MakeKey(KeyLocalPrefix, Key("range-id-generator"))
	// KeyLocalRangeMetadataPrefix is the prefix for keys storing range metadata.
	// The value is a struct of type RangeMetadata.
	KeyLocalRangeMetadataPrefix = MakeKey(KeyLocalPrefix, Key("range-"))
	// KeyLocalRangeSamplePrefix is the prefix for keys storing range write
	// samples.
	KeyLocalRangeSamplePrefix = MakeKey(KeyLocalPrefix, Key("keysample-"))
	// KeyLocalRangeResponseCache is the prefix for keys storing command
	// responses used to guarantee idempotency (see ResponseCache).
	KeyLocalRangeResponseCachePrefix = MakeKey(KeyLocalPrefix, Key("respcache-"))

	// KeyReplicatedPrefix indicates the beginning of the key range
	// that is replicated across the cluster.
	KeyReplicatedPrefix = Key("\x00\x00")

	// KeyMetaPrefix is the prefix for range metadata keys.
	KeyMetaPrefix = MakeKey(KeyReplicatedPrefix, Key("meta"))
	// KeyMeta1Prefix is the first level of key addressing. The value is a
	// RangeDescriptor struct.
	KeyMeta1Prefix = MakeKey(KeyMetaPrefix, Key("1"))
	// KeyMeta2Prefix is the second level of key addressing. The value is a
	// RangeDescriptor struct.
	KeyMeta2Prefix = MakeKey(KeyMetaPrefix, Key("2"))

	// KeyMetaMax is the end of the range of addressing keys.
	KeyMetaMax = Key("\x00\x01")

	// KeyConfigAccountingPrefix specifies the key prefix for accounting
	// configurations. The suffix is the affected key prefix.
	KeyConfigAccountingPrefix = Key("\x00acct")
	// KeyConfigPermissionPrefix specifies the key prefix for accounting
	// configurations. The suffix is the affected key prefix.
	KeyConfigPermissionPrefix = Key("\x00perm")
	// KeyConfigZonePrefix specifies the key prefix for zone
	// configurations. The suffix is the affected key prefix.
	KeyConfigZonePrefix = Key("\x00zone")
	// KeyNodeIDGenerator contains a sequence generator for node IDs.
	KeyNodeIDGenerator = Key("\x00node-id-generator")
	// KeyStoreIDGeneratorPrefix specifies key prefixes for sequence
	// generators, one per node, for store IDs.
	KeyStoreIDGeneratorPrefix = Key("\x00store-id-generator-")
)
