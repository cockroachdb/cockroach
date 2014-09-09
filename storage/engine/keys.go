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

	"github.com/cockroachdb/cockroach/proto"
)

// Key defines the key in the key-value datastore.
type Key []byte

// KeyRange provides a key range from start to end.  If end is nil,
// the key range is defined only for a single key: start.
type KeyRange struct {
	Start, End Key
}

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
	Timestamp proto.Timestamp
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
	if !bytes.HasPrefix(key, KeySystemPrefix) {
		return MakeKey(KeyMeta2Prefix, key)
	}
	if bytes.HasPrefix(key, KeyMeta2Prefix) {
		return MakeKey(KeyMeta1Prefix, key[len(KeyMeta2Prefix):])
	}

	return KeyMin
}

// RangeMetadataLookupKey returns the metadata key at which this range
// descriptor should be stored as a value.
func RangeMetadataLookupKey(r *proto.RangeDescriptor) Key {
	return RangeMetaKey(r.EndKey)
}

// ValidateRangeMetaKey validates that the given key is a valid Range Metadata
// key.  It must have an appropriate metadata range prefix, and the original key
// value must be less thas KeyMax.  As a special case, KeyMin is considered a
// valid Range Metadata Key.
func ValidateRangeMetaKey(key Key) error {
	// KeyMin is a valid key.
	if len(key) == 0 {
		return nil
	}
	// Key must be at least as long as KeyMeta1Prefix.
	if len(key) < len(KeyMeta1Prefix) {
		return NewInvalidRangeMetaKeyError(key)
	}

	prefix, body := key[:len(KeyMeta1Prefix)], key[len(KeyMeta1Prefix):]

	// The prefix must be equal to KeyMeta1Prefix or KeyMeta2Prefix
	if !bytes.HasPrefix(key, KeyMetaPrefix) {
		return NewInvalidRangeMetaKeyError(key)
	}
	if lvl := string(prefix[len(KeyMetaPrefix)]); lvl != "1" && lvl != "2" {
		return NewInvalidRangeMetaKeyError(key)
	}
	// Body of the key must sort before KeyMax
	if !body.Less(KeyMax) {
		return NewInvalidRangeMetaKeyError(key)
	}

	return nil
}

// Constants for system-reserved keys in the KV map.
var (
	// KeyMin is a minimum key value which sorts before all other keys.
	KeyMin = Key("")
	// KeyMax is a maximum key value which sorts after all other
	// keys. Because keys are stored using an ordered encoding (see
	// storage/encoding.go), they will never start with \xff.
	KeyMax = Key("\xff")

	// KeyLocalPrefix is the prefix for keys which hold data local to a
	// RocksDB instance, such as range accounting information (e.g. key
	// samples), response cache values and transaction records. Some
	// local data are replicated, such as transaction rows, but are
	// located in the local area so that they remain in proximity to one
	// or more keys which they affect, but without unnecessarily
	// polluting the key space.
	//
	// The local key prefix has been deliberately chosen to sort before
	// the KeySystemPrefix, because these local keys are not addressable
	// via the meta range addressing indexes.
	KeyLocalPrefix = Key("\x00\x00\x00")

	// KeyLocalIdent stores an immutable identifier for this store,
	// created when the store is first bootstrapped.
	KeyLocalIdent = MakeKey(KeyLocalPrefix, Key("store-ident"))
	// KeyLocalRangeIDGenerator is a range ID generator sequence. Range IDs
	// must be unique per node ID.
	KeyLocalRangeIDGenerator = MakeKey(KeyLocalPrefix, Key("range-idgen"))
	// KeyLocalRangeMetadataPrefix is the prefix for keys storing range metadata.
	// The value is a struct of type RangeMetadata.
	KeyLocalRangeMetadataPrefix = MakeKey(KeyLocalPrefix, Key("range-"))
	// KeyLocalRangeResponseCachePrefix is the prefix for keys storing command
	// responses used to guarantee idempotency (see ResponseCache). This key
	// prefix is duplicated in rocksdb_compaction.cc and must be kept in sync
	// if modified here.
	KeyLocalRangeResponseCachePrefix = MakeKey(KeyLocalPrefix, Key("respcache-"))
	// KeyLocalTransactionPrefix specifies the key prefix for
	// transaction records. The suffix is the transaction id. This key
	// prefix is duplicated in rocksdb_compaction.cc and must be kept in
	// sync if modified here.
	KeyLocalTransactionPrefix = MakeKey(KeyLocalPrefix, Key("txn-"))

	// KeySystemPrefix indicates the beginning of the key range for
	// global, system data which are replicated across the cluster.
	KeySystemPrefix = Key("\x00")

	// KeyMetaPrefix is the prefix for range metadata keys. Notice that
	// an extra null character in the prefix causes all range addressing
	// records to sort before any system tables which they might describe.
	KeyMetaPrefix = MakeKey(KeySystemPrefix, Key("\x00meta"))
	// KeyMeta1Prefix is the first level of key addressing. The value is a
	// RangeDescriptor struct.
	KeyMeta1Prefix = MakeKey(KeyMetaPrefix, Key("1"))
	// KeyMeta2Prefix is the second level of key addressing. The value is a
	// RangeDescriptor struct.
	KeyMeta2Prefix = MakeKey(KeyMetaPrefix, Key("2"))

	// KeyMetaMax is the end of the range of addressing keys.
	KeyMetaMax = MakeKey(KeySystemPrefix, Key("\x01"))

	// KeyConfigAccountingPrefix specifies the key prefix for accounting
	// configurations. The suffix is the affected key prefix.
	KeyConfigAccountingPrefix = MakeKey(KeySystemPrefix, Key("acct"))
	// KeyConfigPermissionPrefix specifies the key prefix for accounting
	// configurations. The suffix is the affected key prefix.
	KeyConfigPermissionPrefix = MakeKey(KeySystemPrefix, Key("perm"))
	// KeyConfigZonePrefix specifies the key prefix for zone
	// configurations. The suffix is the affected key prefix.
	KeyConfigZonePrefix = MakeKey(KeySystemPrefix, Key("zone"))
	// KeyTransactionPrefix specifies the key prefix for transaction
	// records. The suffix is the transaction id.
	KeyTransactionPrefix = MakeKey(KeySystemPrefix, Key("tx"))
	// KeyNodeIDGenerator contains a sequence generator for node IDs.
	KeyNodeIDGenerator = MakeKey(KeySystemPrefix, Key("node-idgen"))
	// KeySchemaPrefix specifies key prefixes for schema definitions.
	KeySchemaPrefix = MakeKey(KeySystemPrefix, Key("schema"))
	// KeyStoreIDGeneratorPrefix specifies key prefixes for sequence
	// generators, one per node, for store IDs.
	KeyStoreIDGeneratorPrefix = MakeKey(KeySystemPrefix, Key("store-idgen-"))
)
