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
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package keys

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func makeKey(keys ...[]byte) []byte {
	return bytes.Join(keys, nil)
}

// MakeStoreKey creates a store-local key based on the metadata key
// suffix, and optional detail.
func MakeStoreKey(suffix, detail roachpb.RKey) roachpb.Key {
	key := make(roachpb.Key, 0, len(localStorePrefix)+len(suffix)+len(detail))
	key = append(key, localStorePrefix...)
	key = append(key, suffix...)
	key = append(key, detail...)
	return key
}

// StoreIdentKey returns a store-local key for the store metadata.
func StoreIdentKey() roachpb.Key {
	return MakeStoreKey(localStoreIdentSuffix, nil)
}

// StoreGossipKey returns a store-local key for the gossip bootstrap metadata.
func StoreGossipKey() roachpb.Key {
	return MakeStoreKey(localStoreGossipSuffix, nil)
}

// StoreClusterVersionKey returns a store-local key for the cluster version.
func StoreClusterVersionKey() roachpb.Key {
	return MakeStoreKey(localStoreClusterVersionSuffix, nil)
}

// StoreLastUpKey returns the key for the store's "last up" timestamp.
func StoreLastUpKey() roachpb.Key {
	return MakeStoreKey(localStoreLastUpSuffix, nil)
}

// NodeLivenessKey returns the key for the node liveness record.
func NodeLivenessKey(nodeID roachpb.NodeID) roachpb.Key {
	key := make(roachpb.Key, 0, len(NodeLivenessPrefix)+9)
	key = append(key, NodeLivenessPrefix...)
	key = encoding.EncodeUvarintAscending(key, uint64(nodeID))
	return key
}

// NodeStatusKey returns the key for accessing the node status for the
// specified node ID.
func NodeStatusKey(nodeID roachpb.NodeID) roachpb.Key {
	key := make(roachpb.Key, 0, len(StatusNodePrefix)+9)
	key = append(key, StatusNodePrefix...)
	key = encoding.EncodeUvarintAscending(key, uint64(nodeID))
	return key
}

func makePrefixWithRangeID(prefix []byte, rangeID roachpb.RangeID, infix roachpb.RKey) roachpb.Key {
	// Size the key buffer so that it is large enough for most callers.
	key := make(roachpb.Key, 0, 32)
	key = append(key, prefix...)
	key = encoding.EncodeUvarintAscending(key, uint64(rangeID))
	key = append(key, infix...)
	return key
}

// MakeRangeIDPrefix creates a range-local key prefix from
// rangeID for both replicated and unreplicated data.
func MakeRangeIDPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return makePrefixWithRangeID(LocalRangeIDPrefix, rangeID, nil)
}

// MakeRangeIDReplicatedPrefix creates a range-local key prefix from
// rangeID for all Raft replicated data.
func MakeRangeIDReplicatedPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return makePrefixWithRangeID(LocalRangeIDPrefix, rangeID, localRangeIDReplicatedInfix)
}

// makeRangeIDReplicatedKey creates a range-local key based on the range's
// Range ID, metadata key suffix, and optional detail.
func makeRangeIDReplicatedKey(rangeID roachpb.RangeID, suffix, detail roachpb.RKey) roachpb.Key {
	if len(suffix) != localSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, localSuffixLength))
	}

	key := MakeRangeIDReplicatedPrefix(rangeID)
	key = append(key, suffix...)
	key = append(key, detail...)
	return key
}

// DecodeRangeIDKey parses a local range ID key into range ID, infix,
// suffix, and detail.
func DecodeRangeIDKey(
	key roachpb.Key,
) (rangeID roachpb.RangeID, infix, suffix, detail roachpb.Key, err error) {
	if !bytes.HasPrefix(key, LocalRangeIDPrefix) {
		return 0, nil, nil, nil, errors.Errorf("key %s does not have %s prefix", key, LocalRangeIDPrefix)
	}
	// Cut the prefix, the Range ID, and the infix specifier.
	b := key[len(LocalRangeIDPrefix):]
	b, rangeInt, err := encoding.DecodeUvarintAscending(b)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	if len(b) < localSuffixLength+1 {
		return 0, nil, nil, nil, errors.Errorf("malformed key does not contain range ID infix and suffix")
	}
	infix = b[:1]
	b = b[1:]
	suffix = b[:localSuffixLength]
	b = b[localSuffixLength:]

	return roachpb.RangeID(rangeInt), infix, suffix, b, nil
}

// AbortCacheKey returns a range-local key by Range ID for an
// abort cache entry, with detail specified by encoding the
// supplied transaction ID.
func AbortCacheKey(rangeID roachpb.RangeID, txnID uuid.UUID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).AbortCacheKey(txnID)
}

// DecodeAbortCacheKey decodes the provided abort cache entry,
// returning the transaction ID.
func DecodeAbortCacheKey(key roachpb.Key, dest []byte) (uuid.UUID, error) {
	_, _, suffix, detail, err := DecodeRangeIDKey(key)
	if err != nil {
		return uuid.UUID{}, err
	}
	if !bytes.Equal(suffix, LocalAbortCacheSuffix) {
		return uuid.UUID{}, errors.Errorf("key %s does not contain the abort cache suffix %s",
			key, LocalAbortCacheSuffix)
	}
	// Decode the id.
	detail, idBytes, err := encoding.DecodeBytesAscending(detail, dest)
	if err != nil {
		return uuid.UUID{}, err
	}
	if len(detail) > 0 {
		return uuid.UUID{}, errors.Errorf("key %q has leftover bytes after decode: %s; indicates corrupt key", key, detail)
	}
	txnID, err := uuid.FromBytes(idBytes)
	return txnID, err
}

// RaftTombstoneKey returns a system-local key for a raft tombstone.
func RaftTombstoneKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftTombstoneKey()
}

// RaftAppliedIndexKey returns a system-local key for a raft applied index.
func RaftAppliedIndexKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftAppliedIndexKey()
}

// LeaseAppliedIndexKey returns a system-local key for a lease applied index.
func LeaseAppliedIndexKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).LeaseAppliedIndexKey()
}

// RaftTruncatedStateKey returns a system-local key for a RaftTruncatedState.
func RaftTruncatedStateKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftTruncatedStateKey()
}

// RangeFrozenStatusKey returns a system-local key for the frozen status.
func RangeFrozenStatusKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeFrozenStatusKey()
}

// RangeLeaseKey returns a system-local key for a range lease.
func RangeLeaseKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeLeaseKey()
}

// RangeStatsKey returns the key for accessing the MVCCStats struct
// for the specified Range ID.
func RangeStatsKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeStatsKey()
}

// RangeLastGCKey returns a system-local key for last used GC threshold on the
// user keyspace. Reads and writes <= this timestamp will not be served.
//
// TODO(tschottdorf): should be renamed to RangeGCThresholdKey.
func RangeLastGCKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeLastGCKey()
}

// RangeTxnSpanGCThresholdKey returns a system-local key for last used GC
// threshold on the txn span.
func RangeTxnSpanGCThresholdKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeTxnSpanGCThresholdKey()
}

// MakeRangeIDUnreplicatedPrefix creates a range-local key prefix from
// rangeID for all unreplicated data.
func MakeRangeIDUnreplicatedPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return makePrefixWithRangeID(LocalRangeIDPrefix, rangeID, localRangeIDUnreplicatedInfix)
}

// makeRangeIDUnreplicatedKey creates a range-local unreplicated key based
// on the range's Range ID, metadata key suffix, and optional detail.
func makeRangeIDUnreplicatedKey(
	rangeID roachpb.RangeID, suffix roachpb.RKey, detail roachpb.RKey,
) roachpb.Key {
	if len(suffix) != localSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, localSuffixLength))
	}

	key := MakeRangeIDUnreplicatedPrefix(rangeID)
	key = append(key, suffix...)
	key = append(key, detail...)
	return key
}

// RaftHardStateKey returns a system-local key for a Raft HardState.
func RaftHardStateKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftHardStateKey()
}

// RaftLastIndexKey returns a system-local key for the last index of the
// Raft log.
func RaftLastIndexKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftLastIndexKey()
}

// RaftLogPrefix returns the system-local prefix shared by all entries
// in a Raft log.
func RaftLogPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftLogPrefix()
}

// RaftLogKey returns a system-local key for a Raft log entry.
func RaftLogKey(rangeID roachpb.RangeID, logIndex uint64) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftLogKey(logIndex)
}

// RangeLastReplicaGCTimestampKey returns a range-local key for
// the range's last replica GC timestamp.
func RangeLastReplicaGCTimestampKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeLastReplicaGCTimestampKey()
}

// RangeLastVerificationTimestampKeyDeprecated returns a range-local
// key for the range's last verification timestamp.
func RangeLastVerificationTimestampKeyDeprecated(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeLastVerificationTimestampKeyDeprecated()
}

// MakeRangeKey creates a range-local key based on the range
// start key, metadata key suffix, and optional detail (e.g. the
// transaction ID for a txn record, etc.).
func MakeRangeKey(key, suffix, detail roachpb.RKey) roachpb.Key {
	if len(suffix) != localSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, localSuffixLength))
	}
	buf := MakeRangeKeyPrefix(key)
	buf = append(buf, suffix...)
	buf = append(buf, detail...)
	return buf
}

// MakeRangeKeyPrefix creates a key prefix under which all range-local keys
// can be found.
func MakeRangeKeyPrefix(key roachpb.RKey) roachpb.Key {
	buf := make(roachpb.Key, 0, len(LocalRangePrefix)+len(key)+1)
	buf = append(buf, LocalRangePrefix...)
	buf = encoding.EncodeBytesAscending(buf, key)
	return buf
}

// DecodeRangeKey decodes the range key into range start key,
// suffix and optional detail (may be nil).
func DecodeRangeKey(key roachpb.Key) (startKey, suffix, detail roachpb.Key, err error) {
	if !bytes.HasPrefix(key, LocalRangePrefix) {
		return nil, nil, nil, errors.Errorf("key %q does not have %q prefix",
			key, LocalRangePrefix)
	}
	// Cut the prefix and the Range ID.
	b := key[len(LocalRangePrefix):]
	b, startKey, err = encoding.DecodeBytesAscending(b, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(b) < localSuffixLength {
		return nil, nil, nil, errors.Errorf("key %q does not have suffix of length %d",
			key, localSuffixLength)
	}
	// Cut the suffix.
	suffix = b[:localSuffixLength]
	detail = b[localSuffixLength:]
	return
}

// RangeDescriptorKey returns a range-local key for the descriptor
// for the range with specified key.
func RangeDescriptorKey(key roachpb.RKey) roachpb.Key {
	return MakeRangeKey(key, LocalRangeDescriptorSuffix, nil)
}

// TransactionKey returns a transaction key based on the provided
// transaction key and ID. The base key is encoded in order to
// guarantee that all transaction records for a range sort together.
func TransactionKey(key roachpb.Key, txnID uuid.UUID) roachpb.Key {
	rk, err := Addr(key)
	if err != nil {
		panic(err)
	}
	return MakeRangeKey(rk, LocalTransactionSuffix, roachpb.RKey(txnID.GetBytes()))
}

// QueueLastProcessedKey returns a range-local key for last processed
// timestamps for the named queue. These keys represent per-range last
// processed times.
func QueueLastProcessedKey(key roachpb.RKey, queue string) roachpb.Key {
	return MakeRangeKey(key, LocalQueueLastProcessedSuffix, roachpb.RKey(queue))
}

// IsLocal performs a cheap check that returns true iff a range-local key is
// passed, that is, a key for which `Addr` would return a non-identical RKey
// (or a decoding error).
//
// TODO(tschottdorf): we need a better name for these keys as only some of
// them are local and it's been identified as an area that is not understood
// by many of the team's developers. An obvious suggestion is "system" (as
// opposed to "user") keys, but unfortunately that name has already been
// claimed by a related (but not identical) concept.
func IsLocal(k roachpb.Key) bool {
	return bytes.HasPrefix(k, localPrefix)
}

// Addr returns the address for the key, used to lookup the range containing
// the key. In the normal case, this is simply the key's value. However, for
// local keys, such as transaction records, range-spanning binary tree node
// pointers, the address is the inner encoded key, with the local key prefix
// and the suffix and optional detail removed. This address unwrapping is
// performed repeatedly in the case of doubly-local keys. In this way, local
// keys address to the same range as non-local keys, but are stored separately
// so that they don't collide with user-space or global system keys.
//
// However, not all local keys are addressable in the global map. Only range
// local keys incorporating a range key (start key or transaction key) are
// addressable (e.g. range metadata and txn records). Range local keys
// incorporating the Range ID are not (e.g. abort cache entries, and range
// stats).
func Addr(k roachpb.Key) (roachpb.RKey, error) {
	if !IsLocal(k) {
		return roachpb.RKey(k), nil
	}

	for {
		if bytes.HasPrefix(k, localStorePrefix) {
			return nil, errors.Errorf("store-local key %q is not addressable", k)
		}
		if bytes.HasPrefix(k, LocalRangeIDPrefix) {
			return nil, errors.Errorf("local range ID key %q is not addressable", k)
		}
		if !bytes.HasPrefix(k, LocalRangePrefix) {
			return nil, errors.Errorf("local key %q malformed; should contain prefix %q",
				k, LocalRangePrefix)
		}
		k = k[len(LocalRangePrefix):]
		var err error
		// Decode the encoded key, throw away the suffix and detail.
		if _, k, err = encoding.DecodeBytesAscending(k, nil); err != nil {
			return nil, err
		}
		if !IsLocal(k) {
			break
		}
	}
	return roachpb.RKey(k), nil
}

// MustAddr calls Addr and panics on errors.
func MustAddr(k roachpb.Key) roachpb.RKey {
	rk, err := Addr(k)
	if err != nil {
		panic(errors.Wrapf(err, "could not take address of '%s'", k))
	}
	return rk
}

// AddrUpperBound returns the address for the key, used to lookup the range containing
// the key. However, unlike Addr, it will return the following key that local range
// keys address to. This is necessary because range-local keys exist conceptually in the
// space between regular keys. Addr() returns the regular key that is just to the left
// of a range-local key, which is guaranteed to be located on the same range. AddrUpperBound()
// returns the regular key that is just to the right, which may not be on the same range
// but is suitable for use as the EndKey of a span involving a range-local key.
func AddrUpperBound(k roachpb.Key) (roachpb.RKey, error) {
	rk, err := Addr(k)
	if err != nil {
		return rk, err
	}
	if IsLocal(k) {
		// The upper bound for a range-local key that addresses to key k
		// is the key directly after k.
		rk = rk.Next()
	}
	return rk, nil
}

// RangeMetaKey returns a range metadata (meta1, meta2) indexing key for the
// given key.
//
// - For RKeyMin, KeyMin is returned.
// - For a meta1 key, KeyMin is returned.
// - For a meta2 key, a meta1 key is returned.
// - For an ordinary key, a meta2 key is returned.
func RangeMetaKey(key roachpb.RKey) roachpb.Key {
	if len(key) == 0 { // key.Equal(roachpb.RKeyMin)
		return roachpb.KeyMin
	}
	var prefix roachpb.Key
	switch key[0] {
	case meta1PrefixByte:
		return roachpb.KeyMin
	case meta2PrefixByte:
		prefix = Meta1Prefix
		key = key[len(Meta2Prefix):]
	default:
		prefix = Meta2Prefix
	}

	buf := make(roachpb.Key, 0, len(prefix)+len(key))
	buf = append(buf, prefix...)
	buf = append(buf, key...)
	return buf
}

// UserKey returns an ordinary key for the given range metadata (meta1, meta2)
// indexing key.
//
// - For RKeyMin, Meta1Prefix is returned.
// - For a meta1 key, a meta2 key is returned.
// - For a meta2 key, an ordinary key is returned.
// - For an ordinary key, the input key is returned.
func UserKey(key roachpb.RKey) roachpb.RKey {
	if len(key) == 0 { // key.Equal(roachpb.RKeyMin)
		return roachpb.RKey(Meta1Prefix)
	}
	var prefix roachpb.Key
	switch key[0] {
	case meta1PrefixByte:
		prefix = Meta2Prefix
		key = key[len(Meta1Prefix):]
	case meta2PrefixByte:
		key = key[len(Meta2Prefix):]
	}

	buf := make(roachpb.RKey, 0, len(prefix)+len(key))
	buf = append(buf, prefix...)
	buf = append(buf, key...)
	return buf
}

// validateRangeMetaKey validates that the given key is a valid Range Metadata
// key. This checks only the constraints common to forward and backwards scans:
// correct prefix and not exceeding KeyMax.
func validateRangeMetaKey(key roachpb.RKey) error {
	// KeyMin is a valid key.
	if key.Equal(roachpb.RKeyMin) {
		return nil
	}
	// Key must be at least as long as Meta1Prefix.
	if len(key) < len(Meta1Prefix) {
		return NewInvalidRangeMetaKeyError("too short", key)
	}

	prefix, body := key[:len(Meta1Prefix)], key[len(Meta1Prefix):]
	if !prefix.Equal(Meta2Prefix) && !prefix.Equal(Meta1Prefix) {
		return NewInvalidRangeMetaKeyError("not a meta key", key)
	}

	if roachpb.RKeyMax.Less(body) {
		return NewInvalidRangeMetaKeyError("body of meta key range lookup is > KeyMax", key)
	}
	return nil
}

// MetaScanBounds returns the range [start,end) within which the desired meta
// record can be found by means of an engine scan. The given key must be a
// valid RangeMetaKey as defined by validateRangeMetaKey.
// TODO(tschottdorf): a lot of casting going on inside.
func MetaScanBounds(key roachpb.RKey) (roachpb.Key, roachpb.Key, error) {
	if err := validateRangeMetaKey(key); err != nil {
		return nil, nil, err
	}

	if key.Equal(Meta2KeyMax) {
		return nil, nil, NewInvalidRangeMetaKeyError("Meta2KeyMax can't be used as the key of scan", key)
	}

	if key.Equal(roachpb.RKeyMin) {
		// Special case KeyMin: find the first entry in meta1.
		return Meta1Prefix, Meta1Prefix.PrefixEnd(), nil
	}
	if key.Equal(Meta1KeyMax) {
		// Special case Meta1KeyMax: this is the last key in Meta1, we don't want
		// to start at Next().
		return Meta1KeyMax, Meta1Prefix.PrefixEnd(), nil
	}
	// Otherwise find the first entry greater than the given key in the same meta prefix.
	return key.Next().AsRawKey(), key[:len(Meta1Prefix)].PrefixEnd().AsRawKey(), nil
}

// MetaReverseScanBounds returns the range [start,end) within which the desired
// meta record can be found by means of a reverse engine scan. The given key
// must be a valid RangeMetaKey as defined by validateRangeMetaKey.
func MetaReverseScanBounds(key roachpb.RKey) (roachpb.Key, roachpb.Key, error) {
	if err := validateRangeMetaKey(key); err != nil {
		return nil, nil, err
	}

	if key.Equal(roachpb.RKeyMin) || key.Equal(Meta1Prefix) {
		return nil, nil, NewInvalidRangeMetaKeyError("KeyMin and Meta1Prefix can't be used as the key of reverse scan", key)
	}
	if key.Equal(Meta2Prefix) {
		// Special case Meta2Prefix: this is the first key in Meta2, and the scan
		// interval covers all of Meta1.
		return Meta1Prefix, key.Next().AsRawKey(), nil
	}
	// Otherwise find the first entry greater than the given key and find the last entry
	// in the same prefix. For MVCCReverseScan the endKey is exclusive, if we want to find
	// the range descriptor the given key specified,we need to set the key.Next() as the
	// MVCCReverseScan`s endKey. For example:
	// If we have ranges [a,f) and [f,z), then we'll have corresponding meta records
	// at f and z. If you're looking for the meta record for key f, then you want the
	// second record (exclusive in MVCCReverseScan), hence key.Next() below.
	return key[:len(Meta1Prefix)].AsRawKey(), key.Next().AsRawKey(), nil
}

// MakeTablePrefix returns the key prefix used for the table's data.
func MakeTablePrefix(tableID uint32) []byte {
	return encoding.EncodeUvarintAscending(nil, uint64(tableID))
}

// DecodeTablePrefix validates that the given key has a table prefix, returning
// the remainder of the key (with the prefix removed) and the decoded descriptor
// ID of the table.
func DecodeTablePrefix(key roachpb.Key) ([]byte, uint64, error) {
	if encoding.PeekType(key) != encoding.Int {
		return key, 0, errors.Errorf("invalid key prefix: %q", key)
	}
	return encoding.DecodeUvarintAscending(key)
}

// MakeFamilyKey returns the key for the family in the given row by appending to
// the passed key.
func MakeFamilyKey(key []byte, famID uint32) []byte {
	if famID == 0 {
		// As an optimization, family 0 is encoded without a length suffix.
		return encoding.EncodeUvarintAscending(key, 0)
	}
	size := len(key)
	key = encoding.EncodeUvarintAscending(key, uint64(famID))
	// Note that we assume that `len(key)-size` will always be encoded to a
	// single byte by EncodeUvarint. This is currently always true because the
	// varint encoding will encode 1-9 bytes.
	return encoding.EncodeUvarintAscending(key, uint64(len(key)-size))
}

// EnsureSafeSplitKey transforms an SQL table key such that it is a valid split key
// (i.e. does not occur in the middle of a row).
func EnsureSafeSplitKey(key roachpb.Key) (roachpb.Key, error) {
	if encoding.PeekType(key) != encoding.Int {
		// Not a table key, so already a split key.
		return key, nil
	}

	n := len(key)
	// The column ID length is encoded as a varint and we take advantage of the
	// fact that the column ID itself will be encoded in 0-9 bytes and thus the
	// length of the column ID data will fit in a single byte.
	buf := key[n-1:]
	if encoding.PeekType(buf) != encoding.Int {
		// The last byte is not a valid column ID suffix.
		return nil, errors.Errorf("%s: not a valid table key", key)
	}

	// Strip off the family ID / column ID suffix from the buf. The last byte of the buf
	// contains the length of the column ID suffix (which might be 0 if the buf
	// does not contain a column ID suffix).
	_, colIDLen, err := encoding.DecodeUvarintAscending(buf)
	if err != nil {
		return nil, err
	}
	if int(colIDLen)+1 > n {
		// The column ID length was impossible. colIDLen is the length of
		// the encoded column ID suffix. We add 1 to account for the byte
		// holding the length of the encoded column ID and if that total
		// (colIDLen+1) is greater than the key suffix (n == len(buf))
		// then we bail. Note that we don't consider this an error because
		// EnsureSafeSplitKey can be called on keys that look like table
		// keys but which do not have a column ID length suffix (e.g
		// by SystemConfig.ComputeSplitKey).
		return nil, errors.Errorf("%s: malformed table key", key)
	}
	return key[:len(key)-int(colIDLen)-1], nil
}

// Range returns a key range encompassing all the keys in the Batch.
func Range(ba roachpb.BatchRequest) (roachpb.RSpan, error) {
	from := roachpb.RKeyMax
	to := roachpb.RKeyMin
	for _, arg := range ba.Requests {
		req := arg.GetInner()
		if _, ok := req.(*roachpb.NoopRequest); ok {
			continue
		}
		h := req.Header()
		if !roachpb.IsRange(req) && len(h.EndKey) != 0 {
			return roachpb.RSpan{}, errors.Errorf("end key specified for non-range operation: %s", req)
		}

		key, err := Addr(h.Key)
		if err != nil {
			return roachpb.RSpan{}, err
		}
		if key.Less(from) {
			// Key is smaller than `from`.
			from = key
		}
		if !key.Less(to) {
			// Key.Next() is larger than `to`.
			if bytes.Compare(key, roachpb.RKeyMax) > 0 {
				return roachpb.RSpan{}, errors.Errorf("%s must be less than KeyMax", key)
			}
			to = key.Next()
		}

		if len(h.EndKey) == 0 {
			continue
		}
		endKey, err := AddrUpperBound(h.EndKey)
		if err != nil {
			return roachpb.RSpan{}, err
		}
		if bytes.Compare(roachpb.RKeyMax, endKey) < 0 {
			return roachpb.RSpan{}, errors.Errorf("%s must be less than or equal to KeyMax", endKey)
		}
		if to.Less(endKey) {
			// EndKey is larger than `to`.
			to = endKey
		}
	}
	return roachpb.RSpan{Key: from, EndKey: to}, nil
}

// RangeIDPrefixBuf provides methods for generating range ID local keys while
// avoiding an allocation on every key generated. The generated keys are only
// valid until the next call to one of the key generation methods.
type RangeIDPrefixBuf roachpb.Key

// MakeRangeIDPrefixBuf creates a new range ID prefix buf suitable for
// generating the various range ID local keys.
func MakeRangeIDPrefixBuf(rangeID roachpb.RangeID) RangeIDPrefixBuf {
	return RangeIDPrefixBuf(MakeRangeIDPrefix(rangeID))
}

func (b RangeIDPrefixBuf) replicatedPrefix() roachpb.Key {
	return append(roachpb.Key(b), localRangeIDReplicatedInfix...)
}

func (b RangeIDPrefixBuf) unreplicatedPrefix() roachpb.Key {
	return append(roachpb.Key(b), localRangeIDUnreplicatedInfix...)
}

// AbortCacheKey returns a range-local key by Range ID for an abort cache
// entry, with detail specified by encoding the supplied transaction ID.
func (b RangeIDPrefixBuf) AbortCacheKey(txnID uuid.UUID) roachpb.Key {
	key := append(b.replicatedPrefix(), LocalAbortCacheSuffix...)
	return encoding.EncodeBytesAscending(key, txnID.GetBytes())
}

// RaftTombstoneKey returns a system-local key for a raft tombstone.
func (b RangeIDPrefixBuf) RaftTombstoneKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRaftTombstoneSuffix...)
}

// RaftAppliedIndexKey returns a system-local key for a raft applied index.
func (b RangeIDPrefixBuf) RaftAppliedIndexKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRaftAppliedIndexSuffix...)
}

// LeaseAppliedIndexKey returns a system-local key for a lease applied index.
func (b RangeIDPrefixBuf) LeaseAppliedIndexKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalLeaseAppliedIndexSuffix...)
}

// RaftTruncatedStateKey returns a system-local key for a RaftTruncatedState.
func (b RangeIDPrefixBuf) RaftTruncatedStateKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRaftTruncatedStateSuffix...)
}

// RangeFrozenStatusKey returns a system-local key for the frozen status.
func (b RangeIDPrefixBuf) RangeFrozenStatusKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRangeFrozenStatusSuffix...)
}

// RangeLeaseKey returns a system-local key for a range lease.
func (b RangeIDPrefixBuf) RangeLeaseKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRangeLeaseSuffix...)
}

// RangeStatsKey returns the key for accessing the MVCCStats struct
// for the specified Range ID.
func (b RangeIDPrefixBuf) RangeStatsKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRangeStatsSuffix...)
}

// RangeLastGCKey returns a system-local key for the last GC.
func (b RangeIDPrefixBuf) RangeLastGCKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRangeLastGCSuffix...)
}

// RangeTxnSpanGCThresholdKey returns a system-local key for last used GC
// threshold on the txn span.
func (b RangeIDPrefixBuf) RangeTxnSpanGCThresholdKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalTxnSpanGCThresholdSuffix...)
}

// RaftHardStateKey returns a system-local key for a Raft HardState.
func (b RangeIDPrefixBuf) RaftHardStateKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRaftHardStateSuffix...)
}

// RaftLastIndexKey returns a system-local key for the last index of the
// Raft log.
func (b RangeIDPrefixBuf) RaftLastIndexKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRaftLastIndexSuffix...)
}

// RaftLogPrefix returns the system-local prefix shared by all entries
// in a Raft log.
func (b RangeIDPrefixBuf) RaftLogPrefix() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRaftLogSuffix...)
}

// RaftLogKey returns a system-local key for a Raft log entry.
func (b RangeIDPrefixBuf) RaftLogKey(logIndex uint64) roachpb.Key {
	return encoding.EncodeUint64Ascending(b.RaftLogPrefix(), logIndex)
}

// RangeLastReplicaGCTimestampKey returns a range-local key for
// the range's last replica GC timestamp.
func (b RangeIDPrefixBuf) RangeLastReplicaGCTimestampKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRangeLastReplicaGCTimestampSuffix...)
}

// RangeLastVerificationTimestampKeyDeprecated returns a range-local
// key for the range's last verification timestamp.
func (b RangeIDPrefixBuf) RangeLastVerificationTimestampKeyDeprecated() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRangeLastVerificationTimestampSuffixDeprecated...)
}

// RangeReplicaDestroyedErrorKey returns a range-local key for
// the range's replica destroyed error.
func (b RangeIDPrefixBuf) RangeReplicaDestroyedErrorKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRangeReplicaDestroyedErrorSuffix...)
}
