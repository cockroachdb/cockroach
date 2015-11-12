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
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

// MakeKey makes a new key which is the concatenation of the
// given inputs, in order.
func MakeKey(keys ...[]byte) []byte {
	return roachpb.MakeKey(keys...)
}

// MakeStoreKey creates a store-local key based on the metadata key
// suffix, and optional detail.
func MakeStoreKey(suffix, detail roachpb.RKey) roachpb.Key {
	return roachpb.Key(MakeKey(localStorePrefix, suffix, detail))
}

// StoreIdentKey returns a store-local key for the store metadata.
func StoreIdentKey() roachpb.Key {
	return MakeStoreKey(localStoreIdentSuffix, roachpb.RKey{})
}

// StoreStatusKey returns the key for accessing the store status for the
// specified store ID.
func StoreStatusKey(storeID int32) roachpb.Key {
	return roachpb.Key(MakeKey(StatusStorePrefix, encoding.EncodeUvarint(nil, uint64(storeID))))
}

// NodeStatusKey returns the key for accessing the node status for the
// specified node ID.
func NodeStatusKey(nodeID int32) roachpb.Key {
	return MakeKey(StatusNodePrefix, encoding.EncodeUvarint(nil, uint64(nodeID)))
}

// MakeRangeIDPrefix creates a range-local key prefix from
// rangeID.
func MakeRangeIDPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return MakeKey(LocalRangeIDPrefix, encoding.EncodeUvarint(nil, uint64(rangeID)))
}

// MakeRangeIDKey creates a range-local key based on the range's
// Range ID, metadata key suffix, and optional detail.
func MakeRangeIDKey(rangeID roachpb.RangeID, suffix, detail roachpb.RKey) roachpb.Key {
	if len(suffix) != localSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, localSuffixLength))
	}
	return MakeKey(MakeRangeIDPrefix(rangeID), suffix, detail)
}

// RaftLogKey returns a system-local key for a Raft log entry.
func RaftLogKey(rangeID roachpb.RangeID, logIndex uint64) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftLogSuffix,
		encoding.EncodeUint64(nil, logIndex))
}

// RaftLogPrefix returns the system-local prefix shared by all entries in a Raft log.
func RaftLogPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftLogSuffix, roachpb.RKey{})
}

// RaftHardStateKey returns a system-local key for a Raft HardState.
func RaftHardStateKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftHardStateSuffix, roachpb.RKey{})
}

// RaftTruncatedStateKey returns a system-local key for a RaftTruncatedState.
func RaftTruncatedStateKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftTruncatedStateSuffix, roachpb.RKey{})
}

// RaftAppliedIndexKey returns a system-local key for a raft applied index.
func RaftAppliedIndexKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftAppliedIndexSuffix, roachpb.RKey{})
}

// RaftLeaderLeaseKey returns a system-local key for a raft leader lease.
func RaftLeaderLeaseKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftLeaderLeaseSuffix, roachpb.RKey{})
}

// RaftTombstoneKey returns a system-local key for a raft tombstone.
func RaftTombstoneKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftTombstoneSuffix, roachpb.RKey{})
}

// RaftLastIndexKey returns a system-local key for a raft last index.
func RaftLastIndexKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftLastIndexSuffix, roachpb.RKey{})
}

// RangeStatsKey returns the key for accessing the MVCCStats struct
// for the specified Range ID.
func RangeStatsKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRangeStatsSuffix, roachpb.RKey{})
}

// SequenceCacheKey returns a range-local key by Range ID for a
// sequence cache entry, with detail specified by encoding the
// supplied transaction ID and sequence number.
func SequenceCacheKey(rangeID roachpb.RangeID, id []byte, seq uint32) roachpb.Key {
	return MakeRangeIDKey(rangeID, LocalSequenceCacheSuffix,
		encoding.EncodeUint32Decreasing(
			encoding.EncodeBytes(nil, id), seq))
}

// SequenceCacheKeyPrefix returns the prefix common to all sequence cache keys
// for the given ID.
func SequenceCacheKeyPrefix(rangeID roachpb.RangeID, id []byte) roachpb.Key {
	return MakeRangeIDKey(rangeID, LocalSequenceCacheSuffix, encoding.EncodeBytes(nil, id))
}

// MakeRangeKey creates a range-local key based on the range
// start key, metadata key suffix, and optional detail (e.g. the
// transaction ID for a txn record, etc.).
func MakeRangeKey(key, suffix, detail roachpb.RKey) roachpb.Key {
	if len(suffix) != localSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, localSuffixLength))
	}
	return MakeKey(MakeRangeKeyPrefix(key), suffix, detail)
}

// MakeRangeKeyPrefix creates a key prefix under which all range-local keys
// can be found.
func MakeRangeKeyPrefix(key roachpb.RKey) roachpb.Key {
	return MakeKey(LocalRangePrefix, encoding.EncodeBytes(nil, key))
}

// DecodeRangeKey decodes the range key into range start key,
// suffix and optional detail (may be nil).
func DecodeRangeKey(key roachpb.Key) (startKey, suffix, detail roachpb.Key, err error) {
	if !bytes.HasPrefix(key, LocalRangePrefix) {
		return nil, nil, nil, util.Errorf("key %q does not have %q prefix",
			key, LocalRangePrefix)
	}
	// Cut the prefix and the Range ID.
	b := key[len(LocalRangePrefix):]
	b, startKey, err = encoding.DecodeBytes(b, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(b) < localSuffixLength {
		return nil, nil, nil, util.Errorf("key %q does not have suffix of length %d",
			key, localSuffixLength)
	}
	// Cut the suffix.
	suffix = b[:localSuffixLength]
	detail = b[localSuffixLength:]
	return
}

// RangeGCMetadataKey returns a range-local key for range garbage
// collection metadata.
func RangeGCMetadataKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRangeGCMetadataSuffix, roachpb.RKey{})
}

// RangeLastVerificationTimestampKey returns a range-local key for
// the range's last verification timestamp.
func RangeLastVerificationTimestampKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRangeLastVerificationTimestampSuffix, roachpb.RKey{})
}

// RangeTreeNodeKey returns a range-local key for the range's
// node in the range tree.
func RangeTreeNodeKey(key roachpb.RKey) roachpb.Key {
	return MakeRangeKey(key, localRangeTreeNodeSuffix, roachpb.RKey{})
}

// RangeDescriptorKey returns a range-local key for the descriptor
// for the range with specified key.
func RangeDescriptorKey(key roachpb.RKey) roachpb.Key {
	return MakeRangeKey(key, LocalRangeDescriptorSuffix, roachpb.RKey{})
}

// TransactionKey returns a transaction key based on the provided
// transaction key and ID. The base key is encoded in order to
// guarantee that all transaction records for a range sort together.
func TransactionKey(key roachpb.Key, id []byte) roachpb.Key {
	return MakeRangeKey(Addr(key), localTransactionSuffix, roachpb.RKey(id))
}

// Addr returns the address for the key, used to lookup the range containing
// the key. In the normal case, this is simply the key's value. However, for
// local keys, such as transaction records, range-spanning binary tree node
// pointers, the address is the trailing suffix of the key, with the local key
// prefix removed. In this way, local keys address to the same range as
// non-local keys, but are stored separately so that they don't collide with
// user-space or global system keys.
//
// However, not all local keys are addressable in the global map. Only range
// local keys incorporating a range key (start key or transaction key) are
// addressable (e.g. range metadata and txn records). Range local keys
// incorporating the Range ID are not (e.g. sequence cache entries, and range
// stats).
//
// TODO(pmattis): Should KeyAddress return an error when the key is malformed?
func Addr(k roachpb.Key) roachpb.RKey {
	if k == nil {
		return nil
	}

	if !bytes.HasPrefix(k, localPrefix) {
		return roachpb.RKey(k)
	}
	if bytes.HasPrefix(k, LocalRangePrefix) {
		k = k[len(LocalRangePrefix):]
		_, k, err := encoding.DecodeBytes(k, nil)
		if err != nil {
			panic(err)
		}
		return roachpb.RKey(k)
	}
	log.Fatalf("local key %q malformed; should contain prefix %q",
		k, LocalRangePrefix)
	return nil
}

// RangeMetaKey returns a range metadata (meta1, meta2) indexing key
// for the given key. For ordinary keys this returns a level 2
// metadata key - for level 2 keys, it returns a level 1 key. For
// level 1 keys and local keys, KeyMin is returned.
func RangeMetaKey(key roachpb.RKey) roachpb.Key {
	if len(key) == 0 {
		return roachpb.KeyMin
	}
	if !bytes.HasPrefix(key, MetaPrefix) {
		return MakeKey(Meta2Prefix, key)
	}
	if bytes.HasPrefix(key, Meta2Prefix) {
		return MakeKey(Meta1Prefix, key[len(Meta2Prefix):])
	}

	return roachpb.KeyMin
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
	var key []byte
	key = append(key, TableDataPrefix...)
	key = encoding.EncodeUvarint(key, uint64(tableID))
	return key
}

// Range returns a key range encompassing all the keys in the Batch.
// TODO(tschottdorf): there is no protection for doubly-local keys here;
// maybe Range should return an error.
func Range(ba roachpb.BatchRequest) roachpb.RSpan {
	from := roachpb.RKeyMax
	to := roachpb.RKeyMin
	for _, arg := range ba.Requests {
		req := arg.GetInner()
		if req.Method() == roachpb.Noop {
			continue
		}
		h := req.Header()
		key := Addr(h.Key)
		if key.Less(from) {
			// Key is smaller than `from`.
			from = key
		}
		if !key.Less(to) {
			// Key.Next() is larger than `to`.
			to = key.Next()
		}
		if endKey := Addr(h.EndKey); to.Less(endKey) {
			// EndKey is larger than `to`.
			to = endKey
		}
	}
	return roachpb.RSpan{Key: from, EndKey: to}
}

type dictEntry struct {
	name   string
	prefix roachpb.Key
	// print the key's pretty value, key has been removed prefix data
	ppFunc func(key roachpb.Key) string
}

var (
	keyDict = []struct {
		name    string
		start   roachpb.Key
		end     roachpb.Key
		entries []dictEntry
	}{
		{name: "/Local", start: localPrefix, end: LocalMax, entries: []dictEntry{
			{name: "/Store", prefix: roachpb.Key(localStorePrefix), ppFunc: localStoreKeyPrint},
			{name: "/RangeID", prefix: roachpb.Key(LocalRangeIDPrefix), ppFunc: localRangeIDKeyPrint},
			{name: "/Range", prefix: LocalRangePrefix, ppFunc: localRangeKeyPrint},
		}},
		{name: "/System", start: SystemPrefix, end: SystemMax, entries: []dictEntry{
			{name: "/Meta2", prefix: Meta2Prefix, ppFunc: print},
			{name: "/Meta1", prefix: Meta1Prefix, ppFunc: print},
			//{name: "Meta", prefix: MetaPrefix},
			{name: "/StatusStore", prefix: StatusStorePrefix, ppFunc: decodeKeyPrint},
			{name: "/StatusNode", prefix: StatusNodePrefix, ppFunc: decodeKeyPrint},
			//{name: "Status", prefix: StatusPrefix},
		}},
		{name: "/Table", start: TableDataPrefix, end: nil, entries: []dictEntry{
			{name: "", prefix: TableDataPrefix, ppFunc: decodeKeyPrint},
		}},
	}

	rangeIDSuffixDict = []struct {
		name   string
		suffix []byte
		ppFunc func(key roachpb.Key) string
	}{
		{name: "ResponseCache", suffix: LocalResponseCacheSuffix, ppFunc: decodeKeyPrint},
		{name: "RaftLeaderLease", suffix: localRaftLeaderLeaseSuffix},
		{name: "RaftTombstone", suffix: localRaftTombstoneSuffix},
		{name: "RaftHardState", suffix: localRaftHardStateSuffix},
		{name: "RaftAppliedIndex", suffix: localRaftAppliedIndexSuffix},
		{name: "RaftLog", suffix: localRaftLogSuffix, ppFunc: raftLogKeyPrint},
		{name: "RaftTruncatedState", suffix: localRaftTruncatedStateSuffix},
		{name: "RaftLastIndex", suffix: localRaftLastIndexSuffix},
		{name: "RangeGCMetadata", suffix: localRangeGCMetadataSuffix},
		{name: "RangeLastVerificationTimestamp", suffix: localRangeLastVerificationTimestampSuffix},
		{name: "RangeStats", suffix: localRangeStatsSuffix},
	}

	rangeSuffixDict = []struct {
		name   string
		suffix []byte
		atEnd  bool
	}{
		{name: "RangeDescriptor", suffix: LocalRangeDescriptorSuffix, atEnd: true},
		{name: "RangeTreeNode", suffix: localRangeTreeNodeSuffix, atEnd: true},
		{name: "Transaction", suffix: localTransactionSuffix, atEnd: false},
	}
)

func localStoreKeyPrint(key roachpb.Key) string {
	if bytes.HasPrefix(key, localStoreIdentSuffix) {
		return "/storeIdent"
	}

	return fmt.Sprintf("%q", []byte(key))
}

func raftLogKeyPrint(key roachpb.Key) string {
	var logIndex uint64
	var err error
	key, logIndex, err = encoding.DecodeUint64(key)
	if err != nil {
		return fmt.Sprintf("/err<%v:%q>", err, []byte(key))
	}

	return fmt.Sprintf("/logIndex:%d", logIndex)
}

func localRangeIDKeyPrint(key roachpb.Key) string {
	var buf bytes.Buffer
	if encoding.PeekType(key) != encoding.Int {
		return fmt.Sprintf("/err<%q>", []byte(key))
	}

	// get range id
	key, i, err := encoding.DecodeVarint(key)
	if err != nil {
		return fmt.Sprintf("/err<%v:%q>", err, []byte(key))
	}

	fmt.Fprintf(&buf, "/%d", i)

	// get suffix

	hasSuffix := false
	for _, s := range rangeIDSuffixDict {
		if bytes.HasPrefix(key, s.suffix) {
			fmt.Fprintf(&buf, "/%s", s.name)
			key = key[len(s.suffix):]
			if s.ppFunc != nil && len(key) != 0 {
				fmt.Fprintf(&buf, "%s", s.ppFunc(key))
				return buf.String()
			}
			hasSuffix = true
			break
		}
	}

	// get encode values
	if hasSuffix {
		fmt.Fprintf(&buf, "%s", decodeKeyPrint(key))
	} else {
		fmt.Fprintf(&buf, "%q", []byte(key))
	}

	return buf.String()
}

func localRangeKeyPrint(key roachpb.Key) string {
	var buf bytes.Buffer

	for _, s := range rangeSuffixDict {
		if s.atEnd {
			if bytes.HasSuffix(key, s.suffix) {
				key = key[:len(key)-len(s.suffix)]
				fmt.Fprintf(&buf, "/%s%s", s.name, decodeKeyPrint(key))
				return buf.String()
			}
		} else {
			begin := bytes.Index(key, s.suffix)
			if begin > 0 {
				addrKey := key[:begin]
				id := key[(begin + len(s.suffix)):]
				fmt.Fprintf(&buf, "/%s/addrKey:%s/id:%q", s.name, decodeKeyPrint(addrKey), []byte(id))
				return buf.String()
			}
		}
	}
	fmt.Fprintf(&buf, "%s", decodeKeyPrint(key))
	return buf.String()
}

func print(key roachpb.Key) string {
	return fmt.Sprintf("/%q", []byte(key))
}

// TableDataPrefix
func decodeKeyPrint(key roachpb.Key) string {
	var buf bytes.Buffer
	for k := 0; len(key) > 0; k++ {
		var err error
		switch encoding.PeekType(key) {
		case encoding.Null:
			key, _ = encoding.DecodeIfNull(key)
			fmt.Fprintf(&buf, "/NULL")
		case encoding.NotNull:
			key, _ = encoding.DecodeIfNotNull(key)
			fmt.Fprintf(&buf, "/#")
		case encoding.Int:
			var i int64
			key, i, err = encoding.DecodeVarint(key)
			if err == nil {
				fmt.Fprintf(&buf, "/%d", i)
			}
		case encoding.Float:
			var f float64
			key, f, err = encoding.DecodeFloat(key, nil)
			if err == nil {
				fmt.Fprintf(&buf, "/%f", f)
			}
		case encoding.Bytes:
			var s string
			key, s, err = encoding.DecodeString(key, nil)
			if err == nil {
				fmt.Fprintf(&buf, "/%q", s)
			}
		case encoding.Time:
			var t time.Time
			key, t, err = encoding.DecodeTime(key)
			if err == nil {
				fmt.Fprintf(&buf, "/%s", t.UTC().Format(time.UnixDate))
			}
		default:
			// This shouldn't ever happen, but if it does let the loop exit.
			fmt.Fprintf(&buf, "/%q", []byte(key))
			key = nil
		}

		if err != nil {
			fmt.Fprintf(&buf, "/<%v>", err)
			continue
		}
	}
	return buf.String()
}

// PrettyPrint print the key with more human readable, which organized as follow
func PrettyPrint(key roachpb.Key) string {
	var buf bytes.Buffer
	for _, k := range keyDict {
		if k.end != nil && (key.Compare(k.start) < 0 || key.Compare(k.end) > 0) {
			continue
		}

		fmt.Fprintf(&buf, "%s", k.name)
		if k.end != nil && k.end.Compare(key) == 0 {
			fmt.Fprintf(&buf, "/Max")
			return buf.String()
		}

		hasPrefix := false
		for _, e := range k.entries {
			if bytes.HasPrefix(key, e.prefix) {
				hasPrefix = true
				key = key[len(e.prefix):]
				fmt.Fprintf(&buf, "%s%s", e.name, e.ppFunc(key))
				break
			}
		}
		if !hasPrefix {
			fmt.Fprintf(&buf, "/%q", []byte(key))
		}

		return buf.String()
	}

	return fmt.Sprintf("%q", []byte(key))
}

func init() {
	roachpb.PrettyPrintKey = PrettyPrint
}
