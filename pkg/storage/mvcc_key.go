// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

var (
	// MVCCKeyMax sorts after all other MVCC keys.
	MVCCKeyMax = MakeMVCCMetadataKey(roachpb.KeyMax)
	// NilKey is the nil MVCCKey.
	NilKey = MVCCKey{}
)

const (
	mvccEncodedTimeSentinelLen  = 1
	mvccEncodedTimeWallLen      = 8
	mvccEncodedTimeLogicalLen   = 4
	mvccEncodedTimeSyntheticLen = 1
	mvccEncodedTimeLengthLen    = 1
)

// MVCCKey is a versioned key, distinguished from roachpb.Key with the addition
// of a "version" timestamp.
//
// The version timestamp dictates the key's visibility to readers. Readers with
// read timestamps equal to or greater than the version timestamp observe the
// key. Readers with read timestamps below the version timestamp ignore the key.
// Keys are stored in decreasing version order, with the exception of version
// zero (timestamp 0), which is referred to as a "meta" version and is stored
// before all other versions of the same key.
type MVCCKey struct {
	Key       roachpb.Key
	Timestamp hlc.Timestamp
}

// MakeMVCCMetadataKey creates an MVCCKey from a roachpb.Key.
func MakeMVCCMetadataKey(key roachpb.Key) MVCCKey {
	return MVCCKey{Key: key}
}

// Next returns the next key.
func (k MVCCKey) Next() MVCCKey {
	ts := k.Timestamp.Prev()
	if ts.IsEmpty() {
		return MVCCKey{
			Key: k.Key.Next(),
		}
	}
	return MVCCKey{
		Key:       k.Key,
		Timestamp: ts,
	}
}

// Clone returns a copy of the key.
func (k MVCCKey) Clone() MVCCKey {
	k.Key = k.Key.Clone()
	return k
}

// Compare returns -1 if this key is less than the given key, 0 if they're
// equal, or 1 if this is greater. Comparison is by key,timestamp, where larger
// timestamps sort before smaller ones except empty ones which sort first (like
// elsewhere in MVCC).
func (k MVCCKey) Compare(o MVCCKey) int {
	if c := k.Key.Compare(o.Key); c != 0 {
		return c
	}
	if k.Timestamp.IsEmpty() && !o.Timestamp.IsEmpty() {
		return -1
	} else if !k.Timestamp.IsEmpty() && o.Timestamp.IsEmpty() {
		return 1
	} else {
		return -k.Timestamp.Compare(o.Timestamp) // timestamps sort in reverse
	}
}

// Less compares two keys.
func (k MVCCKey) Less(l MVCCKey) bool {
	if c := k.Key.Compare(l.Key); c != 0 {
		return c < 0
	}
	if !k.IsValue() {
		return l.IsValue()
	} else if !l.IsValue() {
		return false
	}
	return l.Timestamp.Less(k.Timestamp)
}

// Equal returns whether two keys are identical.
func (k MVCCKey) Equal(l MVCCKey) bool {
	return k.Key.Compare(l.Key) == 0 && k.Timestamp.EqOrdering(l.Timestamp)
}

// IsValue returns true iff the timestamp is non-zero.
func (k MVCCKey) IsValue() bool {
	return !k.Timestamp.IsEmpty()
}

// EncodedSize returns the size of the MVCCKey when encoded.
//
// TODO(itsbilal): Reconcile this with Len(). Would require updating MVCC stats
// tests to reflect the more accurate lengths provided by Len().
func (k MVCCKey) EncodedSize() int {
	n := len(k.Key) + 1
	if k.IsValue() {
		// Note that this isn't quite accurate: timestamps consume between 8-13
		// bytes. Fixing this only adjusts the accounting for timestamps, not the
		// actual on disk storage.
		n += int(MVCCVersionTimestampSize)
	}
	return n
}

// String returns a string-formatted version of the key.
func (k MVCCKey) String() string {
	if !k.IsValue() {
		return k.Key.String()
	}
	return fmt.Sprintf("%s/%s", k.Key, k.Timestamp)
}

// Format implements the fmt.Formatter interface.
func (k MVCCKey) Format(f fmt.State, c rune) {
	fmt.Fprintf(f, "%s/%s", k.Key, k.Timestamp)
}

// Len returns the size of the MVCCKey when encoded. Implements the
// pebble.Encodeable interface.
func (k MVCCKey) Len() int {
	return encodedMVCCKeyLength(k)
}

// EncodeMVCCKey encodes an MVCCKey into its Pebble representation. The encoding
// takes the following forms, where trailing time components are omitted when
// zero-valued:
//
// [key] [sentinel] [timeWall] [timeLogical] [timeSynthetic] [timeLength]
// [key] [sentinel] [timeWall] [timeLogical] [timeLength]
// [key] [sentinel] [timeWall] [timeLength]
// [key] [sentinel]
//
// key:           the unmodified binary key            (variable length)
// sentinel:      separates key and timestamp          (1 byte: 0x00)
// timeWall:      Timestamp.WallTime                   (8 bytes: big-endian uint64)
// timeLogical:   Timestamp.Logical                    (4 bytes: big-endian uint32)
// timeSynthetic: Timestamp.Synthetic                  (1 byte: 0x01 when set)
// timeLength:    encoded timestamp length inc. itself (1 byte: uint8)
//
// The sentinel byte can be used to detect a key without a timestamp, since
// timeLength will never be 0 (it includes itself in the length).
func EncodeMVCCKey(key MVCCKey) []byte {
	keyLen := encodedMVCCKeyLength(key)
	buf := make([]byte, keyLen)
	encodeMVCCKeyToBuf(buf, key, keyLen)
	return buf
}

// EncodeMVCCKeyToBuf encodes an MVCCKey into its Pebble representation, reusing
// the given byte buffer if it has sufficient capacity.
func EncodeMVCCKeyToBuf(buf []byte, key MVCCKey) []byte {
	keyLen := encodedMVCCKeyLength(key)
	if cap(buf) < keyLen {
		buf = make([]byte, keyLen)
	} else {
		buf = buf[:keyLen]
	}
	encodeMVCCKeyToBuf(buf, key, keyLen)
	return buf
}

// EncodeMVCCKeyPrefix encodes an MVCC user key (without timestamp) into its
// Pebble prefix representation.
func EncodeMVCCKeyPrefix(key roachpb.Key) []byte {
	return EncodeMVCCKey(MVCCKey{Key: key})
}

// encodeMVCCKeyToBuf encodes an MVCCKey into its Pebble representation to the
// target buffer, which must have the correct size.
func encodeMVCCKeyToBuf(buf []byte, key MVCCKey, keyLen int) {
	copy(buf, key.Key)
	pos := len(key.Key)

	buf[pos] = 0 // sentinel byte
	pos += mvccEncodedTimeSentinelLen

	tsLen := keyLen - pos - mvccEncodedTimeLengthLen
	if tsLen > 0 {
		encodeMVCCTimestampToBuf(buf[pos:], key.Timestamp)
		pos += tsLen
		buf[pos] = byte(tsLen + mvccEncodedTimeLengthLen)
	}
}

// encodeMVCCTimestamp encodes an MVCC timestamp into its Pebble
// representation, excluding length suffix and sentinel byte.
func encodeMVCCTimestamp(ts hlc.Timestamp) []byte {
	tsLen := encodedMVCCTimestampLength(ts)
	if tsLen == 0 {
		return nil
	}
	buf := make([]byte, tsLen)
	encodeMVCCTimestampToBuf(buf, ts)
	return buf
}

// EncodeMVCCTimestampSuffix encodes an MVCC timestamp into its Pebble
// representation, including the length suffix but excluding the sentinel byte.
// This is equivalent to the Pebble suffix.
func EncodeMVCCTimestampSuffix(ts hlc.Timestamp) []byte {
	return encodeMVCCTimestampSuffixToBuf(nil, ts)
}

// encodeMVCCTimestampSuffixToBuf encodes an MVCC timestamp into its Pebble
// representation, including the length suffix but excluding the sentinel byte.
// This is equivalent to the Pebble suffix. It reuses the given byte buffer if
// it has sufficient capacity.
func encodeMVCCTimestampSuffixToBuf(buf []byte, ts hlc.Timestamp) []byte {
	tsLen := encodedMVCCTimestampLength(ts)
	if tsLen == 0 {
		return buf[:0]
	}
	suffixLen := tsLen + mvccEncodedTimeLengthLen
	if cap(buf) < suffixLen {
		buf = make([]byte, suffixLen)
	} else {
		buf = buf[:suffixLen]
	}
	encodeMVCCTimestampToBuf(buf, ts)
	buf[tsLen] = byte(suffixLen)
	return buf
}

// encodeMVCCTimestampToBuf encodes an MVCC timestamp into its Pebble
// representation, excluding the length suffix and sentinel byte. The target
// buffer must have the correct size, and the timestamp must not be empty.
func encodeMVCCTimestampToBuf(buf []byte, ts hlc.Timestamp) {
	binary.BigEndian.PutUint64(buf, uint64(ts.WallTime))
	if ts.Logical != 0 || ts.Synthetic {
		binary.BigEndian.PutUint32(buf[mvccEncodedTimeWallLen:], uint32(ts.Logical))
		if ts.Synthetic {
			buf[mvccEncodedTimeWallLen+mvccEncodedTimeLogicalLen] = 1
		}
	}
}

// encodedMVCCKeyLength returns the encoded length of the given MVCCKey.
func encodedMVCCKeyLength(key MVCCKey) int {
	// NB: We don't call into EncodedMVCCKeyPrefixLength() or
	// EncodedMVCCTimestampSuffixLength() here because the additional function
	// call overhead is significant.
	keyLen := len(key.Key) + mvccEncodedTimeSentinelLen
	if !key.Timestamp.IsEmpty() {
		keyLen += mvccEncodedTimeWallLen + mvccEncodedTimeLengthLen
		if key.Timestamp.Logical != 0 || key.Timestamp.Synthetic {
			keyLen += mvccEncodedTimeLogicalLen
			if key.Timestamp.Synthetic {
				// TODO(nvanbenschoten): stop writing Synthetic timestamps in v23.1.
				keyLen += mvccEncodedTimeSyntheticLen
			}
		}
	}
	return keyLen
}

// EncodedMVCCKeyPrefixLength returns the encoded length of a roachpb.Key prefix
// including the sentinel byte.
func EncodedMVCCKeyPrefixLength(key roachpb.Key) int {
	return len(key) + mvccEncodedTimeSentinelLen
}

// encodedMVCCTimestampLength returns the encoded length of the given MVCC
// timestamp, excluding the length suffix and sentinel bytes.
func encodedMVCCTimestampLength(ts hlc.Timestamp) int {
	// This is backwards, but encodedMVCCKeyLength() is called in the
	// EncodeMVCCKey() hot path and an additional function call to this function
	// shows ~6% overhead in benchmarks. We therefore do the timestamp length
	// calculation inline in encodedMVCCKeyLength(), and remove the excess here.
	tsLen := encodedMVCCKeyLength(MVCCKey{Timestamp: ts}) - mvccEncodedTimeSentinelLen
	if tsLen > 0 {
		tsLen -= mvccEncodedTimeLengthLen
	}
	return tsLen
}

// EncodedMVCCTimestampSuffixLength returns the encoded length of the
// given MVCC timestamp, including the length suffix. It returns 0
// if the timestamp is empty.
func EncodedMVCCTimestampSuffixLength(ts hlc.Timestamp) int {
	// This is backwards, see comment in encodedMVCCTimestampLength() for why.
	return encodedMVCCKeyLength(MVCCKey{Timestamp: ts}) - mvccEncodedTimeSentinelLen
}

// TODO(erikgrinaker): merge in the enginepb decoding functions once it can
// avoid the storage package's problematic CGo dependency (via Pebble).

// DecodeMVCCKey decodes an MVCCKey from its Pebble representation.
func DecodeMVCCKey(encodedKey []byte) (MVCCKey, error) {
	k, ts, err := enginepb.DecodeKey(encodedKey)
	return MVCCKey{k, ts}, err
}

// decodeMVCCTimestamp decodes an MVCC timestamp from its Pebble representation,
// excluding the length suffix.
func decodeMVCCTimestamp(encodedTS []byte) (hlc.Timestamp, error) {
	// NB: This logic is duplicated in enginepb.DecodeKey() to avoid the
	// overhead of an additional function call there (~13%).
	var ts hlc.Timestamp
	switch len(encodedTS) {
	case 0:
		// No-op.
	case 8:
		ts.WallTime = int64(binary.BigEndian.Uint64(encodedTS[0:8]))
	case 12:
		ts.WallTime = int64(binary.BigEndian.Uint64(encodedTS[0:8]))
		ts.Logical = int32(binary.BigEndian.Uint32(encodedTS[8:12]))
	case 13:
		ts.WallTime = int64(binary.BigEndian.Uint64(encodedTS[0:8]))
		ts.Logical = int32(binary.BigEndian.Uint32(encodedTS[8:12]))
		// TODO(nvanbenschoten): stop writing Synthetic timestamps in v23.1.
		ts.Synthetic = encodedTS[12] != 0
	default:
		return hlc.Timestamp{}, errors.Errorf("bad timestamp %x", encodedTS)
	}
	return ts, nil
}

// DecodeMVCCTimestampSuffix decodes an MVCC timestamp from its Pebble representation,
// including the length suffix.
func DecodeMVCCTimestampSuffix(encodedTS []byte) (hlc.Timestamp, error) {
	if len(encodedTS) == 0 {
		return hlc.Timestamp{}, nil
	}
	encodedLen := len(encodedTS)
	if suffixLen := int(encodedTS[encodedLen-1]); suffixLen != encodedLen {
		return hlc.Timestamp{}, errors.Errorf(
			"bad timestamp: found length suffix %d, actual length %d", suffixLen, encodedLen)
	}
	return decodeMVCCTimestamp(encodedTS[:encodedLen-1])
}

// MVCCRangeKey is a versioned key span.
type MVCCRangeKey struct {
	StartKey  roachpb.Key
	EndKey    roachpb.Key
	Timestamp hlc.Timestamp
}

// Bounds returns the range key bounds as a Span.
func (k MVCCRangeKey) Bounds() roachpb.Span {
	return roachpb.Span{Key: k.StartKey, EndKey: k.EndKey}
}

// Clone returns a copy of the range key.
func (k MVCCRangeKey) Clone() MVCCRangeKey {
	// k is already a copy, but byte slices must be cloned.
	k.StartKey = k.StartKey.Clone()
	k.EndKey = k.EndKey.Clone()
	return k
}

// Compare returns -1 if this key is less than the given key, 0 if they're
// equal, or 1 if this is greater. Comparison is by start,timestamp,end, where
// larger timestamps sort before smaller ones except empty ones which sort first
// (like elsewhere in MVCC).
func (k MVCCRangeKey) Compare(o MVCCRangeKey) int {
	if c := k.StartKey.Compare(o.StartKey); c != 0 {
		return c
	}
	if k.Timestamp.IsEmpty() && !o.Timestamp.IsEmpty() {
		return -1
	} else if !k.Timestamp.IsEmpty() && o.Timestamp.IsEmpty() {
		return 1
	} else if c := k.Timestamp.Compare(o.Timestamp); c != 0 {
		return -c // timestamps sort in reverse
	}
	return k.EndKey.Compare(o.EndKey)
}

// EncodedSize returns the encoded size of this range key. This does not
// accurately reflect the on-disk size of the key, due to Pebble range key
// stacking and fragmentation.
//
// NB: This calculation differs from MVCCKey in that MVCCKey.EncodedSize()
// incorrectly always uses 13 bytes for the timestamp while this method
// calculates the actual encoded size.
func (k MVCCRangeKey) EncodedSize() int {
	return EncodedMVCCKeyPrefixLength(k.StartKey) +
		EncodedMVCCKeyPrefixLength(k.EndKey) +
		EncodedMVCCTimestampSuffixLength(k.Timestamp)
}

// String formats the range key.
func (k MVCCRangeKey) String() string {
	s := roachpb.Span{Key: k.StartKey, EndKey: k.EndKey}.String()
	if !k.Timestamp.IsEmpty() {
		s += fmt.Sprintf("/%s", k.Timestamp)
	}
	return s
}

// Validate returns an error if the range key is invalid.
//
// This validation is for writing range keys (or checking existing range keys),
// not for filters/bounds, so e.g. specifying an empty start key is invalid even
// though it would be valid to start a range key scan at an empty start key.
func (k MVCCRangeKey) Validate() (err error) {
	defer func() {
		err = errors.Wrapf(err, "invalid range key %s", k)
	}()

	switch {
	case len(k.StartKey) == 0:
		// We don't allow an empty start key, because we don't allow writing point
		// keys at the empty key. The first valid key is 0x00.
		return errors.Errorf("no start key")
	case len(k.EndKey) == 0:
		return errors.Errorf("no end key")
	case k.Timestamp.IsEmpty():
		return errors.Errorf("no timestamp")
	case k.StartKey.Compare(k.EndKey) >= 0:
		return errors.Errorf("start key %s is at or after end key %s", k.StartKey, k.EndKey)
	default:
		return nil
	}
}

// MVCCRangeKeyStack represents a stack of range key fragments as returned
// by SimpleMVCCIterator.RangeKeys(). All fragments have the same key bounds,
// and are ordered from newest to oldest.
type MVCCRangeKeyStack struct {
	Bounds   roachpb.Span
	Versions MVCCRangeKeyVersions
}

// MVCCRangeKeyVersions represents a stack of range key fragment versions.
type MVCCRangeKeyVersions []MVCCRangeKeyVersion

// MVCCRangeKeyVersion represents a single range key fragment version.
type MVCCRangeKeyVersion struct {
	Timestamp hlc.Timestamp
	Value     []byte
}

// AsRangeKey returns an MVCCRangeKey for the given version. Byte slices
// are shared with the stack.
func (s MVCCRangeKeyStack) AsRangeKey(v MVCCRangeKeyVersion) MVCCRangeKey {
	return MVCCRangeKey{
		StartKey:  s.Bounds.Key,
		EndKey:    s.Bounds.EndKey,
		Timestamp: v.Timestamp,
	}
}

// AsRangeKeys converts the stack into a slice of MVCCRangeKey. Byte slices
// are shared with the stack.
func (s MVCCRangeKeyStack) AsRangeKeys() []MVCCRangeKey {
	rangeKeys := make([]MVCCRangeKey, 0, len(s.Versions))
	for _, v := range s.Versions {
		rangeKeys = append(rangeKeys, s.AsRangeKey(v))
	}
	return rangeKeys
}

// AsRangeKeyValue returns an MVCCRangeKeyValue for the given version. Byte
// slices are shared with the stack.
func (s MVCCRangeKeyStack) AsRangeKeyValue(v MVCCRangeKeyVersion) MVCCRangeKeyValue {
	return MVCCRangeKeyValue{
		RangeKey: s.AsRangeKey(v),
		Value:    v.Value,
	}
}

// AsRangeKeyValues converts the stack into a slice of MVCCRangeKeyValue. Byte
// slices are shared with the stack.
func (s MVCCRangeKeyStack) AsRangeKeyValues() []MVCCRangeKeyValue {
	kvs := make([]MVCCRangeKeyValue, 0, len(s.Versions))
	for _, v := range s.Versions {
		kvs = append(kvs, s.AsRangeKeyValue(v))
	}
	return kvs
}

// CanMergeRight returns true if the current stack will merge with the given
// right-hand stack. The key bounds must touch exactly, i.e. lhs.EndKey must
// equal rhs.Key.
func (s MVCCRangeKeyStack) CanMergeRight(r MVCCRangeKeyStack) bool {
	if s.IsEmpty() || s.Len() != r.Len() || !s.Bounds.EndKey.Equal(r.Bounds.Key) {
		return false
	}
	for i := range s.Versions {
		if !s.Versions[i].Equal(r.Versions[i]) {
			return false
		}
	}
	return true
}

// Clone clones the stack.
func (s MVCCRangeKeyStack) Clone() MVCCRangeKeyStack {
	// TODO(erikgrinaker): We can optimize this by using a single memory
	// allocation for all byte slices in the entire stack.
	s.Bounds = s.Bounds.Clone()
	s.Versions = s.Versions.Clone()
	return s
}

// Covers returns true if any range key in the stack covers the given point key.
func (s MVCCRangeKeyStack) Covers(k MVCCKey) bool {
	return s.Versions.Covers(k.Timestamp) && s.Bounds.ContainsKey(k.Key)
}

// CoversTimestamp returns true if any range key in the stack covers the given timestamp.
func (s MVCCRangeKeyStack) CoversTimestamp(ts hlc.Timestamp) bool {
	return s.Versions.Covers(ts)
}

// FirstAbove does a binary search for the first range key version at or above
// the given timestamp. Returns false if no matching range key was found.
func (s MVCCRangeKeyStack) FirstAbove(ts hlc.Timestamp) (MVCCRangeKeyVersion, bool) {
	return s.Versions.FirstAbove(ts)
}

// FirstBelow does a binary search for the first range key version at or below
// the given timestamp. Returns false if no matching range key was found.
func (s MVCCRangeKeyStack) FirstBelow(ts hlc.Timestamp) (MVCCRangeKeyVersion, bool) {
	return s.Versions.FirstBelow(ts)
}

// HasBetween checks whether an MVCC range key exists between the two given
// timestamps (both inclusive, in order).
func (s MVCCRangeKeyStack) HasBetween(lower, upper hlc.Timestamp) bool {
	return s.Versions.HasBetween(lower, upper)
}

// IsEmpty returns true if the stack is empty (no versions).
func (s MVCCRangeKeyStack) IsEmpty() bool {
	return s.Versions.IsEmpty()
}

// Len returns the number of versions in the stack.
func (s MVCCRangeKeyStack) Len() int {
	return len(s.Versions)
}

// Timestamps returns the timestamps of all versions.
func (s MVCCRangeKeyStack) Timestamps() []hlc.Timestamp {
	return s.Versions.Timestamps()
}

// Trim trims the versions to the time span [from, to] (both inclusive).
func (s MVCCRangeKeyStack) Trim(from, to hlc.Timestamp) MVCCRangeKeyStack {
	s.Versions = s.Versions.Trim(from, to)
	return s
}

// Clone clones the versions.
func (v MVCCRangeKeyVersions) Clone() MVCCRangeKeyVersions {
	c := make(MVCCRangeKeyVersions, len(v))
	for i, version := range v {
		c[i] = version.Clone()
	}
	return c
}

// Covers returns true if any version in the stack is above the given timestamp.
func (v MVCCRangeKeyVersions) Covers(ts hlc.Timestamp) bool {
	return !v.IsEmpty() && ts.LessEq(v[0].Timestamp)
}

// FirstAbove does a binary search for the first range key version at or above
// the given timestamp. Returns false if no matching range key was found.
func (v MVCCRangeKeyVersions) FirstAbove(ts hlc.Timestamp) (MVCCRangeKeyVersion, bool) {
	// This is kind of odd due to sort.Search() semantics: we do a binary search
	// for the first range key that's below the timestamp, then return the
	// previous range key if any.
	if i := sort.Search(len(v), func(i int) bool {
		return v[i].Timestamp.Less(ts)
	}); i > 0 {
		return v[i-1], true
	}
	return MVCCRangeKeyVersion{}, false
}

// FirstBelow does a binary search for the first range key version at or below
// the given timestamp. Returns false if no matching range key was found.
func (v MVCCRangeKeyVersions) FirstBelow(ts hlc.Timestamp) (MVCCRangeKeyVersion, bool) {
	if i := sort.Search(len(v), func(i int) bool {
		return v[i].Timestamp.LessEq(ts)
	}); i < len(v) {
		return v[i], true
	}
	return MVCCRangeKeyVersion{}, false
}

// HasBetween checks whether an MVCC range key exists between the two given
// timestamps (both inclusive, in order).
func (v MVCCRangeKeyVersions) HasBetween(lower, upper hlc.Timestamp) bool {
	if version, ok := v.FirstAbove(lower); ok {
		// Consider equal timestamps to be "between". This shouldn't really happen,
		// since MVCC enforces point and range keys can't have the same timestamp.
		return version.Timestamp.LessEq(upper)
	}
	return false
}

// IsEmpty returns true if the stack is empty (no versions).
func (v MVCCRangeKeyVersions) IsEmpty() bool {
	return len(v) == 0
}

// Timestamps returns the timestamps of all versions.
func (v MVCCRangeKeyVersions) Timestamps() []hlc.Timestamp {
	timestamps := make([]hlc.Timestamp, 0, len(v))
	for _, version := range v {
		timestamps = append(timestamps, version.Timestamp)
	}
	return timestamps
}

// Trim trims the versions to the time span [from, to] (both inclusive).
func (v MVCCRangeKeyVersions) Trim(from, to hlc.Timestamp) MVCCRangeKeyVersions {
	// We assume that to will often be near the current time, and use a linear
	// rather than a binary search, which will often match on the first range key.
	start := len(v)
	for i, version := range v {
		if version.Timestamp.LessEq(to) {
			start = i
			break
		}
	}
	v = v[start:]

	// We then use a binary search to find the lower bound.
	end := sort.Search(len(v), func(i int) bool {
		return v[i].Timestamp.Less(from)
	})
	return v[:end]
}

// Clone clones the version.
func (v MVCCRangeKeyVersion) Clone() MVCCRangeKeyVersion {
	if v.Value != nil {
		v.Value = append([]byte(nil), v.Value...)
	}
	return v
}

// Equal returns true if the two versions are equal.
func (v MVCCRangeKeyVersion) Equal(o MVCCRangeKeyVersion) bool {
	return v.Timestamp.Equal(o.Timestamp) && bytes.Equal(v.Value, o.Value)
}

// FirstRangeKeyAbove does a binary search for the first range key at or above
// the given timestamp. It assumes the range keys are ordered in descending
// timestamp order, as returned by SimpleMVCCIterator.RangeKeys(). Returns false
// if no matching range key was found.
//
// TODO(erikgrinaker): Consider using a new type for []MVCCRangeKeyValue as
// returned by SimpleMVCCIterator.RangeKeys(), and add this as a method.
func FirstRangeKeyAbove(rangeKeys []MVCCRangeKeyValue, ts hlc.Timestamp) (MVCCRangeKeyValue, bool) {
	// This is kind of odd due to sort.Search() semantics: we do a binary search
	// for the first range tombstone that's below the timestamp, then return the
	// previous range tombstone if any.
	if i := sort.Search(len(rangeKeys), func(i int) bool {
		return rangeKeys[i].RangeKey.Timestamp.Less(ts)
	}); i > 0 {
		return rangeKeys[i-1], true
	}
	return MVCCRangeKeyValue{}, false
}

// HasRangeKeyBetween checks whether an MVCC range key exists between the two
// given timestamps (in order). It assumes the range keys are ordered in
// descending timestamp order, as returned by SimpleMVCCIterator.RangeKeys().
func HasRangeKeyBetween(rangeKeys []MVCCRangeKeyValue, upper, lower hlc.Timestamp) bool {
	if len(rangeKeys) == 0 {
		return false
	}
	if util.RaceEnabled && upper.Less(lower) {
		panic(errors.AssertionFailedf("HasRangeKeyBetween given upper %s <= lower %s", upper, lower))
	}
	if rkv, ok := FirstRangeKeyAbove(rangeKeys, lower); ok {
		// Consider equal timestamps to be "between". This shouldn't really happen,
		// since MVCC enforces point and range keys can't have the same timestamp.
		return rkv.RangeKey.Timestamp.LessEq(upper)
	}
	return false
}
