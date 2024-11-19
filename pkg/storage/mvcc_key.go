// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"sort"
	"strings"

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

// CloneInto copies the key into the provided destination MVCCKey, reusing and
// overwriting its key slice.
func (k MVCCKey) CloneInto(dst *MVCCKey) {
	dst.Key = append(dst.Key[:0], k.Key...)
	dst.Timestamp = k.Timestamp
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
	return k.Key.Compare(l.Key) == 0 && k.Timestamp == l.Timestamp
}

// IsValue returns true iff the timestamp is non-zero.
func (k MVCCKey) IsValue() bool {
	return !k.Timestamp.IsEmpty()
}

// EncodedSize returns the size of the MVCCKey when encoded.
//
// TODO(itsbilal): Reconcile this with Len(). Would require updating MVCC stats
// tests to reflect the more accurate lengths provided by Len().
// TODO(nvanbenschoten): Change the return value to an int64. That's what every
// caller wants.
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
//
// The timeSynthetic form is no longer written by the current version of the
// code, but can be encountered in the wild until we migrate it away. Until
// then, decoding routines must be prepared to handle it, but can ignore the
// synthetic bit.
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

// EncodeMVCCTimestampToBuf encodes an MVCC timestamp into its Pebble
// representation, excluding the length suffix and sentinel byte, reusing the
// given byte slice if it has sufficient capacity.
func EncodeMVCCTimestampToBuf(buf []byte, ts hlc.Timestamp) []byte {
	tsLen := encodedMVCCTimestampLength(ts)
	if tsLen == 0 {
		return buf[:0]
	}
	if cap(buf) < tsLen {
		buf = make([]byte, tsLen)
	} else {
		buf = buf[:tsLen]
	}
	encodeMVCCTimestampToBuf(buf, ts)
	return buf
}

// encodeMVCCTimestampToBuf encodes an MVCC timestamp into its Pebble
// representation, excluding the length suffix and sentinel byte. The target
// buffer must have the correct size, and the timestamp must not be empty.
func encodeMVCCTimestampToBuf(buf []byte, ts hlc.Timestamp) {
	binary.BigEndian.PutUint64(buf, uint64(ts.WallTime))
	if ts.Logical != 0 {
		binary.BigEndian.PutUint32(buf[mvccEncodedTimeWallLen:], uint32(ts.Logical))
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
		if key.Timestamp.Logical != 0 {
			keyLen += mvccEncodedTimeLogicalLen
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
	case 12, 13:
		ts.WallTime = int64(binary.BigEndian.Uint64(encodedTS[0:8]))
		ts.Logical = int32(binary.BigEndian.Uint32(encodedTS[8:12]))
		// NOTE: byte 13 used to store the timestamp's synthetic bit, but this is no
		// longer consulted and can be ignored during decoding.
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
	// EncodedTimestampSuffix is an optional encoded representation of Timestamp
	// as a Pebble "suffix". When reading range keys from the engine, the
	// iterator copies the verbatim encoded timestamp here. There historically
	// have been multiple representations of a timestamp that were intended to
	// be logically equivalent. A bug in CockroachDB's pebble.Comparer
	// implementation prevented some encodings from being considered equivalent.
	// See #129592.
	//
	// To work around this wart within the comparer, we preserve a copy of the
	// physical encoded timestamp we read off the engine. If a MVCCRangeKey with
	// a non-empty EncodedTimestampSuffix is cleared via ClearMVCCRangeKey, the
	// RangeKeyUnset tombstone is written with the verbatim
	// EncodedTimestampSuffix.
	EncodedTimestampSuffix []byte
}

// AsStack returns the range key as a range key stack with the given value.
func (k MVCCRangeKey) AsStack(valueRaw []byte) MVCCRangeKeyStack {
	return MVCCRangeKeyStack{
		Bounds: k.Bounds(),
		Versions: MVCCRangeKeyVersions{{
			Timestamp:              k.Timestamp,
			Value:                  valueRaw,
			EncodedTimestampSuffix: k.EncodedTimestampSuffix,
		}},
	}
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
	k.EncodedTimestampSuffix = slices.Clone(k.EncodedTimestampSuffix)
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

// Includes returns if this MVCCRangeKey's bounds include the specified key.
func (k MVCCRangeKey) Includes(key roachpb.Key) bool {
	return k.StartKey.Compare(key) <= 0 && k.EndKey.Compare(key) > 0
}

// Overlaps returns true if this MVCCRangeKey overlaps with the specified one.
func (k MVCCRangeKey) Overlaps(b MVCCRangeKey) bool {
	return k.StartKey.Compare(b.EndKey) < 0 && k.EndKey.Compare(b.StartKey) > 0
}

// Deletes returns whether this MVCCRangeKey deletes the specified MVCC key.
func (k MVCCRangeKey) Deletes(key MVCCKey) bool {
	return k.Includes(key.Key) && !k.Timestamp.Less(key.Timestamp)
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
	// EncodedTimestampSuffix is an optional encoded representation of Timestamp
	// as a Pebble "suffix". When reading range keys from the engine, the
	// iterator copies the verbatim encoded timestamp here. There historically
	// have been multiple representations of a timestamp that were intended to
	// be logically equivalent. A bug in CockroachDB's pebble.Comparer
	// implementation prevented some encodings from being considered equivalent.
	// See #129592.
	//
	// To work around this wart within the comparer, we preserve a copy of the
	// physical encoded timestamp we read off the engine. If a MVCCRangeKey with
	// a non-empty EncodedTimestampSuffix is cleared via ClearMVCCRangeKey, the
	// RangeKeyUnset tombstone is written with the verbatim
	// EncodedTimestampSuffix.
	EncodedTimestampSuffix []byte
}

// CloneInto copies the version into the provided destination
// MVCCRangeKeyVersion, reusing and overwriting its value slice.
func (v MVCCRangeKeyVersion) CloneInto(dst *MVCCRangeKeyVersion) {
	dst.Timestamp = v.Timestamp
	dst.Value = append(dst.Value[:0], v.Value...)
	dst.EncodedTimestampSuffix = append(dst.EncodedTimestampSuffix[:0], v.EncodedTimestampSuffix...)
}

// AsRangeKey returns an MVCCRangeKey for the given version. Byte slices
// are shared with the stack.
func (s MVCCRangeKeyStack) AsRangeKey(v MVCCRangeKeyVersion) MVCCRangeKey {
	return MVCCRangeKey{
		StartKey:               s.Bounds.Key,
		EndKey:                 s.Bounds.EndKey,
		Timestamp:              v.Timestamp,
		EncodedTimestampSuffix: v.EncodedTimestampSuffix,
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
// right-hand stack. The key bounds must touch exactly, i.e. the left-hand
// EndKey must equal the right-hand Key.
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

// Clear clears the stack but retains the byte slices. It is useful to
// empty out a stack being used as a CloneInto() target.
func (s *MVCCRangeKeyStack) Clear() {
	s.Bounds.Key = s.Bounds.Key[:0]
	s.Bounds.EndKey = s.Bounds.EndKey[:0]
	s.Versions.Clear()
}

// Clone clones the stack.
func (s MVCCRangeKeyStack) Clone() MVCCRangeKeyStack {
	s.Bounds = s.Bounds.Clone()
	s.Versions = s.Versions.Clone()
	return s
}

// CloneInto clones the stack into the given stack reference, reusing its byte
// and version slices where possible.
//
// TODO(erikgrinaker): Consider using a single allocation for all byte slices.
// However, we currently expect the majority of range keys to have to have no
// value, so we'll typically only make two allocations for the key bounds.
func (s MVCCRangeKeyStack) CloneInto(c *MVCCRangeKeyStack) {
	c.Bounds.Key = append(c.Bounds.Key[:0], s.Bounds.Key...)
	c.Bounds.EndKey = append(c.Bounds.EndKey[:0], s.Bounds.EndKey...)
	s.Versions.CloneInto(&c.Versions)
}

// Covers returns true if any range key in the stack covers the given point key.
// A timestamp of 0 (i.e. an intent) is considered to be above all timestamps,
// and thus not covered by any range key.
func (s MVCCRangeKeyStack) Covers(k MVCCKey) bool {
	return s.Versions.Covers(k.Timestamp) && s.Bounds.ContainsKey(k.Key)
}

// CoversTimestamp returns true if any range key in the stack covers the given timestamp.
func (s MVCCRangeKeyStack) CoversTimestamp(ts hlc.Timestamp) bool {
	return s.Versions.Covers(ts)
}

// Equal returns true if the range key stacks are equal.
func (s MVCCRangeKeyStack) Equal(o MVCCRangeKeyStack) bool {
	return s.Bounds.Equal(o.Bounds) && s.Versions.Equal(o.Versions)
}

// Excise removes the versions in the given [from, to] span (inclusive, in
// order) in place, returning true if any versions were removed.
func (s *MVCCRangeKeyStack) Excise(from, to hlc.Timestamp) bool {
	return s.Versions.Excise(from, to)
}

// FirstAtOrAbove does a binary search for the first range key version at or
// above the given timestamp. Returns false if no matching range key was found.
func (s MVCCRangeKeyStack) FirstAtOrAbove(ts hlc.Timestamp) (MVCCRangeKeyVersion, bool) {
	return s.Versions.FirstAtOrAbove(ts)
}

// FirstAtOrBelow does a binary search for the first range key version at or
// below the given timestamp. Returns false if no matching range key was found.
func (s MVCCRangeKeyStack) FirstAtOrBelow(ts hlc.Timestamp) (MVCCRangeKeyVersion, bool) {
	return s.Versions.FirstAtOrBelow(ts)
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

// Newest returns the timestamp of the newest range key in the stack.
func (s MVCCRangeKeyStack) Newest() hlc.Timestamp {
	return s.Versions.Newest()
}

// Oldest returns the timestamp of the oldest range key in the stack.
func (s MVCCRangeKeyStack) Oldest() hlc.Timestamp {
	return s.Versions.Oldest()
}

// Remove removes the given version from the stack, returning true if it was
// found.
func (s *MVCCRangeKeyStack) Remove(ts hlc.Timestamp) (MVCCRangeKeyVersion, bool) {
	return s.Versions.Remove(ts)
}

// String formats the MVCCRangeKeyStack as a string.
func (s MVCCRangeKeyStack) String() string {
	return fmt.Sprintf("%s%s", s.Bounds, s.Versions)
}

// Timestamps returns the timestamps of all versions.
func (s MVCCRangeKeyStack) Timestamps() []hlc.Timestamp {
	return s.Versions.Timestamps()
}

// Trim trims the versions to the time span [from, to] (both inclusive in order)
// in place. Returns true if any versions were removed.
func (s *MVCCRangeKeyStack) Trim(from, to hlc.Timestamp) bool {
	return s.Versions.Trim(from, to)
}

// Clear clears out the version stack, but retains any byte slices.
func (v *MVCCRangeKeyVersions) Clear() {
	*v = (*v)[:0]
}

// Clone clones the versions.
func (v MVCCRangeKeyVersions) Clone() MVCCRangeKeyVersions {
	c := make(MVCCRangeKeyVersions, len(v))
	for i, version := range v {
		c[i] = version.Clone()
	}
	return c
}

// CloneInto clones the versions, reusing the byte slices and backing array of
// the given slice.
func (v MVCCRangeKeyVersions) CloneInto(c *MVCCRangeKeyVersions) {
	if length, capacity := len(v), cap(*c); length > capacity {
		// Extend the slice, keeping the existing versions to reuse their Value byte
		// slices. The compiler optimizes away the intermediate, appended slice.
		*c = append((*c)[:capacity], make(MVCCRangeKeyVersions, length-capacity)...)
	} else {
		*c = (*c)[:length]
	}
	for i := range v {
		(*c)[i].Timestamp = v[i].Timestamp
		(*c)[i].Value = append((*c)[i].Value[:0], v[i].Value...)
		(*c)[i].EncodedTimestampSuffix = append((*c)[i].EncodedTimestampSuffix[:0], v[i].EncodedTimestampSuffix...)
	}
}

// Covers returns true if any version in the stack is above the given timestamp.
// A timestamp of 0 (i.e. an intent) is considered to be above all timestamps,
// and thus not covered by any range key.
func (v MVCCRangeKeyVersions) Covers(ts hlc.Timestamp) bool {
	return !v.IsEmpty() && !ts.IsEmpty() && ts.LessEq(v[0].Timestamp)
}

// Equal returns whether versions in the specified MVCCRangeKeyVersions match
// exactly (in timestamps and values) with those in itself.
func (v MVCCRangeKeyVersions) Equal(other MVCCRangeKeyVersions) bool {
	if len(v) != len(other) {
		return false
	}
	for i := range v {
		if !v[i].Equal(other[i]) {
			return false
		}
	}
	return true
}

// Excise removes the versions in the given [from, to] span (inclusive, in
// order) in place, returning true if any versions were removed.
func (v *MVCCRangeKeyVersions) Excise(from, to hlc.Timestamp) bool {
	// We assume that to will often be near the current time, and use a linear
	// rather than a binary search, which will often match on the first range key.
	start := len(*v)
	for i, version := range *v {
		if version.Timestamp.LessEq(to) {
			start = i
			break
		}
	}

	// We then use a binary search to find the lower bound.
	end := sort.Search(len(*v), func(i int) bool {
		return (*v)[i].Timestamp.Less(from)
	})

	if start >= end {
		return false
	} else if start == 0 {
		*v = (*v)[end:]
	} else {
		*v = append((*v)[:start], (*v)[end:]...)
	}
	return true
}

// FirstAtOrAbove does a binary search for the first range key version at or
// above the given timestamp. Returns false if no matching range key was found.
func (v MVCCRangeKeyVersions) FirstAtOrAbove(ts hlc.Timestamp) (MVCCRangeKeyVersion, bool) {
	// This is kind of odd due to sort.Search() semantics: we do a binary search
	// for the first range key that's below the timestamp, then return the
	// previous range key if any.
	if length := len(v); length > 0 {
		if i := sort.Search(length, func(i int) bool {
			return v[i].Timestamp.Less(ts)
		}); i > 0 {
			return v[i-1], true
		}
	}
	return MVCCRangeKeyVersion{}, false
}

// FirstAtOrBelow does a binary search for the first range key version at or
// below the given timestamp. Returns false if no matching range key was found.
func (v MVCCRangeKeyVersions) FirstAtOrBelow(ts hlc.Timestamp) (MVCCRangeKeyVersion, bool) {
	if length := len(v); length > 0 {
		if i := sort.Search(length, func(i int) bool {
			return v[i].Timestamp.LessEq(ts)
		}); i < length {
			return v[i], true
		}
	}
	return MVCCRangeKeyVersion{}, false
}

// HasBetween checks whether an MVCC range key exists between the two given
// timestamps (both inclusive, in order).
func (v MVCCRangeKeyVersions) HasBetween(lower, upper hlc.Timestamp) bool {
	if version, ok := v.FirstAtOrAbove(lower); ok {
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

// Newest returns the timestamp of the newest range key in the stack.
func (v MVCCRangeKeyVersions) Newest() hlc.Timestamp {
	if v.IsEmpty() {
		return hlc.Timestamp{}
	}
	return v[0].Timestamp
}

// Oldest returns the timestamp of the oldest range key in the stack.
func (v MVCCRangeKeyVersions) Oldest() hlc.Timestamp {
	if v.IsEmpty() {
		return hlc.Timestamp{}
	}
	return v[len(v)-1].Timestamp
}

// Remove removes the given timestamp in place, returning it and true if it was
// found.
func (v *MVCCRangeKeyVersions) Remove(ts hlc.Timestamp) (MVCCRangeKeyVersion, bool) {
	if v.IsEmpty() {
		return MVCCRangeKeyVersion{}, false
	}
	// Fast path: check first version.
	if (*v)[0].Timestamp.Equal(ts) {
		r := (*v)[0]
		*v = (*v)[1:]
		return r, true
	}
	if i := sort.Search(len(*v), func(i int) bool {
		return (*v)[i].Timestamp.LessEq(ts)
	}); i < len(*v) && (*v)[i].Timestamp.Equal(ts) {
		r := (*v)[i]
		*v = append((*v)[:i], (*v)[i+1:]...)
		return r, true
	}
	return MVCCRangeKeyVersion{}, false
}

// String formats the MVCCRangeKeyVersions as a string.
func (v MVCCRangeKeyVersions) String() string {
	var sb strings.Builder
	sb.WriteString("[")
	for i, version := range v {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(version.String())
	}
	sb.WriteString("]")
	return sb.String()
}

// Timestamps returns the timestamps of all versions.
func (v MVCCRangeKeyVersions) Timestamps() []hlc.Timestamp {
	timestamps := make([]hlc.Timestamp, 0, len(v))
	for _, version := range v {
		timestamps = append(timestamps, version.Timestamp)
	}
	return timestamps
}

// Trim trims the versions to the time span [from, to] (both inclusive in
// order) in place. Returns true if any versions were removed.
func (v *MVCCRangeKeyVersions) Trim(from, to hlc.Timestamp) bool {
	var removed bool

	// We assume that to will often be near the current time, and use a linear
	// rather than a binary search, which will often match on the first range key.
	start := len(*v)
	for i, version := range *v {
		if version.Timestamp.LessEq(to) {
			start = i
			break
		}
	}
	*v = (*v)[start:]
	removed = start > 0

	// We then use a binary search to find the lower bound.
	if end := sort.Search(len(*v), func(i int) bool {
		return (*v)[i].Timestamp.Less(from)
	}); end < len(*v) {
		*v = (*v)[:end]
		removed = true
	}

	return removed
}

// Clone clones the version.
func (v MVCCRangeKeyVersion) Clone() MVCCRangeKeyVersion {
	if v.Value != nil {
		v.Value = append([]byte(nil), v.Value...)
	}
	if v.EncodedTimestampSuffix != nil {
		v.EncodedTimestampSuffix = append([]byte(nil), v.EncodedTimestampSuffix...)
	}
	return v
}

// Equal returns true if the two versions are equal.
func (v MVCCRangeKeyVersion) Equal(o MVCCRangeKeyVersion) bool {
	return v.Timestamp.Equal(o.Timestamp) && bytes.Equal(v.Value, o.Value)
}

// String formats the MVCCRangeKeyVersion as a string.
func (v MVCCRangeKeyVersion) String() string {
	return fmt.Sprintf("%s=%x", v.Timestamp, v.Value)
}

// EncodeMVCCTimestampSuffixWithSyntheticBitForTesting is a utility to encode
// the provided timestamp as a MVCC timestamp key suffix with the synthetic bit
// set. The synthetic bit is no longer encoded/decoded into the hlc.Timestamp
// but may exist in existing databases. This utility allows a test to construct
// a timestamp with the synthetic bit for testing appropriate handling of
// existing keys with the bit set. It should only be used in tests. See #129592.
//
// TODO(jackson): Remove this function when we've migrated all keys to unset the
// synthetic bit.
func EncodeMVCCTimestampSuffixWithSyntheticBitForTesting(ts hlc.Timestamp) []byte {
	const mvccEncodedTimestampWithSyntheticBitLen = mvccEncodedTimeWallLen +
		mvccEncodedTimeLogicalLen +
		mvccEncodedTimeSyntheticLen +
		mvccEncodedTimeLengthLen
	suffix := make([]byte, mvccEncodedTimestampWithSyntheticBitLen)
	encodeMVCCTimestampToBuf(suffix, ts)
	suffix[len(suffix)-2] = 0x01 // Synthetic bit.
	suffix[len(suffix)-1] = mvccEncodedTimestampWithSyntheticBitLen
	if decodedTS, err := DecodeMVCCTimestampSuffix(suffix); err != nil {
		panic(err)
	} else if !ts.Equal(decodedTS) {
		panic(errors.AssertionFailedf("manufactured MVCC timestamp with synthetic bit decoded to %s not %s",
			ts, decodedTS))
	}
	return suffix
}
