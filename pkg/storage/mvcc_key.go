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
	"encoding/binary"
	"fmt"

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
// of a "version" timestamp and an optional "local" clock timestamp.
//
// The version timestamp dictates the key's visibility to readers. Readers with
// read timestamps equal to or greater than the version timestamp observe the
// key. Readers with read timestamps below the version timestamp ignore the key.
// Keys are stored in decreasing version order, with the exception of version
// zero (timestamp 0), which is referred to as a "meta" version and is stored
// before all other versions of the same key.
//
// The local clock timestamp records the value of the local HLC clock on the
// leaseholder when the key was originally written. It is used to make claims
// about the relative real time ordering of the key's writer and readers when
// comparing a reader's uncertainty interval (and observed timestamps) to the
// key. Ignoring edge cases, readers with an observed timestamp from the key's
// leaseholder that is greater than the local clock timestamp stored in the key
// cannot make claims about real time ordering and must consider it possible
// that the key's write occurred before the read began. However, readers with an
// observed timestamp from the key's leaseholder that is less than the clock
// timestamp can claim that the reader captured that observed timestamp before
// the key was written and therefore can consider the key's write to have been
// concurrent with the read. In doing so, the reader can avoid an uncertainty
// restart. For more, see pkg/kv/kvserver/observedts/doc.go.
//
// If the local timestamp is empty, it is assumed to be equal to the version
// timestamp. This property is used to avoid storing the local clock timestamp
// in the common case where the HLC clock on the leaseholder is greater than or
// equal to the version timestamp at the time that it is written. If is safe for
// the local clock timestamp to be rounded down, as this will simply lead to
// additional uncertainty restarts. However, it is not safe for the local clock
// timestamp to be rounded up, as this could lead to stale reads.
type MVCCKey struct {
	Key            roachpb.Key
	Timestamp      hlc.Timestamp
	LocalTimestamp hlc.ClockTimestamp
}

// MakeMVCCMetadataKey creates an MVCCKey from a roachpb.Key.
func MakeMVCCMetadataKey(key roachpb.Key) MVCCKey {
	return MVCCKey{Key: key}
}

// Next returns the next key.
// The returned key will never contain a LocalTimestamp.
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
// The method does not consider either key's LocalTimestamp.
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
		// Note that this isn't quite accurate: timestamps consume between 9-25
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
	if k.Timestamp.Synthetic || !k.LocalTimestampNeeded() {
		return fmt.Sprintf("%s/%s", k.Key, k.Timestamp)
	}
	return fmt.Sprintf("%s/%s@%s", k.Key, k.Timestamp, k.LocalTimestamp)
}

// Format implements the fmt.Formatter interface.
func (k MVCCKey) Format(f fmt.State, c rune) {
	// TODO(nvanbenschoten): add the following logic to mirror String.
	//  if !k.IsValue() {
	//  	fmt.Fprintf(f, "%s", k.Key)
	//  } else if { ...
	if k.Timestamp.Synthetic || !k.LocalTimestampNeeded() {
		fmt.Fprintf(f, "%s/%s", k.Key, k.Timestamp)
	} else {
		fmt.Fprintf(f, "%s/%s@%s", k.Key, k.Timestamp, k.LocalTimestamp)
	}
}

// Len returns the size of the MVCCKey when encoded. Implements the
// pebble.Encodeable interface.
func (k MVCCKey) Len() int {
	return encodedMVCCKeyLength(k)
}

// LocalTimestampNeeded returns whether the MVCCKey's local timestamp is needed,
// or whether it can be implied by (i.e. set to the same value as) its version
// timestamp.
func (k MVCCKey) LocalTimestampNeeded() bool {
	// If the local timestamp is empty, it is assumed to be equal to the version
	// timestamp and so the local timestamp is not needed.
	return !k.LocalTimestamp.IsEmpty() &&
		// If the local timestamp is not empty, it is safe for the local clock
		// timestamp to be rounded down, as this will simply lead to additional
		// uncertainty restarts. In such cases, the local timestamp is not needed.
		// However, it is not safe for the local clock timestamp to be rounded up,
		// as this could lead to stale reads. As a result, in such cases, the local
		// timestamp is needed and cannot be implied by the version timestamp.
		k.LocalTimestamp.ToTimestamp().Less(k.Timestamp)
}

// Normalize returns a normalized version of the MVCCKey. The returned MVCCKey
// is equivalent to the result of encodings and then decoding the receiver.
func (k MVCCKey) Normalize() MVCCKey {
	if k.Timestamp.Synthetic {
		k.LocalTimestamp = hlc.MinClockTimestamp
	} else if !k.LocalTimestampNeeded() {
		k.LocalTimestamp = hlc.ClockTimestamp(k.Timestamp)
	}
	return k
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
		encodeMVCCTimestampToBuf(buf[pos:], key.Timestamp, key.LocalTimestamp)
		pos += tsLen
		buf[pos] = byte(tsLen + mvccEncodedTimeLengthLen)
	}
}

// encodeMVCCTimestamp encodes an MVCC timestamp and a local timestamp into
// their Pebble representation, excluding length suffix and sentinel byte.
func encodeMVCCTimestamp(ts hlc.Timestamp, localTs hlc.ClockTimestamp) []byte {
	tsLen := encodedMVCCTimestampLength(ts, localTs)
	if tsLen == 0 {
		return nil
	}
	buf := make([]byte, tsLen)
	encodeMVCCTimestampToBuf(buf, ts, localTs)
	return buf
}

// EncodeMVCCTimestampSuffix encodes an MVCC timestamp and a local timestamp
// into their Pebble representation, including the length suffix but excluding
// the sentinel byte. This is equivalent to the Pebble suffix.
func EncodeMVCCTimestampSuffix(ts hlc.Timestamp, localTs hlc.ClockTimestamp) []byte {
	tsLen := encodedMVCCTimestampLength(ts, localTs)
	if tsLen == 0 {
		return nil
	}
	buf := make([]byte, tsLen+mvccEncodedTimeLengthLen)
	encodeMVCCTimestampToBuf(buf, ts, localTs)
	buf[tsLen] = byte(tsLen + mvccEncodedTimeLengthLen)
	return buf
}

// encodeMVCCTimestampToBuf encodes an MVCC timestamp and a local timestamp into
// their Pebble representation, excluding the length suffix and sentinel byte.
// The target buffer must have the correct size, and the timestamp must not be
// empty.
func encodeMVCCTimestampToBuf(buf []byte, ts hlc.Timestamp, localTs hlc.ClockTimestamp) {
	binary.BigEndian.PutUint64(buf, uint64(ts.WallTime))
	const afterWallTime = mvccEncodedTimeWallLen
	needLocalTs := !localTs.IsEmpty() && localTs.ToTimestamp().Less(ts) // LocalTimestampNeeded
	if ts.Logical != 0 || ts.Synthetic || needLocalTs {
		binary.BigEndian.PutUint32(buf[afterWallTime:], uint32(ts.Logical))
		const afterLogical = afterWallTime + mvccEncodedTimeLogicalLen
		if ts.Synthetic {
			// TODO(nvanbenschoten): stop writing Synthetic timestamps in v23.1.
			buf[afterLogical] = 1
		} else if needLocalTs {
			binary.BigEndian.PutUint64(buf[afterLogical:], uint64(localTs.WallTime))
			const afterLocalWallTime = afterLogical + mvccEncodedTimeWallLen
			if localTs.Logical != 0 {
				binary.BigEndian.PutUint32(buf[afterLocalWallTime:], uint32(localTs.Logical))
			}
		}
	}
}

// encodedMVCCKeyLength returns the encoded length of the given MVCCKey.
func encodedMVCCKeyLength(key MVCCKey) int {
	// NB: We don't call into encodedMVCCKeyPrefixLength() or
	// encodedMVCCTimestampSuffixLength() here because the additional function
	// call overhead is significant.
	keyLen := len(key.Key) + mvccEncodedTimeSentinelLen
	if !key.Timestamp.IsEmpty() {
		keyLen += mvccEncodedTimeWallLen + mvccEncodedTimeLengthLen
		needLocalTs := key.LocalTimestampNeeded()
		if key.Timestamp.Logical != 0 || key.Timestamp.Synthetic || needLocalTs {
			keyLen += mvccEncodedTimeLogicalLen
			if key.Timestamp.Synthetic {
				// TODO(nvanbenschoten): stop writing Synthetic timestamps in v23.1.
				keyLen += mvccEncodedTimeSyntheticLen
			} else if needLocalTs {
				keyLen += mvccEncodedTimeWallLen
				if key.LocalTimestamp.Logical != 0 {
					keyLen += mvccEncodedTimeLogicalLen
				}
			}
		}
	}
	return keyLen
}

// encodedMVCCKeyPrefixLength returns the encoded length of a roachpb.Key prefix
// including the sentinel byte.
func encodedMVCCKeyPrefixLength(key roachpb.Key) int {
	return len(key) + mvccEncodedTimeSentinelLen
}

// encodedMVCCTimestampLength returns the encoded length of the given MVCC
// timestamp and local timestamp, excluding the length suffix and sentinel
// bytes.
func encodedMVCCTimestampLength(ts hlc.Timestamp, localTs hlc.ClockTimestamp) int {
	// This is backwards, but encodedMVCCKeyLength() is called in the
	// EncodeMVCCKey() hot path and an additional function call to this function
	// shows ~6% overhead in benchmarks. We therefore do the timestamp length
	// calculation inline in encodedMVCCKeyLength(), and remove the excess here.
	key := MVCCKey{Timestamp: ts, LocalTimestamp: localTs}
	tsLen := encodedMVCCKeyLength(key) - mvccEncodedTimeSentinelLen
	if tsLen > 0 {
		tsLen -= mvccEncodedTimeLengthLen
	}
	return tsLen
}

// encodedMVCCTimestampSuffixLength returns the encoded length of the
// given MVCC timestamp, including the length suffix. It returns 0
// if the timestamp is empty.
func encodedMVCCTimestampSuffixLength(ts hlc.Timestamp, localTs hlc.ClockTimestamp) int {
	// This is backwards, see comment in encodedMVCCTimestampLength() for why.
	return encodedMVCCKeyLength(MVCCKey{Timestamp: ts, LocalTimestamp: localTs}) - mvccEncodedTimeSentinelLen
}

// TODO(erikgrinaker): merge in the enginepb decoding functions once it can
// avoid the storage package's problematic CGo dependency (via Pebble).

// DecodeMVCCKey decodes an MVCCKey from its Pebble representation.
func DecodeMVCCKey(encodedKey []byte) (MVCCKey, error) {
	k, ts, localTs, err := enginepb.DecodeKey(encodedKey)
	return MVCCKey{k, ts, localTs}, err
}

// decodeMVCCTimestamp decodes an MVCC timestamp and local timestamp from their
// Pebble representation, excluding the length suffix.
func decodeMVCCTimestamp(encodedTS []byte) (hlc.Timestamp, hlc.ClockTimestamp, error) {
	// NB: This logic is duplicated in enginepb.DecodeKey() to avoid the
	// overhead of an additional function call there (~13%).
	var ts hlc.Timestamp
	var localTs hlc.ClockTimestamp
	switch len(encodedTS) {
	case 0:
		// No-op.
	case 8:
		ts.WallTime = int64(binary.BigEndian.Uint64(encodedTS[0:8]))
		localTs = hlc.ClockTimestamp(ts) // see MVCCKey.Normalize
	case 12:
		ts.WallTime = int64(binary.BigEndian.Uint64(encodedTS[0:8]))
		ts.Logical = int32(binary.BigEndian.Uint32(encodedTS[8:12]))
		localTs = hlc.ClockTimestamp(ts) // see MVCCKey.Normalize
	case 13:
		ts.WallTime = int64(binary.BigEndian.Uint64(encodedTS[0:8]))
		ts.Logical = int32(binary.BigEndian.Uint32(encodedTS[8:12]))
		// TODO(nvanbenschoten): In v23.1, remove the Synthetic flag.
		ts.Synthetic = encodedTS[12] != 0
		localTs = hlc.MinClockTimestamp // see MVCCKey.Normalize
	case 20:
		ts.WallTime = int64(binary.BigEndian.Uint64(encodedTS[0:8]))
		ts.Logical = int32(binary.BigEndian.Uint32(encodedTS[8:12]))
		localTs.WallTime = int64(binary.BigEndian.Uint64(encodedTS[12:20]))
	case 24:
		ts.WallTime = int64(binary.BigEndian.Uint64(encodedTS[0:8]))
		ts.Logical = int32(binary.BigEndian.Uint32(encodedTS[8:12]))
		localTs.WallTime = int64(binary.BigEndian.Uint64(encodedTS[12:20]))
		localTs.Logical = int32(binary.BigEndian.Uint32(encodedTS[20:24]))
	default:
		return hlc.Timestamp{}, hlc.ClockTimestamp{}, errors.Errorf("bad timestamp %x", encodedTS)
	}
	return ts, localTs, nil
}

// decodeMVCCTimestampSuffix decodes an MVCC timestamp and a local timestamp
// from their Pebble representation, including the length suffix.
func decodeMVCCTimestampSuffix(encodedTS []byte) (hlc.Timestamp, hlc.ClockTimestamp, error) {
	if len(encodedTS) == 0 {
		return hlc.Timestamp{}, hlc.ClockTimestamp{}, nil
	}
	encodedLen := len(encodedTS)
	if suffixLen := int(encodedTS[encodedLen-1]); suffixLen != encodedLen {
		return hlc.Timestamp{}, hlc.ClockTimestamp{}, errors.Errorf(
			"bad timestamp: found length suffix %d, actual length %d", suffixLen, encodedLen)
	}
	return decodeMVCCTimestamp(encodedTS[:encodedLen-1])
}
