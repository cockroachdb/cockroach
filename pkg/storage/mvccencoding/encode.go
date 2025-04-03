// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mvccencoding

import (
	"encoding/binary"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// TODO(yuzefovich): consider moving storage.MVCCKey into this package and
// perhaps renaming it to something like 'mvcckey'.

const (
	mvccEncodedTimeSentinelLen  = 1
	mvccEncodedTimeWallLen      = 8
	mvccEncodedTimeLogicalLen   = 4
	mvccEncodedTimeSyntheticLen = 1
	mvccEncodedTimeLengthLen    = 1
)

// EncodeMVCCKeyToBufSized encodes an MVCCKey (which is decomposed into its
// parts) into its Pebble representation to the target buffer, which must have
// the correct size.
func EncodeMVCCKeyToBufSized(buf []byte, key roachpb.Key, ts hlc.Timestamp, keyLen int) {
	copy(buf, key)
	pos := len(key)

	buf[pos] = 0 // sentinel byte
	pos += mvccEncodedTimeSentinelLen

	tsLen := keyLen - pos - mvccEncodedTimeLengthLen
	if tsLen > 0 {
		EncodeMVCCTimestampToBufSized(buf[pos:], ts)
		pos += tsLen
		buf[pos] = byte(tsLen + mvccEncodedTimeLengthLen)
	}
}

// EncodeMVCCTimestampSuffix encodes an MVCC timestamp into its Pebble
// representation, including the length suffix but excluding the sentinel byte.
// This is equivalent to the Pebble suffix.
func EncodeMVCCTimestampSuffix(ts hlc.Timestamp) []byte {
	return EncodeMVCCTimestampSuffixToBuf(nil, ts)
}

// EncodeMVCCTimestampSuffixToBuf encodes an MVCC timestamp into its Pebble
// representation, including the length suffix but excluding the sentinel byte.
// This is equivalent to the Pebble suffix. It reuses the given byte buffer if
// it has sufficient capacity.
func EncodeMVCCTimestampSuffixToBuf(buf []byte, ts hlc.Timestamp) []byte {
	tsLen := EncodedMVCCTimestampLength(ts)
	if tsLen == 0 {
		return buf[:0]
	}
	suffixLen := tsLen + mvccEncodedTimeLengthLen
	if cap(buf) < suffixLen {
		buf = make([]byte, suffixLen)
	} else {
		buf = buf[:suffixLen]
	}
	EncodeMVCCTimestampToBufSized(buf, ts)
	buf[tsLen] = byte(suffixLen)
	return buf
}

// EncodeMVCCTimestampToBufSized encodes an MVCC timestamp into its Pebble
// representation, excluding the length suffix and sentinel byte. The target
// buffer must have the correct size, and the timestamp must not be empty.
func EncodeMVCCTimestampToBufSized(buf []byte, ts hlc.Timestamp) {
	binary.BigEndian.PutUint64(buf, uint64(ts.WallTime))
	if ts.Logical != 0 {
		binary.BigEndian.PutUint32(buf[mvccEncodedTimeWallLen:], uint32(ts.Logical))
	}
}

// EncodedMVCCKeyLength returns the encoded length of the given MVCCKey (which
// is decomposed into its parts).
func EncodedMVCCKeyLength(key roachpb.Key, ts hlc.Timestamp) int {
	// NB: We don't call into EncodedMVCCKeyPrefixLength() or
	// EncodedMVCCTimestampSuffixLength() here because the additional function
	// call overhead is significant.
	keyLen := len(key) + mvccEncodedTimeSentinelLen
	if !ts.IsEmpty() {
		keyLen += mvccEncodedTimeWallLen + mvccEncodedTimeLengthLen
		if ts.Logical != 0 {
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

// EncodedMVCCTimestampLength returns the encoded length of the given MVCC
// timestamp, excluding the length suffix and sentinel bytes.
func EncodedMVCCTimestampLength(ts hlc.Timestamp) int {
	// This is backwards, but EncodedMVCCKeyLength() is called in the
	// EncodeMVCCKey() hot path and an additional function call to this function
	// shows ~6% overhead in benchmarks. We therefore do the timestamp length
	// calculation inline in EncodedMVCCKeyLength(), and remove the excess here.
	tsLen := EncodedMVCCKeyLength(nil /* key */, ts) - mvccEncodedTimeSentinelLen
	if tsLen > 0 {
		tsLen -= mvccEncodedTimeLengthLen
	}
	return tsLen
}

// EncodedMVCCTimestampSuffixLength returns the encoded length of the
// given MVCC timestamp, including the length suffix. It returns 0
// if the timestamp is empty.
func EncodedMVCCTimestampSuffixLength(ts hlc.Timestamp) int {
	// This is backwards, see comment in EncodedMVCCTimestampLength() for why.
	return EncodedMVCCKeyLength(nil /* key */, ts) - mvccEncodedTimeSentinelLen
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
	EncodeMVCCTimestampToBufSized(suffix, ts)
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
