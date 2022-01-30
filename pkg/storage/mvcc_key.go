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
)

var (
	// MVCCKeyMax is a maximum mvcc-encoded key value which sorts after
	// all other keys.
	MVCCKeyMax = MakeMVCCMetadataKey(roachpb.KeyMax)
	// NilKey is the nil MVCCKey.
	NilKey = MVCCKey{}
)

// MVCCKey is a versioned key, distinguished from roachpb.Key with the addition
// of a timestamp.
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
	return encodedKeyLength(k)
}

// EncodeKey encodes an engine.MVCC key into the RocksDB representation.
func EncodeKey(key MVCCKey) []byte {
	keyLen := key.Len()
	buf := make([]byte, keyLen)
	encodeKeyToBuf(buf, key, keyLen)
	return buf
}

// EncodeKeyToBuf encodes an engine.MVCC key into the RocksDB representation.
func EncodeKeyToBuf(buf []byte, key MVCCKey) []byte {
	keyLen := key.Len()
	if cap(buf) < keyLen {
		buf = make([]byte, keyLen)
	} else {
		buf = buf[:keyLen]
	}
	encodeKeyToBuf(buf, key, keyLen)
	return buf
}

func encodeKeyToBuf(buf []byte, key MVCCKey, keyLen int) {
	const (
		timestampSentinelLen = 1
		walltimeEncodedLen   = 8
		logicalEncodedLen    = 4
		syntheticEncodedLen  = 1
	)

	copy(buf, key.Key)

	pos := len(key.Key)
	timestampLength := keyLen - pos - 1
	if timestampLength > 0 {
		buf[pos] = 0
		pos += timestampSentinelLen
		binary.BigEndian.PutUint64(buf[pos:], uint64(key.Timestamp.WallTime))
		pos += walltimeEncodedLen
		if key.Timestamp.Logical != 0 || key.Timestamp.Synthetic {
			binary.BigEndian.PutUint32(buf[pos:], uint32(key.Timestamp.Logical))
			pos += logicalEncodedLen
		}
		if key.Timestamp.Synthetic {
			buf[pos] = 1
			pos += syntheticEncodedLen
		}
	}
	buf[len(buf)-1] = byte(timestampLength)
}

func encodeTimestamp(ts hlc.Timestamp) []byte {
	_, encodedTS, _ := enginepb.SplitMVCCKey(EncodeKey(MVCCKey{Timestamp: ts}))
	return encodedTS
}

// DecodeMVCCKey decodes an engine.MVCCKey from its serialized representation.
func DecodeMVCCKey(encodedKey []byte) (MVCCKey, error) {
	// TODO(erikgrinaker): merge in the enginepb decoding functions when it no
	// longer involves a problematic GCO dependency (via Pebble).
	k, ts, err := enginepb.DecodeKey(encodedKey)
	return MVCCKey{k, ts}, err
}

func encodedKeyLength(key MVCCKey) int {
	const (
		timestampSentinelLen      = 1
		walltimeEncodedLen        = 8
		logicalEncodedLen         = 4
		syntheticEncodedLen       = 1
		timestampEncodedLengthLen = 1
	)

	n := len(key.Key) + timestampEncodedLengthLen
	if !key.Timestamp.IsEmpty() {
		n += timestampSentinelLen + walltimeEncodedLen
		if key.Timestamp.Logical != 0 || key.Timestamp.Synthetic {
			n += logicalEncodedLen
		}
		if key.Timestamp.Synthetic {
			n += syntheticEncodedLen
		}
	}
	return n
}
