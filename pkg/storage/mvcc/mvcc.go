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

package mvcc

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

const (
	// VersionTimestampSize is the size of the timestamp portion of MVCC version keys (used to update stats).
	VersionTimestampSize int64 = 12
)

var (
	// KeyMax is a maximum mvcc-encoded key value which sorts after
	// all other keys.`
	KeyMax = MakeMVCCMetadataKey(roachpb.KeyMax)
	// NilKey is the nil Key.
	NilKey = Key{}
)

// Key is a versioned key, distinguished from roachpb.Key with the addition
// of a timestamp.
type Key struct {
	Key       roachpb.Key
	Timestamp hlc.Timestamp
}

// MakeMVCCMetadataKey creates an Key from a roachpb.Key.
func MakeMVCCMetadataKey(key roachpb.Key) Key {
	return Key{Key: key}
}

// Next returns the next key.
func (k Key) Next() Key {
	ts := k.Timestamp.Prev()
	if ts == (hlc.Timestamp{}) {
		return Key{
			Key: k.Key.Next(),
		}
	}
	return Key{
		Key:       k.Key,
		Timestamp: ts,
	}
}

// Less compares two keys.
func (k Key) Less(l Key) bool {
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
func (k Key) Equal(l Key) bool {
	return k.Key.Compare(l.Key) == 0 && k.Timestamp == l.Timestamp
}

// IsValue returns true iff the timestamp is non-zero.
func (k Key) IsValue() bool {
	return k.Timestamp != (hlc.Timestamp{})
}

// EncodedSize returns the size of the Key when encoded.
func (k Key) EncodedSize() int {
	n := len(k.Key) + 1
	if k.IsValue() {
		// Note that this isn't quite accurate: timestamps consume between 8-13
		// bytes. Fixing this only adjusts the accounting for timestamps, not the
		// actual on disk storage.
		n += int(VersionTimestampSize)
	}
	return n
}

// String returns a string-formatted version of the key.
func (k Key) String() string {
	if !k.IsValue() {
		return k.Key.String()
	}
	return fmt.Sprintf("%s/%s", k.Key, k.Timestamp)
}

// KeyValue contains the raw bytes of the value for a key.
type KeyValue struct {
	Key   Key
	Value []byte
}

// ScanDecodeKeyValue decodes a key/value pair returned in an MVCCScan
// "batch" (this is not the RocksDB batch repr format), returning both the
// key/value and the suffix of data remaining in the batch.
func ScanDecodeKeyValue(repr []byte) (key Key, value []byte, orepr []byte, err error) {
	if len(repr) < 8 {
		return key, nil, repr, errors.Errorf("unexpected batch EOF")
	}
	v := binary.LittleEndian.Uint64(repr)
	keySize := v >> 32
	valSize := v & ((1 << 32) - 1)
	if (keySize + valSize) > uint64(len(repr)) {
		return key, nil, nil, fmt.Errorf("expected %d bytes, but only %d remaining",
			keySize+valSize, len(repr))
	}
	repr = repr[8:]
	rawKey := repr[:keySize]
	value = repr[keySize : keySize+valSize]
	repr = repr[keySize+valSize:]
	key, err = DecodeKey(rawKey)
	return key, value, repr, err
}

// DecodeKey decodes an engine.Key from its serialized representation. This
// decoding must match engine/db.cc:DecodeKey().
func DecodeKey(encodedKey []byte) (Key, error) {
	key, ts, ok := SplitKey(encodedKey)
	if !ok {
		return Key{}, errors.Errorf("invalid encoded mvcc key: %x", encodedKey)
	}

	mvccKey := Key{Key: key}
	switch len(ts) {
	case 0:
		// No-op.
	case 8:
		mvccKey.Timestamp.WallTime = int64(binary.BigEndian.Uint64(ts[0:8]))
	case 12:
		mvccKey.Timestamp.WallTime = int64(binary.BigEndian.Uint64(ts[0:8]))
		mvccKey.Timestamp.Logical = int32(binary.BigEndian.Uint32(ts[8:12]))
	default:
		return Key{}, errors.Errorf(
			"invalid encoded mvcc key: %x bad timestamp %x", encodedKey, ts)
	}

	return mvccKey, nil
}

// SplitKey returns the key and timestamp components of an encoded MVCC key. This
// decoding must match engine/db.cc:SplitKey().
func SplitKey(Key []byte) (key []byte, ts []byte, ok bool) {
	if len(Key) == 0 {
		return nil, nil, false
	}
	tsLen := int(Key[len(Key)-1])
	keyPartEnd := len(Key) - 1 - tsLen
	if keyPartEnd < 0 {
		return nil, nil, false
	}

	key = Key[:keyPartEnd]
	if tsLen > 0 {
		ts = Key[keyPartEnd+1 : len(Key)-1]
	}
	return key, ts, true
}
