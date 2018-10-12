// Copyright 2018 The Cockroach Authors.
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

package enginepb

import (
	"encoding/binary"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

// Helpers here are split out of storage/engine since that package also contains
// rocksdb-interfacing code that pulls in our c-deps build requirements. These
// helpers are used by packages that do not want to pull in all of that, so they
// live here instead. We might want to undertake a larger refactor to split up
// the engine package, pulling MVCC logic out and/or pulling concrete rocksdb
// code out from abstract interfaces -- See #30114 and #30001.

// SplitMVCCKey returns the key and timestamp components of an encoded MVCC key.
// This decoding must match engine/db.cc:SplitKey().
func SplitMVCCKey(mvccKey []byte) (key []byte, ts []byte, ok bool) {
	if len(mvccKey) == 0 {
		return nil, nil, false
	}
	tsLen := int(mvccKey[len(mvccKey)-1])
	keyPartEnd := len(mvccKey) - 1 - tsLen
	if keyPartEnd < 0 {
		return nil, nil, false
	}

	key = mvccKey[:keyPartEnd]
	if tsLen > 0 {
		ts = mvccKey[keyPartEnd+1 : len(mvccKey)-1]
	}
	return key, ts, true
}

// DecodeKey decodes an key/timestamp from its serialized representation. This
// decoding must match engine/db.cc:DecodeKey().
func DecodeKey(encodedKey []byte) (key []byte, timestamp hlc.Timestamp, _ error) {
	key, ts, ok := SplitMVCCKey(encodedKey)
	if !ok {
		return nil, timestamp, errors.Errorf("invalid encoded mvcc key: %x", encodedKey)
	}
	switch len(ts) {
	case 0:
		// No-op.
	case 8:
		timestamp.WallTime = int64(binary.BigEndian.Uint64(ts[0:8]))
	case 12:
		timestamp.WallTime = int64(binary.BigEndian.Uint64(ts[0:8]))
		timestamp.Logical = int32(binary.BigEndian.Uint32(ts[8:12]))
	default:
		return nil, timestamp, errors.Errorf(
			"invalid encoded mvcc key: %x bad timestamp %x", encodedKey, ts)
	}

	return key, timestamp, nil
}

// ScanDecodeKeyValue decodes a key/value pair from a binary stream, such as in
// an MVCCScan "batch" (this is not the RocksDB batch repr format), returning
// both the key/value and the suffix of data remaining in the batch.
func ScanDecodeKeyValue(
	repr []byte,
) (key []byte, ts hlc.Timestamp, value []byte, orepr []byte, err error) {
	if len(repr) < 8 {
		return key, ts, nil, repr, errors.Errorf("unexpected batch EOF")
	}
	v := binary.LittleEndian.Uint64(repr)
	keySize := v >> 32
	valSize := v & ((1 << 32) - 1)
	if (keySize + valSize) > uint64(len(repr)) {
		return key, ts, nil, nil, errors.Errorf("expected %d bytes, but only %d remaining",
			keySize+valSize, len(repr))
	}
	repr = repr[8:]
	rawKey := repr[:keySize]
	value = repr[keySize : keySize+valSize]
	repr = repr[keySize+valSize:]
	key, ts, err = DecodeKey(rawKey)
	return key, ts, value, repr, err
}
