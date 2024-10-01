// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enginepb

import (
	"encoding/binary"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// Helpers here are split out of storage/engine since that package also contains
// rocksdb-interfacing code that pulls in our c-deps build requirements. These
// helpers are used by packages that do not want to pull in all of that, so they
// live here instead. We might want to undertake a larger refactor to split up
// the engine package, pulling MVCC logic out and/or pulling concrete rocksdb
// code out from abstract interfaces -- See #30114 and #30001.

// SplitMVCCKey returns the key and timestamp components of an encoded MVCC key.
// This function is similar to storage.DecodeEngineKey.
// TODO(sumeer): remove SplitMVCCKey. It requires moving EngineKey into
// enginepb.
func SplitMVCCKey(mvccKey []byte) (key []byte, ts []byte, ok bool) {
	if len(mvccKey) == 0 {
		return nil, nil, false
	}
	tsLen := int(mvccKey[len(mvccKey)-1])
	if tsLen == 1 {
		// We never encode an empty version.
		return nil, nil, false
	}
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

// DecodeKey decodes an key/timestamp from its serialized representation.
func DecodeKey(encodedKey []byte) ([]byte, hlc.Timestamp, error) {
	key, encodedTS, ok := SplitMVCCKey(encodedKey)
	if !ok {
		return nil, hlc.Timestamp{}, errors.Errorf("invalid encoded mvcc key: %x", encodedKey)
	}
	// NB: This logic is duplicated with storage.decodeMVCCTimestamp() to avoid the
	// overhead of an additional function call (~13%).
	var timestamp hlc.Timestamp
	switch len(encodedTS) {
	case 0:
		// No-op.
	case 8:
		timestamp.WallTime = int64(binary.BigEndian.Uint64(encodedTS[0:8]))
	case 12, 13:
		timestamp.WallTime = int64(binary.BigEndian.Uint64(encodedTS[0:8]))
		timestamp.Logical = int32(binary.BigEndian.Uint32(encodedTS[8:12]))
		// NOTE: byte 13 used to store the timestamp's synthetic bit, but this is no
		// longer consulted and can be ignored during decoding.
	default:
		return nil, hlc.Timestamp{}, errors.Errorf(
			"invalid encoded mvcc key: %x bad timestamp %x", encodedKey, encodedTS)
	}
	return key, timestamp, nil
}

// kvLenSize is the number of bytes in the length prefix for each key/value
// pair in a MVCCScan batch. The first 4 bytes are a little-endian uint32
// containing the value size in bytes. The second 4 bytes are a little-endian
// uint32 containing the key size in bytes.
const kvLenSize = 8

// ScanDecodeKeyValue decodes a key/value pair from a binary stream, such as in
// an MVCCScan "batch" (this is not the RocksDB batch repr format), returning
// the key/value, the timestamp, and the suffix of data remaining in the batch.
func ScanDecodeKeyValue(
	repr []byte,
) (key []byte, ts hlc.Timestamp, value []byte, orepr []byte, err error) {
	if len(repr) < kvLenSize {
		return key, ts, nil, repr, errors.Errorf("unexpected batch EOF")
	}
	valSize := binary.LittleEndian.Uint32(repr)
	keyEnd := binary.LittleEndian.Uint32(repr[4:kvLenSize]) + kvLenSize
	if (keyEnd + valSize) > uint32(len(repr)) {
		return key, ts, nil, nil, errors.Errorf("expected %d bytes, but only %d remaining",
			keyEnd+valSize, len(repr))
	}
	rawKey := repr[kvLenSize:keyEnd]
	value = repr[keyEnd : keyEnd+valSize]
	repr = repr[keyEnd+valSize:]
	key, ts, err = DecodeKey(rawKey)
	return key, ts, value, repr, err
}

// ScanDecodeKeyValueNoTS decodes a key/value pair from a binary stream, such as
// in an MVCCScan "batch" (this is not the RocksDB batch repr format), returning
// the key/value and the suffix of data remaining in the batch.
func ScanDecodeKeyValueNoTS(repr []byte) (key []byte, value []byte, orepr []byte, err error) {
	if len(repr) < kvLenSize {
		return key, nil, repr, errors.Errorf("unexpected batch EOF")
	}
	valSize := binary.LittleEndian.Uint32(repr)
	keyEnd := binary.LittleEndian.Uint32(repr[4:kvLenSize]) + kvLenSize
	if len(repr) < int(keyEnd+valSize) {
		return key, nil, nil, errors.Errorf("expected %d bytes, but only %d remaining",
			keyEnd+valSize, len(repr))
	}

	ret := repr[keyEnd+valSize:]
	value = repr[keyEnd : keyEnd+valSize]
	var ok bool
	rawKey := repr[kvLenSize:keyEnd]
	key, _, ok = SplitMVCCKey(rawKey)
	if !ok {
		return nil, nil, nil, errors.Errorf("invalid encoded mvcc key: %x", rawKey)
	}
	return key, value, ret, err
}

// ScanDecodeKeyValues decodes all key/value pairs returned in one or more
// MVCCScan "batches" (this is not the RocksDB batch repr format). The provided
// function is called for each key/value pair.
func ScanDecodeKeyValues(
	repr [][]byte, fn func(key []byte, ts hlc.Timestamp, rawBytes []byte) error,
) error {
	var k []byte
	var ts hlc.Timestamp
	var rawBytes []byte
	var err error
	for _, data := range repr {
		for len(data) > 0 {
			k, ts, rawBytes, data, err = ScanDecodeKeyValue(data)
			if err != nil {
				return err
			}
			if err = fn(k, ts, rawBytes); err != nil {
				return err
			}
		}
	}
	return nil
}
