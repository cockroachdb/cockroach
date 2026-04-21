// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// EncodeKey encodes (user_key, mvcc_ts) into the byte sequence used as
// SSTable keys in revlog data files. The encoding sorts
// lexicographically by user_key ascending, then by mvcc_ts ascending,
// under bytes.Compare — so the SSTables can use Pebble's default
// comparer with no per-DB customization. See format §5.
//
// Layout:
//
//	EncodeBytesAscending(user_key) | uint64BE(walltime) | uvarint(logical)
//
// The bytes-ascending encoding is self-delimiting; walltime uses a
// fixed 8 bytes (HLC walltimes in nanoseconds since epoch span most of
// the uint64 range, leaving no varint savings); logical uses a
// sort-preserving uvarint, which is one byte for the common
// logical=0 case.
func EncodeKey(userKey roachpb.Key, ts hlc.Timestamp) []byte {
	var buf []byte
	buf = encoding.EncodeBytesAscending(buf, userKey)
	buf = encoding.EncodeUint64Ascending(buf, uint64(ts.WallTime))
	buf = encoding.EncodeUvarintAscending(buf, uint64(ts.Logical))
	return buf
}

// DecodeKey is the inverse of EncodeKey. It returns an error if b is
// not the exact encoding of a single (user_key, mvcc_ts) pair.
func DecodeKey(b []byte) (roachpb.Key, hlc.Timestamp, error) {
	rest, key, err := encoding.DecodeBytesAscending(b, nil)
	if err != nil {
		return nil, hlc.Timestamp{}, errors.Wrap(err, "decoding revlog key user_key")
	}
	rest, walltime, err := encoding.DecodeUint64Ascending(rest)
	if err != nil {
		return nil, hlc.Timestamp{}, errors.Wrap(err, "decoding revlog key walltime")
	}
	rest, logical, err := encoding.DecodeUvarintAscending(rest)
	if err != nil {
		return nil, hlc.Timestamp{}, errors.Wrap(err, "decoding revlog key logical")
	}
	if len(rest) != 0 {
		return nil, hlc.Timestamp{}, errors.Errorf("trailing bytes after revlog key: %x", rest)
	}
	return roachpb.Key(key), hlc.Timestamp{WallTime: int64(walltime), Logical: int32(logical)}, nil
}

// EncodeKeyPrefix encodes just the user_key portion of a revlog key,
// suitable for SeekGE to the first revision of userKey. The result is
// a strict prefix of EncodeKey(userKey, anyTs) — i.e. it sorts before
// every (userKey, *) and after every (userKey', *) for userKey' <
// userKey.
func EncodeKeyPrefix(userKey roachpb.Key) []byte {
	return encoding.EncodeBytesAscending(nil, userKey)
}
