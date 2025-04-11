// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rspb

import (
	"slices"

	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

const (
	u16size     = 2
	u64size     = 8
	keyKindSize = 1
	keyLen      = keyKindSize + u64size + u16size
)

type CodecV2 struct {
}

var _ Codec = (*CodecV2)(nil)

func (c *CodecV2) Encode(b []byte, k KeyKind, rid RangeID, lid LogID, ridx RaftIndex) []byte {
	b = slices.Grow(b, keyLen)
	b = append(b, (byte)(k))
	b = encoding.EncodeUint64Ascending(b, uint64(rid))
	b = encoding.EncodeUint16Ascending(b, uint16(lid))
	b = encoding.EncodeUint64Ascending(b, uint64(ridx))
	return b
}

func (c *CodecV2) Decode(data []byte) ([]byte, KeyKind, RangeID, LogID, RaftIndex, error) {
	if len(data) < keyLen {
		return nil, 0, 0, 0, 0, errors.Newf("cannot decode key %x: got %d bytes, need %d", data, len(data), keyLen)
	}

	k := KeyKind(data[0])
	data = data[1:]

	var err error
	data, rid, _ := encoding.DecodeUint64Ascending(data)
	data, lid, _ := encoding.DecodeUint16Ascending(data)

	// Raft log entry needs to have an index.
	if len(data) == 0 && k != KeyKindRaftLogEntry {
		return data, k, RangeID(rid), LogID(lid), 0, nil
	}
	data, ridx, err := encoding.DecodeUint64Ascending(data)
	if err != nil {
		return nil, 0, 0, 0, 0, errors.Newf("not enough bytes to decode RaftIndex")
	}

	return data, k, RangeID(rid), LogID(lid), RaftIndex(ridx), nil
}
