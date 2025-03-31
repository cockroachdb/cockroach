// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rspb

import "github.com/cockroachdb/cockroach/pkg/kv/kvpb"

type RaftLogKey kvpb.RaftIndex

func (RaftLogKey) KeyKind() KeyKind {
	return KeyKindRaftLogEntry
}

// <kind_prefix><component1><component2>...

// <kind><rid><lid>[<extra>]

// Codec translates between KeyKind and a byte slice representation.
type Codec interface {
	Encode(b []byte, k KeyKind, rid RangeID, lid LogID, ridx RaftIndex) []byte
	Decode(data []byte) ([]byte, KeyKind, RangeID, LogID, RaftIndex, error)
}
