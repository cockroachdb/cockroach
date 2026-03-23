// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package wagpb

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/redact"
)

// MakeAddr constructs an Addr from a replica identity and raft log index.
func MakeAddr(id roachpb.FullReplicaID, index kvpb.RaftIndex) Addr {
	return Addr{
		RangeID:   id.RangeID,
		ReplicaID: id.ReplicaID,
		Index:     index,
	}
}

// String implements the fmt.Stringer interface.
func (a Addr) String() string {
	return redact.StringWithoutMarkers(a)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (a Addr) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("r%d/%d:%d", a.RangeID, a.ReplicaID, a.Index)
}

// SafeValue implements the redact.SafeValue interface.
func (EventType) SafeValue() {}

var _ redact.SafeValue = EventType(0)

// String implements the fmt.Stringer interface.
func (e Event) String() string {
	return redact.StringWithoutMarkers(e)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (e Event) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("(%s,%s)", e.Addr, e.Type)
}
