// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rspb

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type (
	RangeID   roachpb.RangeID
	ReplicaID roachpb.ReplicaID
	LogID     uint16
	RaftIndex kvpb.RaftIndex
)

// FullLogID uniquely identifies a Raft log. A Replica for a given RangeID may
// exist under multiple (increasing) ReplicaID over its lifetime due to
// rebalancing. Within a single ReplicaID, it may have multiple LogIDs over its
// lifetime due to snapshots.
type FullLogID struct {
	RangeID   RangeID
	ReplicaID ReplicaID
	LogID     LogID
}

func (id FullLogID) String() string {
	return fmt.Sprintf("r%d/%d.%d", id.RangeID, id.ReplicaID, id.LogID)
}
