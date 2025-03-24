// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftstoretoy/logpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type WAGIndex uint64

func (i WAGIndex) String() string { return fmt.Sprintf("op%d", i) }

// FullLogID uniquely identifies a Raft log. A Replica for a given RangeID may
// exist under multiple (increasing) ReplicaID over its lifetime due to
// rebalancing. Within a single ReplicaID, it may have multiple LogIDs over its
// lifetime due to snapshots.
type FullLogID struct {
	RangeID   roachpb.RangeID
	ReplicaID roachpb.ReplicaID
	LogID     logpb.LogID
}

func (id FullLogID) String() string {
	return fmt.Sprintf("r%d/%d.%d", id.RangeID, id.ReplicaID, id.LogID)
}

// ReplicaPos reflects the durable state of a Replica as persisted in the log
// engine. The aim of WAG recovery is to replay operations from the WAG to
// this end state.
type ReplicaPos struct {
	ID       FullLogID
	WAGIndex WAGIndex // WAG operation to replay to
}
