// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestFormatLoadPendingChanges(t *testing.T) {
	target := func(n, s int) roachpb.ReplicationTarget {
		return roachpb.ReplicationTarget{NodeID: roachpb.NodeID(n), StoreID: roachpb.StoreID(s)}
	}
	voter := func(id roachpb.ReplicaID) ReplicaIDAndType {
		return ReplicaIDAndType{
			ReplicaID:   id,
			ReplicaType: ReplicaType{ReplicaType: roachpb.VOTER_FULL},
		}
	}
	leaseholder := func(id roachpb.ReplicaID) ReplicaIDAndType {
		return ReplicaIDAndType{
			ReplicaID:   id,
			ReplicaType: ReplicaType{ReplicaType: roachpb.VOTER_FULL, IsLeaseholder: true},
		}
	}
	nonVoter := func(id roachpb.ReplicaID) ReplicaIDAndType {
		return ReplicaIDAndType{
			ReplicaID:   id,
			ReplicaType: ReplicaType{ReplicaType: roachpb.NON_VOTER},
		}
	}
	none := ReplicaIDAndType{ReplicaID: noReplicaID}
	change := func(id changeID, rangeID roachpb.RangeID, t roachpb.ReplicationTarget,
		prev, next ReplicaIDAndType, delta LoadVector,
	) *pendingReplicaChange {
		return &pendingReplicaChange{
			changeID: id,
			ReplicaChange: ReplicaChange{
				rangeID:   rangeID,
				target:    t,
				prev:      ReplicaState{ReplicaIDAndType: prev},
				next:      next,
				loadDelta: delta,
			},
		}
	}

	tests := []struct {
		name    string
		changes map[changeID]*pendingReplicaChange
	}{
		{name: "empty", changes: map[changeID]*pendingReplicaChange{}},
		{
			name: "all_enacted",
			changes: map[changeID]*pendingReplicaChange{
				1: func() *pendingReplicaChange {
					c := change(1, 7, target(4, 4),
						none,
						voter(unknownReplicaID),
						LoadVector{CPURate: 100})
					c.enactedAtTime = testingBaseTime
					return c
				}(),
			},
		},
		{
			name: "single_add_replica",
			changes: map[changeID]*pendingReplicaChange{
				1: change(1, 7, target(4, 4),
					none,
					voter(unknownReplicaID),
					LoadVector{CPURate: 100, WriteBandwidth: 1024}),
			},
		},
		{
			name: "single_remove_replica",
			changes: map[changeID]*pendingReplicaChange{
				1: change(1, 7, target(3, 3),
					voter(2),
					none,
					LoadVector{CPURate: -100}),
			},
		},
		{
			name: "lease_add_remove",
			changes: map[changeID]*pendingReplicaChange{
				2: change(2, 9, target(2, 2),
					voter(2), leaseholder(2),
					LoadVector{CPURate: 50}),
				1: change(1, 9, target(1, 1),
					leaseholder(1), voter(1),
					LoadVector{CPURate: -50}),
			},
		},
		{
			name: "change_replica_type",
			changes: map[changeID]*pendingReplicaChange{
				1: change(1, 9, target(4, 4),
					voter(3), nonVoter(3),
					LoadVector{}),
			},
		},
		{
			name: "mixed_sorted_by_change_id",
			changes: map[changeID]*pendingReplicaChange{
				3: change(3, 7, target(2, 2),
					voter(1),
					none,
					LoadVector{CPURate: -100}),
				1: change(1, 7, target(4, 4),
					none,
					voter(unknownReplicaID),
					LoadVector{CPURate: 100}),
				2: change(2, 8, target(5, 5),
					none,
					voter(unknownReplicaID),
					LoadVector{CPURate: 200, WriteBandwidth: 2048}),
			},
		},
	}
	w := echotest.NewWalker(t, datapathutils.TestDataPath(t, t.Name()))
	for _, tt := range tests {
		t.Run(tt.name, w.Run(t, tt.name, func(t *testing.T) string {
			return string(redact.Sprintf("%v\n", formatLoadPendingChanges(tt.changes)))
		}))
	}
	// Sanity check: all unsafe values should be marked. Building from the
	// known-safe fields of pendingReplicaChange means the output should not
	// contain any redaction markers.
	out := formatLoadPendingChanges(tests[len(tests)-1].changes)
	require.Equal(t, string(out), out.StripMarkers())
}
