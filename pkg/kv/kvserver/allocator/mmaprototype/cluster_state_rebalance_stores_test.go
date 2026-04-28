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
	require.Equal(t, string(out), string(out.StripMarkers()))
}

func TestComputeMaxFractionPendingIncDec(t *testing.T) {
	tests := []struct {
		name           string
		reported       LoadVector
		adjusted       LoadVector
		expectedMaxInc float64
		expectedMaxDec float64
	}{
		{
			name:           "both_zero",
			reported:       LoadVector{},
			adjusted:       LoadVector{},
			expectedMaxInc: 0,
			expectedMaxDec: 0,
		},
		{
			name:           "reported_zero_adjusted_nonzero",
			reported:       LoadVector{},
			adjusted:       LoadVector{CPURate: 100, WriteBandwidth: 200, ByteSize: 300},
			expectedMaxInc: 1000,
			expectedMaxDec: 1000,
		},
		{
			name:           "reported_nonzero_adjusted_zero",
			reported:       LoadVector{CPURate: 100, WriteBandwidth: 200, ByteSize: 300},
			adjusted:       LoadVector{CPURate: 0, WriteBandwidth: 0, ByteSize: 0},
			expectedMaxInc: 0,
			expectedMaxDec: 1.0, // abs((0-100)/100) = 1.0, same for all dimensions
		},
		{
			name:           "simple_increase",
			reported:       LoadVector{CPURate: 100},
			adjusted:       LoadVector{CPURate: 150},
			expectedMaxInc: 0.5,
			expectedMaxDec: 0,
		},
		{
			name:           "simple_decrease",
			reported:       LoadVector{CPURate: 100},
			adjusted:       LoadVector{CPURate: 50},
			expectedMaxInc: 0,
			expectedMaxDec: 0.5,
		},
		{
			name:           "multiple_dimensions_increase",
			reported:       LoadVector{CPURate: 100, WriteBandwidth: 200, ByteSize: 300},
			adjusted:       LoadVector{CPURate: 150, WriteBandwidth: 250, ByteSize: 400},
			expectedMaxInc: 0.5, // max(0.5, 0.25, 0.333...)
			expectedMaxDec: 0,
		},
		{
			name:           "multiple_dimensions_decrease",
			reported:       LoadVector{CPURate: 100, WriteBandwidth: 200, ByteSize: 300},
			adjusted:       LoadVector{CPURate: 50, WriteBandwidth: 100, ByteSize: 150},
			expectedMaxInc: 0,
			expectedMaxDec: 0.5, // max(0.5, 0.5, 0.5)
		},
		{
			name:           "mixed_increase_and_decrease",
			reported:       LoadVector{CPURate: 100, WriteBandwidth: 200, ByteSize: 300},
			adjusted:       LoadVector{CPURate: 150, WriteBandwidth: 100, ByteSize: 300},
			expectedMaxInc: 0.5, // from CPURate
			expectedMaxDec: 0.5, // from WriteBandwidth
		},
		{
			name:           "negative_adjusted_load",
			reported:       LoadVector{CPURate: 100},
			adjusted:       LoadVector{CPURate: -50},
			expectedMaxInc: 0,
			expectedMaxDec: 1.5, // abs(-50 - 100) / 100 = 1.5
		},
		{
			name:           "fractional_change",
			reported:       LoadVector{CPURate: 1000},
			adjusted:       LoadVector{CPURate: 1100},
			expectedMaxInc: 0.1,
			expectedMaxDec: 0,
		},
		{
			name:           "max_across_dimensions",
			reported:       LoadVector{CPURate: 100, WriteBandwidth: 200, ByteSize: 300},
			adjusted:       LoadVector{CPURate: 120, WriteBandwidth: 300, ByteSize: 450},
			expectedMaxInc: 0.5, // max(0.2, 0.5, 0.5)
			expectedMaxDec: 0,
		},
		{
			name:           "one_dimension_zero_others_change",
			reported:       LoadVector{CPURate: 0, WriteBandwidth: 100, ByteSize: 200},
			adjusted:       LoadVector{CPURate: 50, WriteBandwidth: 150, ByteSize: 100},
			expectedMaxInc: 1000, // from CPURate (reported=0)
			expectedMaxDec: 1000, // from CPURate (reported=0)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maxInc, maxDec := computeMaxFractionPendingIncDec(tt.reported, tt.adjusted)
			require.InDelta(t, tt.expectedMaxInc, maxInc, 1e-9, "max fraction increase")
			require.InDelta(t, tt.expectedMaxDec, maxDec, 1e-9, "max fraction decrease")
		})
	}
}
