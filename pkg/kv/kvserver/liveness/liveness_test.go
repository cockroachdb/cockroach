// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package liveness

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestShouldReplaceLiveness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	toMembershipStatus := func(membership string) livenesspb.MembershipStatus {
		switch membership {
		case "active":
			return livenesspb.MembershipStatus_ACTIVE
		case "decommissioning":
			return livenesspb.MembershipStatus_DECOMMISSIONING
		case "decommissioned":
			return livenesspb.MembershipStatus_DECOMMISSIONED
		default:
			err := fmt.Sprintf("unexpected membership: %s", membership)
			panic(err)
		}
	}

	l := func(epo int64, expiration hlc.Timestamp, draining bool, membership string) Record {
		liveness := livenesspb.Liveness{
			Epoch:      epo,
			Expiration: expiration.ToLegacyTimestamp(),
			Draining:   draining,
			Membership: toMembershipStatus(membership),
		}
		raw, err := protoutil.Marshal(&liveness)
		if err != nil {
			t.Fatal(err)
		}
		return Record{
			Liveness: liveness,
			raw:      raw,
		}
	}
	const (
		no  = false
		yes = true
	)
	now := hlc.Timestamp{WallTime: 12345}

	for _, test := range []struct {
		old, new Record
		exp      bool
	}{
		{
			// Epoch update only.
			l(1, hlc.Timestamp{}, false, "active"),
			l(2, hlc.Timestamp{}, false, "active"),
			yes,
		},
		{
			// No Epoch update, but Expiration update.
			l(1, now, false, "active"),
			l(1, now.Add(0, 1), false, "active"),
			yes,
		},
		{
			// No update.
			l(1, now, false, "active"),
			l(1, now, false, "active"),
			no,
		},
		{
			// Only Decommissioning changes.
			l(1, now, false, "active"),
			l(1, now, false, "decommissioning"),
			yes,
		},
		{
			// Only Decommissioning changes.
			l(1, now, false, "decommissioned"),
			l(1, now, false, "decommissioning"),
			yes,
		},
		{
			// Only Draining changes.
			l(1, now, false, "active"),
			l(1, now, true, "active"),
			yes,
		},
		{
			// Decommissioning changes, but Epoch moves backwards.
			l(10, now, true, "decommissioning"),
			l(9, now, true, "active"),
			no,
		},
		{
			// Draining changes, but Expiration moves backwards.
			l(10, now, false, "active"),
			l(10, now.Add(-1, 0), true, "active"),
			no,
		},
		{
			// Only raw encoding changes.
			l(1, now, false, "active"),
			func() Record {
				r := l(1, now, false, "active")
				r.raw = append(r.raw, []byte("different")...)
				return r
			}(),
			yes,
		},
	} {
		t.Run("", func(t *testing.T) {
			if act := livenessChanged(test.old, test.new); act != test.exp {
				t.Errorf("unexpected update: %+v", test)
			}
		})
	}
}

func TestNodeLivenessLivenessStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(100000, 100000)))
	nl := NewNodeLiveness(NodeLivenessOptions{Clock: clock, Settings: cluster.MakeTestingClusterSettings()})
	now := clock.Now()
	threshold := TimeUntilNodeDead.Default()

	for _, tc := range []struct {
		name     string
		liveness livenesspb.Liveness
		expected livenesspb.NodeLivenessStatus
	}{
		// Valid status.
		{
			name: "Valid 5 min",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(5 * time.Minute).ToLegacyTimestamp(),
				Draining:   false,
			},
			expected: livenesspb.NodeLivenessStatus_LIVE,
		},
		{
			name: "Expires just slightly in the future",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(time.Nanosecond).ToLegacyTimestamp(),
				Draining:   false,
			},
			expected: livenesspb.NodeLivenessStatus_LIVE,
		},
		{
			name: "Expired status",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.ToLegacyTimestamp(),
				Draining:   false,
			},
			expected: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
		{
			name: "Max bound of expired",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(-threshold).AddDuration(time.Nanosecond).ToLegacyTimestamp(),
				Draining:   false,
			},
			expected: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
		{
			name: "Dead",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(-threshold).ToLegacyTimestamp(),
				Draining:   false,
			},
			expected: livenesspb.NodeLivenessStatus_DEAD,
		},
		{
			name: "Decommissioning",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(time.Second).ToLegacyTimestamp(),
				Membership: livenesspb.MembershipStatus_DECOMMISSIONING,
				Draining:   false,
			},
			expected: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
		},
		{
			name: "Decommissioning + expired",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(-threshold).ToLegacyTimestamp(),
				Membership: livenesspb.MembershipStatus_DECOMMISSIONING,
				Draining:   false,
			},
			expected: livenesspb.NodeLivenessStatus_DECOMMISSIONED,
		},
		{
			name: "Decommissioning + live",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(time.Second).ToLegacyTimestamp(),
				Membership: livenesspb.MembershipStatus_DECOMMISSIONED,
				Draining:   false,
			},
			// Despite having marked the node as fully decommissioned, through
			// this NodeLivenessStatus API we still surface the node as
			// "Decommissioning". See #50707 for more details.
			expected: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
		},
		{
			name: "Decommissioning + expired",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(-threshold).ToLegacyTimestamp(),
				Membership: livenesspb.MembershipStatus_DECOMMISSIONED,
				Draining:   false,
			},
			expected: livenesspb.NodeLivenessStatus_DECOMMISSIONED,
		},
		{
			name: "Draining",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(5 * time.Minute).ToLegacyTimestamp(),
				Draining:   true,
			},
			expected: livenesspb.NodeLivenessStatus_DRAINING,
		},
		{
			name: "Decommissioning that is unavailable",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.ToLegacyTimestamp(),
				Draining:   false,
				Membership: livenesspb.MembershipStatus_DECOMMISSIONING,
			},
			expected: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
		{
			name: "Draining that is unavailable",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.ToLegacyTimestamp(),
				Draining:   true,
			},
			expected: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// TODO(baptist): Remove this when LivenessStatus is removed. This is here
			// to make sure we preserve existing behavior. Both the old and new
			// mechanisms should return the same result.
			//			require.Equal(t, tc.expected, storepool.LivenessStatus(tc.liveness, now, threshold))
			require.Equal(t, tc.expected, nl.convertToNodeVitality(tc.liveness).LivenessStatus())
		})
	}
}
