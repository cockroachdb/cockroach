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
	suspectThreshold := TimeAfterNodeSuspect.Default()

	for _, tc := range []struct {
		name           string
		liveness       livenesspb.Liveness
		descriptor     UpdateInfo
		expectedAlive  bool
		expectedStatus livenesspb.NodeLivenessStatus
	}{
		// Assume that we just haven't received a gossip update if we have a
		// liveness record but no descriptor.
		{
			name: "No descriptor",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(threshold).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{},
			expectedAlive:  true,
			expectedStatus: livenesspb.NodeLivenessStatus_LIVE,
		},
		{
			name: "Expired descriptor",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(threshold).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-threshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_DEAD,
		},
		{
			name: "Unavailable recent descriptor",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(threshold).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUnavailableTime: now.AddDuration(-1 * time.Second), lastUpdateTime: now.AddDuration(-1 * time.Second)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
		{
			name: "Unavailable descriptor a long time ago",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(threshold).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUnavailableTime: now.AddDuration(-suspectThreshold), lastUpdateTime: now},
			expectedAlive:  true,
			expectedStatus: livenesspb.NodeLivenessStatus_LIVE,
		},
		{
			name: "Unavailable and Expired descriptor",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(threshold).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUnavailableTime: now.AddDuration(-suspectThreshold), lastUpdateTime: now.AddDuration(-threshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_DEAD,
		},
		// Valid status.
		{
			name: "Valid 5 min",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(5 * time.Minute).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now},
			expectedAlive:  true,
			expectedStatus: livenesspb.NodeLivenessStatus_LIVE,
		},
		{
			name: "Expires just slightly in the future",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(time.Nanosecond).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now},
			expectedAlive:  true,
			expectedStatus: livenesspb.NodeLivenessStatus_LIVE,
		},
		{
			name: "Expired both",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-suspectThreshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
		{
			name: "Max bound of expired",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(-threshold).AddDuration(time.Nanosecond).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-suspectThreshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
		{
			name: "Dead",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(-threshold).ToLegacyTimestamp(),
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-threshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_DEAD,
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
			descriptor:     UpdateInfo{lastUpdateTime: now},
			expectedAlive:  true,
			expectedStatus: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
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
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-suspectThreshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_DECOMMISSIONED,
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
			descriptor: UpdateInfo{lastUpdateTime: now},
			// Despite having marked the node as fully decommissioned, through
			// this NodeLivenessStatus API we still surface the node as
			// "Decommissioning". See #50707 for more details.
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
		},
		{
			name: "Decommissioned + expired",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(-threshold).ToLegacyTimestamp(),
				Membership: livenesspb.MembershipStatus_DECOMMISSIONED,
				Draining:   false,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-threshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_DECOMMISSIONED,
		},
		{
			name: "Draining",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.AddDuration(5 * time.Minute).ToLegacyTimestamp(),
				Draining:   true,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now},
			expectedAlive:  true,
			expectedStatus: livenesspb.NodeLivenessStatus_DRAINING,
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
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-suspectThreshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
		{
			name: "Draining that is unavailable",
			liveness: livenesspb.Liveness{
				NodeID:     1,
				Epoch:      1,
				Expiration: now.ToLegacyTimestamp(),
				Draining:   true,
			},
			descriptor:     UpdateInfo{lastUpdateTime: now.AddDuration(-suspectThreshold)},
			expectedAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// TODO(baptist): Remove this when LivenessStatus is removed. This is here
			// to make sure we preserve existing behavior. Both the old and new
			// mechanisms should return the same result.
			// Use the set node descriptor.
			if (tc.descriptor != UpdateInfo{}) {
				nl.cache.mu.lastNodeUpdate[1] = tc.descriptor
			}

			nv := nl.convertToNodeVitality(tc.liveness)
			require.Equal(t, tc.expectedAlive, nv.IsLive(livenesspb.Rebalance))
			require.Equal(t, tc.expectedStatus, nv.LivenessStatus())
		})
	}
}
