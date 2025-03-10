// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package closedts

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTargetForPolicy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cast := func(i int, unit time.Duration) time.Duration { return time.Duration(i) * unit }
	secs := func(i int) time.Duration { return cast(i, time.Second) }
	millis := func(i int) time.Duration { return cast(i, time.Millisecond) }

	computeExpectedClosedTSTarget := func(now hlc.Timestamp, maxClockOffset time.Duration, sideTransportCloseInterval time.Duration, networkRTT time.Duration) hlc.Timestamp {
		const raftTransportOverhead = 20 * time.Millisecond
		raftTransportPropTime := (networkRTT*3)/2 + raftTransportOverhead
		sideTransportPropTime := networkRTT/2 + sideTransportCloseInterval
		maxTransportPropTime := max(sideTransportPropTime, raftTransportPropTime)
		const bufferTime = 25 * time.Millisecond
		leadTimeAtSender := maxTransportPropTime + maxClockOffset + bufferTime
		return now.Add(leadTimeAtSender.Nanoseconds(), 0)
	}

	now := hlc.Timestamp{WallTime: millis(100).Nanoseconds()}
	maxClockOffset := millis(500)

	sideTransportCloseInterval := millis(50)
	expClosedTSTarget := now.Add((maxClockOffset + millis(245) /* raftTransportPropTime */ +
		millis(25) /* bufferTime */).Nanoseconds(), 0)

	for _, tc := range []struct {
		name                       string
		lagTargetNanos             time.Duration
		leadTargetOverride         time.Duration
		sideTransportCloseInterval time.Duration
		observedMaxNetworkRTT      time.Duration
		rangePolicy                roachpb.RangeClosedTimestampPolicy
		expClosedTSTarget          hlc.Timestamp
	}{
		{
			name:                  "kv.closed_timestamp.target_duration - configured to lag by 3s",
			lagTargetNanos:        secs(3),
			rangePolicy:           roachpb.LAG_BY_CLUSTER_SETTING,
			expClosedTSTarget:     now.Add(-secs(3).Nanoseconds(), 0),
			observedMaxNetworkRTT: DefaultMaxNetworkRTT,
		},
		{
			name:                  "kv.closed_timestamp.target_duration - configured to lag by 1s",
			lagTargetNanos:        secs(1),
			rangePolicy:           roachpb.LAG_BY_CLUSTER_SETTING,
			expClosedTSTarget:     now.Add(-secs(1).Nanoseconds(), 0),
			observedMaxNetworkRTT: DefaultMaxNetworkRTT,
		},
		{
			name:                       "LEAD_FOR_GLOBAL_READS - dominated by side transport closed ts propagation",
			sideTransportCloseInterval: millis(200),
			rangePolicy:                roachpb.LEAD_FOR_GLOBAL_READS,
			expClosedTSTarget: now.
				Add((maxClockOffset +
					millis(275) /* sideTransportPropTime */ +
					millis(25) /* bufferTime */).Nanoseconds(), 0),
			observedMaxNetworkRTT: DefaultMaxNetworkRTT,
		},
		{
			name:                       "LEAD_FOR_GLOBAL_READS - dominated by raft transport closed ts propagation",
			sideTransportCloseInterval: sideTransportCloseInterval,
			rangePolicy:                roachpb.LEAD_FOR_GLOBAL_READS,
			expClosedTSTarget:          expClosedTSTarget,
			observedMaxNetworkRTT:      DefaultMaxNetworkRTT,
		},
		{
			name:                       "kv.closed_timestamp.lead_for_global_reads_auto_tune with no observations yet",
			sideTransportCloseInterval: sideTransportCloseInterval,
			rangePolicy:                roachpb.LEAD_FOR_GLOBAL_READS,
			expClosedTSTarget:          expClosedTSTarget,
			observedMaxNetworkRTT:      DefaultMaxNetworkRTT,
		},
		{
			name:                       "kv.closed_timestamp.lead_for_global_reads_override",
			leadTargetOverride:         millis(1234),
			sideTransportCloseInterval: millis(200),
			rangePolicy:                roachpb.LEAD_FOR_GLOBAL_READS,
			expClosedTSTarget:          now.Add(millis(1234).Nanoseconds(), 0),
			observedMaxNetworkRTT:      DefaultMaxNetworkRTT,
		},
		{
			name:                       "kv.closed_timestamp.lead_for_global_reads_override precedence over auto-tuning",
			leadTargetOverride:         millis(1234),
			sideTransportCloseInterval: millis(200),
			rangePolicy:                roachpb.LEAD_FOR_GLOBAL_READS,
			// auto-tuning is disabled when an override is set.
			expClosedTSTarget:     now.Add(millis(1234).Nanoseconds(), 0),
			observedMaxNetworkRTT: DefaultMaxNetworkRTT,
		},
		{
			name:                       "kv.closed_timestamp.lead_for_global_reads_auto_tune with RTT",
			sideTransportCloseInterval: millis(200),
			rangePolicy:                roachpb.LEAD_FOR_GLOBAL_READS,
			observedMaxNetworkRTT:      millis(200),
			// The observed max network RTT should be clamped to the upperBoundMaxNetworkRTT.
			expClosedTSTarget: computeExpectedClosedTSTarget(now, maxClockOffset, millis(200), millis(200)),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := TargetForPolicy(
				now.UnsafeToClockTimestamp(),  /*now*/
				maxClockOffset,                /*maxClockOffset*/
				tc.lagTargetNanos,             /*lagTargetDuration*/
				tc.leadTargetOverride,         /*leadTargetOverride*/
				tc.sideTransportCloseInterval, /*sideTransportCloseInterval*/
				tc.observedMaxNetworkRTT,      /*observedSideTransportLatency*/
				tc.rangePolicy,                /*policy*/
			)
			require.Equal(t, tc.expClosedTSTarget, target)
		})
	}
}
