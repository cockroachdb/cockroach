// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package closedts

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestNetworkRTTAndPolicyCalculations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name           string
		networkRTT     time.Duration
		expectedPolicy ctpb.RangeClosedTimestampPolicy
		expectedRTT    time.Duration
	}{
		{
			name:           "zero latency",
			networkRTT:     0,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_20MS,
			expectedRTT:    10 * time.Millisecond, // 20ms/2
		},
		{
			name:           "mid first bucket",
			networkRTT:     10 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_20MS,
			expectedRTT:    10 * time.Millisecond, // 20ms/2
		},
		{
			name:           "bucket boundary",
			networkRTT:     40 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_60MS,
			expectedRTT:    30 * time.Millisecond, // 60ms/2
		},
		{
			name:           "just below 300ms",
			networkRTT:     299 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_300MS,
			expectedRTT:    150 * time.Millisecond, // 300ms/2
		},
		{
			name:           "exactly 300ms",
			networkRTT:     300 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_EQUAL_OR_GREATER_THAN_300MS,
			expectedRTT:    160 * time.Millisecond, // (320ms)/2
		},
		{
			name:           "above 300ms",
			networkRTT:     350 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_EQUAL_OR_GREATER_THAN_300MS,
			expectedRTT:    160 * time.Millisecond, // (320ms)/2
		},
		{
			name:           "exactly 20ms",
			networkRTT:     20 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_40MS,
			expectedRTT:    20 * time.Millisecond, // 40ms/2
		},
		{
			name:           "exactly 60ms",
			networkRTT:     60 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_80MS,
			expectedRTT:    40 * time.Millisecond, // 80ms/2
		},
		{
			name:           "exactly 100ms",
			networkRTT:     100 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_120MS,
			expectedRTT:    60 * time.Millisecond, // 120ms/2
		},
		{
			name:           "exactly 140ms",
			networkRTT:     140 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_160MS,
			expectedRTT:    80 * time.Millisecond, // 160ms/2
		},
		{
			name:           "exactly 180ms",
			networkRTT:     180 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_200MS,
			expectedRTT:    100 * time.Millisecond, // 200ms/2
		},
		{
			name:           "exactly 220ms",
			networkRTT:     220 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_240MS,
			expectedRTT:    120 * time.Millisecond, // 240ms/2
		},
		{
			name:           "exactly 260ms",
			networkRTT:     260 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_280MS,
			expectedRTT:    140 * time.Millisecond, // 280ms/2
		},
		{
			name:           "negative latency",
			networkRTT:     -10 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO,
			expectedRTT:    150 * time.Millisecond, // default
		},
		{
			name:           "negative latency",
			networkRTT:     -50 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO,
			expectedRTT:    150 * time.Millisecond, // default
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test RTT -> Policy conversion
			policy := FindBucketBasedOnNetworkRTT(tc.networkRTT)
			require.Equal(t, tc.expectedPolicy, policy,
				"expected policy %v for RTT %v, got %v",
				tc.expectedPolicy, tc.networkRTT, policy)

			// Test Policy -> RTT conversion
			rtt := computeNetworkRTTBasedOnPolicy(policy)
			require.Equal(t, tc.expectedRTT, rtt,
				"expected RTT %v for policy %v, got %v",
				tc.expectedRTT, policy, rtt)
		})
	}
}
