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

// TestNetworkRTTAndPolicyCalculations tests the conversion between network
// RTT and closed timestamp policy.
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
			name:           "negative latency",
			networkRTT:     -10 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO,
			expectedRTT:    DefaultMaxNetworkRTT, // default RTT for no latency info
		},
		{
			name:           "negative latency",
			networkRTT:     -50 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO,
			expectedRTT:    DefaultMaxNetworkRTT, // default RTT for no latency info
		},
		// 0-20ms bucket
		{
			name:           "zero latency",
			networkRTT:     0,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_20MS,
			expectedRTT:    10 * time.Millisecond,
		},
		{
			name:           "0-20ms bucket low",
			networkRTT:     1 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_20MS,
			expectedRTT:    10 * time.Millisecond,
		},
		{
			name:           "0-20ms bucket mid",
			networkRTT:     10 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_20MS,
			expectedRTT:    10 * time.Millisecond,
		},
		{
			name: "high-end bucket boundary",
			// 19.999999999ms in nanoseconds
			networkRTT:     19*time.Millisecond + 999*time.Microsecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_20MS,
			expectedRTT:    10 * time.Millisecond,
		},
		// 20-40ms bucket
		{
			name:           "20-40ms bucket low",
			networkRTT:     20 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_40MS,
			expectedRTT:    30 * time.Millisecond,
		},
		{
			name:           "20-40ms bucket mid",
			networkRTT:     30 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_40MS,
			expectedRTT:    30 * time.Millisecond,
		},
		{
			name:           "20-40ms bucket high",
			networkRTT:     39 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_40MS,
			expectedRTT:    30 * time.Millisecond,
		},
		// 40-60ms bucket
		{
			name:           "40-60ms bucket low",
			networkRTT:     40 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_60MS,
			expectedRTT:    50 * time.Millisecond,
		},
		{
			name:           "40-60ms bucket mid",
			networkRTT:     50 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_60MS,
			expectedRTT:    50 * time.Millisecond,
		},
		{
			name:           "40-60ms bucket high",
			networkRTT:     59 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_60MS,
			expectedRTT:    50 * time.Millisecond,
		},
		// 60-80ms bucket
		{
			name:           "60-80ms bucket low",
			networkRTT:     60 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_80MS,
			expectedRTT:    70 * time.Millisecond,
		},
		{
			name:           "60-80ms bucket mid",
			networkRTT:     70 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_80MS,
			expectedRTT:    70 * time.Millisecond,
		},
		{
			name:           "60-80ms bucket high",
			networkRTT:     79 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_80MS,
			expectedRTT:    70 * time.Millisecond,
		},
		// 80-100ms bucket
		{
			name:           "80-100ms bucket low",
			networkRTT:     80 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_100MS,
			expectedRTT:    90 * time.Millisecond,
		},
		{
			name:           "80-100ms bucket mid",
			networkRTT:     90 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_100MS,
			expectedRTT:    90 * time.Millisecond,
		},
		{
			name:           "80-100ms bucket high",
			networkRTT:     99 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_100MS,
			expectedRTT:    90 * time.Millisecond,
		},
		// 100-120ms bucket
		{
			name:           "100-120ms bucket low",
			networkRTT:     100 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_120MS,
			expectedRTT:    110 * time.Millisecond,
		},
		{
			name:           "100-120ms bucket mid",
			networkRTT:     110 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_120MS,
			expectedRTT:    110 * time.Millisecond,
		},
		{
			name:           "100-120ms bucket high",
			networkRTT:     119 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_120MS,
			expectedRTT:    110 * time.Millisecond,
		},
		// 120-140ms bucket
		{
			name:           "120-140ms bucket low",
			networkRTT:     120 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_140MS,
			expectedRTT:    130 * time.Millisecond,
		},
		{
			name:           "120-140ms bucket mid",
			networkRTT:     130 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_140MS,
			expectedRTT:    130 * time.Millisecond,
		},
		{
			name:           "120-140ms bucket high",
			networkRTT:     139 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_140MS,
			expectedRTT:    130 * time.Millisecond,
		},
		// 140-160ms bucket
		{
			name:           "140-160ms bucket low",
			networkRTT:     140 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_160MS,
			expectedRTT:    150 * time.Millisecond,
		},
		{
			name:           "140-160ms bucket mid",
			networkRTT:     150 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_160MS,
			expectedRTT:    150 * time.Millisecond,
		},
		{
			name:           "140-160ms bucket high",
			networkRTT:     159 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_160MS,
			expectedRTT:    150 * time.Millisecond,
		},
		// 160-180ms bucket
		{
			name:           "160-180ms bucket low",
			networkRTT:     160 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_180MS,
			expectedRTT:    170 * time.Millisecond,
		},
		{
			name:           "160-180ms bucket mid",
			networkRTT:     170 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_180MS,
			expectedRTT:    170 * time.Millisecond,
		},
		{
			name:           "160-180ms bucket high",
			networkRTT:     179 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_180MS,
			expectedRTT:    170 * time.Millisecond,
		},
		// 180-200ms bucket
		{
			name:           "180-200ms bucket low",
			networkRTT:     180 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_200MS,
			expectedRTT:    190 * time.Millisecond,
		},
		{
			name:           "180-200ms bucket mid",
			networkRTT:     190 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_200MS,
			expectedRTT:    190 * time.Millisecond,
		},
		{
			name:           "180-200ms bucket high",
			networkRTT:     199 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_200MS,
			expectedRTT:    190 * time.Millisecond,
		},
		// 200-220ms bucket
		{
			name:           "200-220ms bucket low",
			networkRTT:     200 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_220MS,
			expectedRTT:    210 * time.Millisecond,
		},
		{
			name:           "200-220ms bucket mid",
			networkRTT:     210 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_220MS,
			expectedRTT:    210 * time.Millisecond,
		},
		{
			name:           "200-220ms bucket high",
			networkRTT:     219 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_220MS,
			expectedRTT:    210 * time.Millisecond,
		},
		// 220-240ms bucket
		{
			name:           "220-240ms bucket low",
			networkRTT:     220 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_240MS,
			expectedRTT:    230 * time.Millisecond,
		},
		{
			name:           "220-240ms bucket mid",
			networkRTT:     230 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_240MS,
			expectedRTT:    230 * time.Millisecond,
		},
		{
			name:           "220-240ms bucket high",
			networkRTT:     239 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_240MS,
			expectedRTT:    230 * time.Millisecond,
		},
		// 240-260ms bucket
		{
			name:           "240-260ms bucket low",
			networkRTT:     240 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_260MS,
			expectedRTT:    250 * time.Millisecond,
		},
		{
			name:           "240-260ms bucket mid",
			networkRTT:     250 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_260MS,
			expectedRTT:    250 * time.Millisecond,
		},
		{
			name:           "240-260ms bucket high",
			networkRTT:     259 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_260MS,
			expectedRTT:    250 * time.Millisecond,
		},
		// 260-280ms bucket
		{
			name:           "260-280ms bucket low",
			networkRTT:     260 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_280MS,
			expectedRTT:    270 * time.Millisecond,
		},
		{
			name:           "260-280ms bucket mid",
			networkRTT:     270 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_280MS,
			expectedRTT:    270 * time.Millisecond,
		},
		{
			name:           "260-280ms bucket high",
			networkRTT:     279 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_280MS,
			expectedRTT:    270 * time.Millisecond,
		},
		// 280-300ms bucket
		{
			name:           "280-300ms bucket low",
			networkRTT:     280 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_300MS,
			expectedRTT:    290 * time.Millisecond,
		},
		{
			name:           "280-300ms bucket mid",
			networkRTT:     290 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_300MS,
			expectedRTT:    290 * time.Millisecond,
		},
		{
			name:           "just below 300ms",
			networkRTT:     299 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_300MS,
			expectedRTT:    290 * time.Millisecond,
		},
		// >=300ms bucket
		{
			name:           "exactly 300ms",
			networkRTT:     300 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_EQUAL_OR_GREATER_THAN_300MS,
			expectedRTT:    310 * time.Millisecond,
		},
		{
			name:           ">=300ms bucket mid",
			networkRTT:     350 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_EQUAL_OR_GREATER_THAN_300MS,
			expectedRTT:    310 * time.Millisecond,
		},
		{
			name:           ">=300ms bucket high",
			networkRTT:     400 * time.Millisecond,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_EQUAL_OR_GREATER_THAN_300MS,
			expectedRTT:    310 * time.Millisecond,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test RTT -> Policy conversion.
			policy := FindBucketBasedOnNetworkRTT(tc.networkRTT)
			require.Equal(t, tc.expectedPolicy, policy,
				"expected policy %v for RTT %v, got %v",
				tc.expectedPolicy, tc.networkRTT, policy)

			// Test Policy -> RTT conversion.
			rtt := computeNetworkRTTBasedOnPolicy(policy)
			require.Equal(t, tc.expectedRTT, rtt,
				"expected RTT %v for policy %v, got %v",
				tc.expectedRTT, policy, rtt)
		})
	}
}
