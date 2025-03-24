// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sidetransport

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func TestTargetPolicyLocalityWithoutAutoTuning(t *testing.T) {
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}

	// Map representing cluster without auto-tuning - only has basic policies
	legacyTargets := map[ctpb.RangeClosedTimestampByPolicyLocality]hlc.Timestamp{
		ctpb.LAG_BY_CLUSTER_SETTING:                 ts1,
		ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY: ts2,
	}

	testCases := []struct {
		name           string
		policy         roachpb.RangeClosedTimestampPolicy
		locality       roachpb.LocalityComparisonType
		targetByPolicy map[ctpb.RangeClosedTimestampByPolicyLocality]hlc.Timestamp
		expectedTS     hlc.Timestamp
		expectedPolicy ctpb.RangeClosedTimestampByPolicyLocality
	}{
		{
			name:           "lag by cluster setting falls back to basic policy",
			policy:         roachpb.LAG_BY_CLUSTER_SETTING,
			locality:       roachpb.LocalityComparisonType_CROSS_REGION,
			targetByPolicy: legacyTargets,
			expectedTS:     ts1,
			expectedPolicy: ctpb.LAG_BY_CLUSTER_SETTING,
		},
		{
			name:           "cross region falls back to basic global reads",
			policy:         roachpb.LEAD_FOR_GLOBAL_READS,
			locality:       roachpb.LocalityComparisonType_CROSS_REGION,
			targetByPolicy: legacyTargets,
			expectedTS:     ts2,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY,
		},
		{
			name:           "cross zone falls back to basic global reads",
			policy:         roachpb.LEAD_FOR_GLOBAL_READS,
			locality:       roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE,
			targetByPolicy: legacyTargets,
			expectedTS:     ts2,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY,
		},
		{
			name:           "undefined locality uses no locality policy",
			policy:         roachpb.LEAD_FOR_GLOBAL_READS,
			locality:       roachpb.LocalityComparisonType_UNDEFINED,
			targetByPolicy: legacyTargets,
			expectedTS:     ts2,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY,
		},
		{
			name:           "empty map returns zero timestamp",
			policy:         roachpb.LEAD_FOR_GLOBAL_READS,
			locality:       roachpb.LocalityComparisonType_CROSS_REGION,
			targetByPolicy: map[ctpb.RangeClosedTimestampByPolicyLocality]hlc.Timestamp{},
			expectedTS:     hlc.Timestamp{},
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ts, policy := GetTargetAndPolicyLocality(tc.policy, tc.locality, tc.targetByPolicy)
			require.Equal(t, tc.expectedTS, ts)
			require.Equal(t, tc.expectedPolicy, policy)
		})
	}
}

func TestTargetPolicyLocalityWithAutoTuning(t *testing.T) {
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	ts3 := hlc.Timestamp{WallTime: 3}

	// Map representing cluster with auto-tuning - has locality-specific policies
	autoTuneTargets := map[ctpb.RangeClosedTimestampByPolicyLocality]hlc.Timestamp{
		ctpb.LAG_BY_CLUSTER_SETTING:                  ts1,
		ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY:  ts1,
		ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_REGION: ts2,
		ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_ZONE:   ts2,
		ctpb.LEAD_FOR_GLOBAL_READS_WITH_SAME_ZONE:    ts3,
	}

	testCases := []struct {
		name           string
		policy         roachpb.RangeClosedTimestampPolicy
		locality       roachpb.LocalityComparisonType
		targetByPolicy map[ctpb.RangeClosedTimestampByPolicyLocality]hlc.Timestamp
		expectedTS     hlc.Timestamp
		expectedPolicy ctpb.RangeClosedTimestampByPolicyLocality
		shouldPanic    bool
	}{
		{
			name:           "lag by cluster setting ignores locality",
			policy:         roachpb.LAG_BY_CLUSTER_SETTING,
			locality:       roachpb.LocalityComparisonType_CROSS_REGION,
			targetByPolicy: autoTuneTargets,
			expectedTS:     ts1,
			expectedPolicy: ctpb.LAG_BY_CLUSTER_SETTING,
		},
		{
			name:           "cross region uses specific policy",
			policy:         roachpb.LEAD_FOR_GLOBAL_READS,
			locality:       roachpb.LocalityComparisonType_CROSS_REGION,
			targetByPolicy: autoTuneTargets,
			expectedTS:     ts2,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_REGION,
		},
		{
			name:           "cross zone uses specific policy",
			policy:         roachpb.LEAD_FOR_GLOBAL_READS,
			locality:       roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE,
			targetByPolicy: autoTuneTargets,
			expectedTS:     ts2,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_ZONE,
		},
		{
			name:           "same zone uses specific policy",
			policy:         roachpb.LEAD_FOR_GLOBAL_READS,
			locality:       roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE,
			targetByPolicy: autoTuneTargets,
			expectedTS:     ts3,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_SAME_ZONE,
		},
		{
			name:           "undefined locality uses no locality policy",
			policy:         roachpb.LEAD_FOR_GLOBAL_READS,
			locality:       roachpb.LocalityComparisonType_UNDEFINED,
			targetByPolicy: autoTuneTargets,
			expectedTS:     ts1,
			expectedPolicy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY,
		},
		{
			name:           "invalid policy panics",
			policy:         roachpb.RangeClosedTimestampPolicy(99),
			locality:       roachpb.LocalityComparisonType_CROSS_REGION,
			targetByPolicy: autoTuneTargets,
			shouldPanic:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.shouldPanic {
				require.Panics(t, func() {
					GetTargetAndPolicyLocality(tc.policy, tc.locality, tc.targetByPolicy)
				})
				return
			}
			ts, policy := GetTargetAndPolicyLocality(tc.policy, tc.locality, tc.targetByPolicy)
			require.Equal(t, tc.expectedTS, ts)
			require.Equal(t, tc.expectedPolicy, policy)
		})
	}
}
