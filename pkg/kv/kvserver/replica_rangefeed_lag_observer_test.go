// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestObserveClosedTimestampUpdate asserts that the expected signal is
// generated for each closed timestamp observation over time.
func TestObserveClosedTimestampUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	sv := &st.SV

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	targetDuration := 3 * time.Second
	cancelMultiple := int64(2)
	cancelMinDuration := 60 * time.Second

	RangeFeedLaggingCTCancelMultiple.Override(ctx, sv, cancelMultiple)
	RangeFeedLaggingCTCancelDuration.Override(ctx, sv, cancelMinDuration)

	tests := []struct {
		name    string
		updates []struct {
			closedTS time.Time
			now      time.Time
		}
		expected                   []rangeFeedCTLagSignal
		disableCancelBySettingZero bool
	}{
		{
			name: "no lag",
			updates: []struct {
				closedTS time.Time
				now      time.Time
			}{
				{
					closedTS: baseTime,
					now:      baseTime.Add(targetDuration),
				},
			},
			expected: []rangeFeedCTLagSignal{
				{
					lag:                       targetDuration,
					targetLag:                 targetDuration,
					exceedsNudgeLagThreshold:  false,
					exceedsCancelLagThreshold: false,
				},
			},
		},
		{
			name: "exceeds nudge threshold",
			updates: []struct {
				closedTS time.Time
				now      time.Time
			}{
				{
					closedTS: baseTime,
					now:      baseTime.Add(targetDuration * (laggingRangeFeedCTNudgeMultiple + 1)),
				},
			},
			expected: []rangeFeedCTLagSignal{
				{
					lag:                       targetDuration * (laggingRangeFeedCTNudgeMultiple + 1),
					targetLag:                 targetDuration,
					exceedsNudgeLagThreshold:  true,
					exceedsCancelLagThreshold: false,
				},
			},
		},
		{
			name: "exceeds cancel threshold but not duration",
			updates: []struct {
				closedTS time.Time
				now      time.Time
			}{
				{
					closedTS: baseTime,
					now:      baseTime.Add(targetDuration * (laggingRangeFeedCTNudgeMultiple + 1)),
				},
				{
					closedTS: baseTime,
					now:      baseTime.Add(targetDuration*(laggingRangeFeedCTNudgeMultiple+1) + cancelMinDuration/2),
				},
			},
			expected: []rangeFeedCTLagSignal{
				{
					lag:                       targetDuration * (laggingRangeFeedCTNudgeMultiple + 1),
					targetLag:                 targetDuration,
					exceedsNudgeLagThreshold:  true,
					exceedsCancelLagThreshold: false,
				},
				{
					lag:                       targetDuration*(laggingRangeFeedCTNudgeMultiple+1) + cancelMinDuration/2,
					targetLag:                 targetDuration,
					exceedsNudgeLagThreshold:  true,
					exceedsCancelLagThreshold: false,
				},
			},
		},
		{
			name: "exceeds cancel threshold and duration",
			updates: []struct {
				closedTS time.Time
				now      time.Time
			}{
				{
					closedTS: baseTime,
					now:      baseTime.Add(targetDuration * (laggingRangeFeedCTNudgeMultiple + 1)),
				},
				{
					closedTS: baseTime,
					now:      baseTime.Add(targetDuration*(laggingRangeFeedCTNudgeMultiple+1) + cancelMinDuration + time.Second),
				},
			},
			expected: []rangeFeedCTLagSignal{
				{
					lag:                       targetDuration * (laggingRangeFeedCTNudgeMultiple + 1),
					targetLag:                 targetDuration,
					exceedsNudgeLagThreshold:  true,
					exceedsCancelLagThreshold: false,
				},
				{
					lag:                       targetDuration*(laggingRangeFeedCTNudgeMultiple+1) + cancelMinDuration + time.Second,
					targetLag:                 targetDuration,
					exceedsNudgeLagThreshold:  true,
					exceedsCancelLagThreshold: true,
				},
			},
		},
		{
			name:                       "exceeds cancel threshold and duration but disabled",
			disableCancelBySettingZero: true,
			updates: []struct {
				closedTS time.Time
				now      time.Time
			}{
				{
					closedTS: baseTime,
					now:      baseTime.Add(targetDuration * (laggingRangeFeedCTNudgeMultiple + 1)),
				},
				{
					closedTS: baseTime,
					now:      baseTime.Add(targetDuration*(laggingRangeFeedCTNudgeMultiple+1) + cancelMinDuration + time.Second),
				},
			},
			expected: []rangeFeedCTLagSignal{
				{
					lag:                       targetDuration * (laggingRangeFeedCTNudgeMultiple + 1),
					targetLag:                 targetDuration,
					exceedsNudgeLagThreshold:  true,
					exceedsCancelLagThreshold: false,
				},
				{
					lag:                       targetDuration*(laggingRangeFeedCTNudgeMultiple+1) + cancelMinDuration + time.Second,
					targetLag:                 targetDuration,
					exceedsNudgeLagThreshold:  true,
					exceedsCancelLagThreshold: false,
				},
			},
		},
		{
			name: "recovers from lag",
			updates: []struct {
				closedTS time.Time
				now      time.Time
			}{
				{
					closedTS: baseTime,
					now:      baseTime.Add(targetDuration * (laggingRangeFeedCTNudgeMultiple + 1)),
				},
				{
					closedTS: baseTime.Add(targetDuration * (laggingRangeFeedCTNudgeMultiple + 1)),
					now:      baseTime.Add(targetDuration*(laggingRangeFeedCTNudgeMultiple+1) + targetDuration),
				},
			},
			expected: []rangeFeedCTLagSignal{
				{
					lag:                       targetDuration * (laggingRangeFeedCTNudgeMultiple + 1),
					targetLag:                 targetDuration,
					exceedsNudgeLagThreshold:  true,
					exceedsCancelLagThreshold: false,
				},
				{
					lag:                       targetDuration,
					targetLag:                 targetDuration,
					exceedsNudgeLagThreshold:  false,
					exceedsCancelLagThreshold: false,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			observer := newRangeFeedCTLagObserver()
			if tc.disableCancelBySettingZero {
				RangeFeedLaggingCTCancelMultiple.Override(ctx, sv, 0)
				defer RangeFeedLaggingCTCancelMultiple.Override(ctx, sv, cancelMultiple)
			}
			for i, update := range tc.updates {
				signal := observer.observeClosedTimestampUpdate(ctx, update.closedTS, update.now, sv)
				require.Equal(t, tc.expected[i], signal, "update %d produced unexpected signal", i)
			}
		})
	}
}
