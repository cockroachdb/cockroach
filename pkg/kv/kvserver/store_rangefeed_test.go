// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRangeFeedUpdaterConf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		updates []*settings.DurationSetting
		updVals []time.Duration  // len(updVals) == len(updates)
		want    [2]time.Duration // {refresh, sched}
		waitErr error
	}{{
		want: [...]time.Duration{200 * time.Millisecond, 200 * time.Millisecond}, // default
	}, {
		// By default RangeFeedRefreshInterval picks up SideTransportCloseInterval,
		// and RangeFeedSchedulingInterval picks up RangeFeedRefreshInterval.
		updates: []*settings.DurationSetting{closedts.SideTransportCloseInterval},
		updVals: []time.Duration{10 * time.Millisecond},
		want:    [...]time.Duration{10 * time.Millisecond, 10 * time.Millisecond},
	}, {
		// By default RangeFeedRefreshInterval picks up SideTransportCloseInterval.
		updates: []*settings.DurationSetting{closedts.SideTransportCloseInterval},
		updVals: []time.Duration{0},
		want:    [...]time.Duration{0, 0},
		waitErr: context.DeadlineExceeded,
	}, {
		updates: []*settings.DurationSetting{closedts.SideTransportCloseInterval,
			RangeFeedRefreshInterval},
		updVals: []time.Duration{0, 0},
		want:    [...]time.Duration{0, 0},
		waitErr: context.DeadlineExceeded,
	}, {
		// By default RangeFeedSchedulingInterval picks up RangeFeedRefreshInterval.
		updates: []*settings.DurationSetting{RangeFeedRefreshInterval},
		updVals: []time.Duration{100 * time.Millisecond},
		want:    [...]time.Duration{100 * time.Millisecond, 100 * time.Millisecond},
	}, {
		updates: []*settings.DurationSetting{RangeFeedRefreshInterval, RangeFeedSchedulingInterval},
		updVals: []time.Duration{100 * time.Millisecond, 1 * time.Millisecond},
		want:    [...]time.Duration{100 * time.Millisecond, 1 * time.Millisecond},
	}, {
		updates: []*settings.DurationSetting{RangeFeedSchedulingInterval},
		updVals: []time.Duration{1 * time.Millisecond},
		want:    [...]time.Duration{200 * time.Millisecond, 1 * time.Millisecond},
	}, {
		// Misconfigurations (potentially transient) are handled gracefully.
		updates: []*settings.DurationSetting{closedts.SideTransportCloseInterval,
			RangeFeedRefreshInterval, RangeFeedSchedulingInterval},
		updVals: []time.Duration{1 * time.Second, 10 * time.Second, 100 * time.Second},
		want:    [...]time.Duration{10 * time.Second, 10 * time.Second},
	}, {
		updates: []*settings.DurationSetting{RangeFeedRefreshInterval, RangeFeedSchedulingInterval},
		updVals: []time.Duration{-1 * time.Second, 1 * time.Second},
		want:    [...]time.Duration{200 * time.Millisecond, 200 * time.Millisecond},
	},
	} {
		t.Run("", func(t *testing.T) {
			ctx := context.Background()
			st := cluster.MakeClusterSettings()
			conf := newRangeFeedUpdaterConf(st)
			select {
			case <-conf.changed:
				t.Fatal("unexpected config change")
			default:
			}

			require.Len(t, tc.updVals, len(tc.updates))
			for i, setting := range tc.updates {
				setting.Override(ctx, &st.SV, tc.updVals[i])
			}
			if len(tc.updates) != 0 {
				<-conf.changed // we must observe an update, otherwise the test times out
			}
			refresh, sched := conf.get()
			assert.Equal(t, tc.want, [...]time.Duration{refresh, sched})

			ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
			defer cancel()
			var err error
			refresh, sched, err = conf.wait(ctx)
			require.ErrorIs(t, err, tc.waitErr)
			if tc.waitErr != nil {
				return
			}
			assert.Equal(t, tc.want, [...]time.Duration{refresh, sched})
		})
	}
}

func TestRangeFeedUpdaterSched(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		desc string

		deadline time.Duration   // the overall deadline
		step     time.Duration   // the wake-up interval
		items    int             // the number of items to process
		durs     []time.Duration // actual durations of each wake-up

		wantWork  []int           // expected work on each wake-up
		wantHints []time.Duration // expected deadline hints on each wake-up
	}{{
		desc:     "within-schedule",
		deadline: 200, step: 50, items: 123,
		durs:      []time.Duration{50, 50, 50, 50},
		wantWork:  []int{30, 31, 31, 31},
		wantHints: []time.Duration{50, 100, 150, 200},
	}, {
		desc:     "uneven-steps",
		deadline: 200, step: 60, items: 123,
		durs:      []time.Duration{60, 60, 60, 20},
		wantWork:  []int{36, 37, 37, 13},
		wantHints: []time.Duration{60, 120, 180, 200},
	}, {
		desc:     "within-schedule-with-jitter",
		deadline: 200, step: 50, items: 123,
		durs:      []time.Duration{51, 49, 53, 48},
		wantWork:  []int{30, 31, 31, 31},
		wantHints: []time.Duration{50, 101, 150, 200},
	}, {
		desc:     "with-temporary-delays",
		deadline: 100, step: 10, items: 1000,
		durs:      []time.Duration{10, 20, 20, 30, 10, 10}, // caught up by t=100
		wantWork:  []int{100, 100, 114, 137, 274, 275},
		wantHints: []time.Duration{10, 20, 40, 60, 90, 100},
	}, {
		desc:     "with-delays-past-deadline",
		deadline: 200, step: 50, items: 123,
		durs:      []time.Duration{78, 102, 53}, // longer than 200
		wantWork:  []int{30, 38, 55},
		wantHints: []time.Duration{50, 128, 200},
	}, {
		desc:     "small-work-with-jitter",
		deadline: 200, step: 2, items: 5,
		durs:      []time.Duration{2, 3, 3, 1, 2},
		wantWork:  []int{1, 1, 1, 1, 1},
		wantHints: []time.Duration{2, 4, 7, 10, 200},
	}, {
		desc:     "no-work",
		deadline: 200, step: 2, items: 0,
		durs:      []time.Duration{1}, // doesn't matter
		wantWork:  []int{0},
		wantHints: []time.Duration{200},
	}, {
		desc:     "in-one-go",
		deadline: 222, step: 222, items: 2135,
		durs:      []time.Duration{123}, // doesn't matter
		wantWork:  []int{2135},
		wantHints: []time.Duration{222},
	}, {
		desc:     "not-enough-time",
		deadline: 10, step: 2, items: 900000,
		durs:      []time.Duration{500, 10000},
		wantWork:  []int{180000, 720000},
		wantHints: []time.Duration{2, 10},
	},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			start := time.UnixMilli(12345)
			sched := newRangeFeedUpdaterSched(start, tc.deadline, tc.step, tc.items)

			gotWork := make([]int, 0, len(tc.wantWork))
			gotHints := make([]time.Duration, 0, len(tc.wantHints))

			for now, durs := sched.start, tc.durs; ; {
				todo, targetTime := sched.next()
				gotWork = append(gotWork, todo)
				gotHints = append(gotHints, targetTime.Sub(start))

				require.NotEmpty(t, durs)
				now = now.Add(durs[0]) // imitate time passage to process the items
				durs = durs[1:]

				sched.tick(now)
				if sched.items == 0 {
					break
				}
			}

			assert.Equal(t, tc.wantWork, gotWork)
			assert.Equal(t, tc.wantHints, gotHints)
		})
	}
}
