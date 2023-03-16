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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

func TestRangeFeedUpdaterPace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		desc   string
		target time.Duration   // target for completing the run
		smear  time.Duration   // the wake-up interval
		work   int             // the number of items to process
		durs   []time.Duration // actual durations of each wake-up
		want   []int           // expected work planned on each wake-up
	}{{
		desc:   "no-smearing",
		target: 200, smear: 200, work: 1234,
		durs: []time.Duration{50},
		want: []int{1234},
	}, {
		desc:   "within-schedule",
		target: 200, smear: 50, work: 123,
		durs: []time.Duration{50, 50, 50, 50},
		want: []int{30, 31, 31, 31},
	}, {
		desc:   "uneven-steps",
		target: 200, smear: 60, work: 123,
		durs: []time.Duration{60, 60, 60, 20},
		want: []int{36, 37, 37, 13},
	}, {
		desc:   "within-schedule-with-jitter",
		target: 200, smear: 50, work: 123,
		durs: []time.Duration{51, 49, 53, 48},
		want: []int{30, 31, 31, 31},
	}, {
		desc:   "with-temporary-delays",
		target: 100, smear: 10, work: 1000,
		durs: []time.Duration{10, 20, 20, 30, 10, 10}, // caught up by t=100
		want: []int{100, 100, 114, 137, 274, 275},
	}, {
		desc:   "with-delays-past-deadline",
		target: 200, smear: 50, work: 123,
		durs: []time.Duration{78, 102, 53}, // longer than 200
		want: []int{30, 38, 55},
	}, {
		desc:   "small-work-with-jitter",
		target: 200, smear: 2, work: 5,
		durs: []time.Duration{2, 3, 3, 1, 2},
		want: []int{1, 1, 1, 1, 1},
	}, {
		desc:   "no-work",
		target: 200, smear: 2, work: 0,
		durs: []time.Duration{0},
		want: []int{0},
	}, {
		desc:   "in-one-go",
		target: 222, smear: 222, work: 2135,
		durs: []time.Duration{123}, // doesn't matter
		want: []int{2135},
	}, {
		desc:   "not-enough-time",
		target: 10, smear: 2, work: 900000,
		durs: []time.Duration{500, 10000},
		want: []int{180000, 720000},
	},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			now := timeutil.Unix(946684800, 0) // Jan 1, 2000
			target := now.Add(tc.target * time.Millisecond)
			got := make([]int, 0, len(tc.want))
			for work, durs := tc.work, tc.durs; ; {
				todo := rangeFeedUpdaterPace(target.Sub(now), tc.smear*time.Millisecond, work)
				got = append(got, todo)
				if work -= todo; work == 0 { // imitate work
					break
				}
				require.NotEmpty(t, durs)
				now = now.Add(durs[0] * time.Millisecond) // imitate time passage
				durs = durs[1:]
			}
			assert.Equal(t, tc.want, got)
		})
	}
}
