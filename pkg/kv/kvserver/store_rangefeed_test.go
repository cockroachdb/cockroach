// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		want    [2]time.Duration // {refresh, smear}
		waitErr error
	}{{
		want: [...]time.Duration{3 * time.Second, 1 * time.Millisecond}, // default
	}, {
		// By default, RangeFeedSmearInterval is 1ms.
		updates: []*settings.DurationSetting{RangeFeedRefreshInterval},
		updVals: []time.Duration{100 * time.Millisecond},
		want:    [...]time.Duration{100 * time.Millisecond, 1 * time.Millisecond},
	}, {
		// When zero, RangeFeedSmearInterval picks up RangeFeedRefreshInterval.
		updates: []*settings.DurationSetting{RangeFeedRefreshInterval, RangeFeedSmearInterval},
		updVals: []time.Duration{100 * time.Millisecond, 0},
		want:    [...]time.Duration{100 * time.Millisecond, 100 * time.Millisecond},
	}, {
		// When zero, RangeFeedSmearInterval picks up RangeFeedRefreshInterval,
		// which defaults to 3s.
		updates: []*settings.DurationSetting{RangeFeedSmearInterval},
		updVals: []time.Duration{0},
		want:    [...]time.Duration{3 * time.Second, 3 * time.Second},
	}, {
		// When zero, RangeFeedRefreshInterval picks up SideTransportCloseInterval.
		updates: []*settings.DurationSetting{RangeFeedRefreshInterval, closedts.SideTransportCloseInterval},
		updVals: []time.Duration{0, 10 * time.Millisecond},
		want:    [...]time.Duration{10 * time.Millisecond, 1 * time.Millisecond},
	}, {
		// Zero value is not a valid configuration.
		updates: []*settings.DurationSetting{closedts.SideTransportCloseInterval,
			RangeFeedRefreshInterval},
		updVals: []time.Duration{0, 0},
		want:    [...]time.Duration{0, 0},
		waitErr: context.DeadlineExceeded,
	}, {
		updates: []*settings.DurationSetting{RangeFeedRefreshInterval, RangeFeedSmearInterval},
		updVals: []time.Duration{100 * time.Millisecond, 5 * time.Millisecond},
		want:    [...]time.Duration{100 * time.Millisecond, 5 * time.Millisecond},
	}, {
		updates: []*settings.DurationSetting{RangeFeedSmearInterval},
		updVals: []time.Duration{5 * time.Millisecond},
		want:    [...]time.Duration{3 * time.Second, 5 * time.Millisecond},
	}, {
		// Misconfigurations (potentially transient) are handled gracefully.
		updates: []*settings.DurationSetting{closedts.SideTransportCloseInterval,
			RangeFeedRefreshInterval, RangeFeedSmearInterval},
		updVals: []time.Duration{1 * time.Second, 10 * time.Second, 100 * time.Second},
		want:    [...]time.Duration{10 * time.Second, 10 * time.Second},
	}, {
		updates: []*settings.DurationSetting{RangeFeedRefreshInterval, RangeFeedSmearInterval},
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
			refresh, smear := conf.get()
			assert.Equal(t, tc.want, [...]time.Duration{refresh, smear})

			ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
			defer cancel()
			var err error
			refresh, smear, err = conf.wait(ctx)
			require.ErrorIs(t, err, tc.waitErr)
			if tc.waitErr != nil {
				return
			}
			assert.Equal(t, tc.want, [...]time.Duration{refresh, smear})
		})
	}
}

func TestRangeFeedUpdaterPace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		desc string

		deadline time.Duration   // deadline for completing the run
		smear    time.Duration   // the wake-up interval
		work     int             // the number of items to process
		durs     []time.Duration // actual durations of each wake-up

		wantWork []int           // expected work planned on each wake-up
		wantBy   []time.Duration // expected "due by" time for each wake-up
		wantDone time.Duration   // expected total duration of the run
	}{{
		desc:     "no-smearing",
		deadline: 200, smear: 200, work: 1234,
		durs:     []time.Duration{55},
		wantWork: []int{1234},
		wantBy:   []time.Duration{200},
		wantDone: 55,
	}, {
		desc:     "within-schedule",
		deadline: 200, smear: 50, work: 123,
		durs:     []time.Duration{10, 5, 50, 20},
		wantWork: []int{30, 31, 31, 31},
		wantBy:   []time.Duration{50, 100, 150, 200},
		wantDone: 170,
	}, {
		desc:     "uneven-steps",
		deadline: 200, smear: 60, work: 123,
		durs:     []time.Duration{33, 44, 55, 11},
		wantWork: []int{36, 37, 37, 13},
		wantBy:   []time.Duration{60, 120, 180, 200},
		wantDone: 191,
	}, {
		desc:     "within-schedule-with-jitter",
		deadline: 200, smear: 50, work: 123,
		durs:     []time.Duration{51, 49, 53, 48},
		wantWork: []int{30, 31, 31, 31},
		wantBy:   []time.Duration{50, 101, 151, 200},
		wantDone: 202,
	}, {
		desc:     "with-temporary-delays",
		deadline: 100, smear: 10, work: 1000,
		durs:     []time.Duration{10, 20, 20, 30, 10, 10}, // caught up by t=100
		wantWork: []int{100, 100, 114, 137, 274, 275},
		wantBy:   []time.Duration{10, 20, 40, 60, 90, 100},
		wantDone: 100,
	}, {
		desc:     "with-delays-past-deadline",
		deadline: 200, smear: 50, work: 123,
		durs:     []time.Duration{78, 102, 53}, // longer than 200
		wantWork: []int{30, 38, 55},
		wantBy:   []time.Duration{50, 128, 200},
		wantDone: 233,
	}, {
		desc:     "small-work-with-jitter",
		deadline: 200, smear: 2, work: 5,
		durs:     []time.Duration{2, 3, 3, 1, 2},
		wantWork: []int{1, 1, 1, 1, 1},
		wantBy:   []time.Duration{2, 4, 7, 10, 12},
		wantDone: 12,
	}, {
		desc:     "no-work",
		deadline: 200, smear: 2, work: 0,
		durs:     []time.Duration{0},
		wantWork: []int{},
		wantBy:   []time.Duration{},
		wantDone: 0,
	}, {
		desc:     "in-one-go",
		deadline: 222, smear: 222, work: 2135,
		durs:     []time.Duration{123},
		wantWork: []int{2135},
		wantBy:   []time.Duration{222},
		wantDone: 123,
	}, {
		desc:     "not-enough-time",
		deadline: 10, smear: 2, work: 900000,
		durs:     []time.Duration{500, 10000},
		wantWork: []int{180000, 720000},
		wantBy:   []time.Duration{2, 500},
		wantDone: 10500,
	},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			durs := tc.durs
			gotWork := make([]int, 0, len(tc.wantWork))
			gotBy := make([]time.Duration, 0, len(tc.wantBy))

			start := timeutil.Unix(946684800, 0) // Jan 1, 2000
			now := start
			deadline := now.Add(tc.deadline)
			for work, startAt := tc.work, now; work != 0; {
				if startAt.After(now) { // imitate waiting
					now = startAt
				}
				todo, by := rangeFeedUpdaterPace(now, deadline, tc.smear, work)
				gotWork = append(gotWork, todo)
				gotBy = append(gotBy, by.Sub(start))

				// Imitate work and time passage during this work.
				work -= todo
				require.NotEmpty(t, durs)
				now = now.Add(durs[0])
				durs = durs[1:]

				startAt = by
			}

			assert.Equal(t, tc.wantWork, gotWork)
			assert.Equal(t, tc.wantBy, gotBy)
			assert.Equal(t, tc.wantDone, now.Sub(start))
		})
	}
}
