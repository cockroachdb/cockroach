// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockTaskPacerConfig is a test implementation of a task pacer configuration.
type MockTaskPacerConfig struct {
	// refresh is the interval between task batches
	refresh time.Duration
	// smear is the interval between task batches
	smear time.Duration
}

func newMockTaskPacerConfig(refresh, smear time.Duration) *MockTaskPacerConfig {
	return &MockTaskPacerConfig{
		refresh: refresh,
		smear:   smear,
	}
}

func (tp *MockTaskPacerConfig) getRefresh() time.Duration {
	return tp.refresh
}

func (tp *MockTaskPacerConfig) getSmear() time.Duration {
	return tp.smear
}

func TestTaskPacer(t *testing.T) {
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
		// If smear is set to 0, the taskPacer should not smear the work over
		// time.
		desc:     "zero-smear",
		deadline: 200, smear: 0, work: 1234,
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

			conf := newMockTaskPacerConfig(tc.deadline, tc.smear)
			pacer := NewTaskPacer(conf)
			pacer.StartTask(now)

			for work, startAt := tc.work, now; work != 0; {
				if startAt.After(now) { // imitate waiting
					now = startAt
				}
				todo, by := pacer.Pace(now, work)
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

// TestTaskPacerAccommodatesConfChanges tests that the taskPacer can accommodate
// changing the refresh and the smear intervals mid-run.
func TestTaskPacerAccommodatesConfChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		desc string

		refreshes []time.Duration // deadline for completing the run
		smears    []time.Duration // the wake-up interval
		work      int             // the number of items to process
		durs      []time.Duration // actual durations of each wake-up

		wantWork []int           // expected work planned on each wake-up
		wantBy   []time.Duration // expected "due by" time for each wake-up
		wantDone time.Duration   // expected total duration of the run
	}{{
		desc:      "constant",
		refreshes: []time.Duration{200, 200, 200, 200},
		smears:    []time.Duration{50, 50, 50, 50},
		work:      123,
		durs:      []time.Duration{5, 5, 5, 5},
		wantWork:  []int{30, 31, 31, 31},
		wantBy:    []time.Duration{50, 100, 150, 200},
		wantDone:  155,
	}, {
		desc:      "decrease-refresh",
		refreshes: []time.Duration{200, 100},
		smears:    []time.Duration{50, 50},
		work:      123,
		durs:      []time.Duration{5, 5},
		wantWork:  []int{30, 93},
		wantBy:    []time.Duration{50, 100},
		wantDone:  55,
	}, {
		desc:      "increase-refresh",
		refreshes: []time.Duration{200, 400, 400, 400, 400, 400, 400, 400},
		smears:    []time.Duration{50, 50, 50, 50, 50, 50, 50, 50},
		work:      123,
		durs:      []time.Duration{5, 5, 5, 5, 5, 5, 5, 5},
		wantWork:  []int{30, 13, 13, 13, 13, 13, 14, 14},
		wantBy:    []time.Duration{50, 100, 150, 200, 250, 300, 350, 400},
		wantDone:  355,
	}, {
		desc:      "decrease-smear",
		refreshes: []time.Duration{200, 200, 200, 200, 200, 200, 200},
		smears:    []time.Duration{50, 25, 25, 25, 25, 25, 25},
		work:      123,
		durs:      []time.Duration{5, 5, 5, 5, 5, 5, 5},
		wantWork:  []int{30, 15, 15, 15, 16, 16, 16},
		wantBy:    []time.Duration{50, 75, 100, 125, 150, 175, 200},
		wantDone:  180,
	}, {
		desc:      "increase-smear",
		refreshes: []time.Duration{200, 200, 200},
		smears:    []time.Duration{50, 100, 100},
		work:      123,
		durs:      []time.Duration{5, 5, 5},
		wantWork:  []int{30, 62, 31},
		wantBy:    []time.Duration{50, 150, 200},
		wantDone:  155,
	}} {
		t.Run(tc.desc, func(t *testing.T) {
			durs := tc.durs
			gotWork := make([]int, 0, len(tc.wantWork))
			gotBy := make([]time.Duration, 0, len(tc.wantBy))

			start := timeutil.Unix(946684800, 0) // Jan 1, 2000
			now := start

			conf := newMockTaskPacerConfig(tc.refreshes[0], tc.smears[0])
			pacer := NewTaskPacer(conf)
			pacer.StartTask(now)

			index := 0
			for work, startAt := tc.work, now; work != 0; {

				conf.refresh = tc.refreshes[index]
				conf.smear = tc.smears[index]

				if startAt.After(now) { // imitate waiting
					now = startAt
				}
				todo, by := pacer.Pace(now, work)
				gotWork = append(gotWork, todo)
				gotBy = append(gotBy, by.Sub(start))

				// Imitate work and time passage during this work.
				work -= todo
				require.NotEmpty(t, durs)
				now = now.Add(durs[0])
				durs = durs[1:]

				startAt = by
				index++
			}

			assert.Equal(t, tc.wantWork, gotWork)
			assert.Equal(t, tc.wantBy, gotBy)
			assert.Equal(t, tc.wantDone, now.Sub(start))
		})
	}

}
