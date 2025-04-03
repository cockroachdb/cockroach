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
			refresh, smear := conf.getRefresh(), conf.getSmear()
			assert.Equal(t, tc.want, [...]time.Duration{refresh, smear})

			ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
			defer cancel()
			err := conf.wait(ctx)
			require.ErrorIs(t, err, tc.waitErr)
			if tc.waitErr != nil {
				return
			}
			refresh, smear = conf.getRefresh(), conf.getSmear()
			assert.Equal(t, tc.want, [...]time.Duration{refresh, smear})
		})
	}
}
