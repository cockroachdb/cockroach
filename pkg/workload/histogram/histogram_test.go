// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package histogram

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestRegistry(t *testing.T) {
	const (
		good = "foo_bar_baz"
		bad  = "yes/we/do*not*@crash"
	)
	r := NewRegistry(10*time.Second, "lorkwoad")
	h := r.GetHandle()

	for i := 0; i < 3; i++ {
		t.Run("", func(t *testing.T) {
			h.Get(good).Record(time.Second)

			h.Get(bad).Record(time.Minute)
			h.Get(bad).Record(time.Second)

			hGood := h.mu.hists[good]
			hBad := h.mu.hists[bad]

			require.NotNil(t, hGood)
			require.NotNil(t, hBad)

			require.Equal(t, good, hGood.name)
			require.Contains(t, hGood.prometheusHistogram.Desc().String(), good)

			require.Equal(t, bad, hBad.name)
			require.Contains(t, hBad.prometheusHistogram.Desc().String(), "yes_we_do_not__crash")

			// Check that the "bad" metric registered both observations. We could do the same for hGood
			// but there's little reason for that to behave differently.
			ch := make(chan prometheus.Metric, 1)
			hBad.prometheusHistogram.Collect(ch)
			var m dto.Metric
			require.NoError(t, (<-ch).Write(&m))
			// The count should be cumulative, i.e. not reset on Tick().
			require.EqualValues(t, 2*(i+1), m.GetHistogram().GetSampleCount())

			r.Tick(func(tick Tick) {
				switch tick.Name {
				case good:
					// The good metric got pinged once, so 1 tick per current histogram
					// and i+1 in total.
					require.EqualValues(t, 1, tick.Hist.TotalCount())
					require.EqualValues(t, i+1, tick.Cumulative.TotalCount())
				case bad:
					// The bad case gets pinged twice per iteration, so double the numbers.
					require.EqualValues(t, 2, tick.Hist.TotalCount())
					require.EqualValues(t, 2*(i+1), tick.Cumulative.TotalCount())
				default:
					t.Errorf("unknown name %s", tick.Name)
				}
			})
		})
	}
}

func TestRegistrySameMetricFromMultipleHandles(t *testing.T) {
	r := NewRegistry(10*time.Second, "jepsen")
	const name = "foo"
	h1 := r.GetHandle()
	h2 := r.GetHandle()
	h1.Get(name).Record(time.Second)
	require.EqualValues(t, 1, h1.Get(name).mu.current.TotalCount())
	require.NotPanics(t, func() {
		h2.Get(name).Record(time.Minute)
	})
	// The handles don't share the underlying *NamedHistogram.
	require.NotEqual(t, h1.Get(name), h2.Get(name))
	require.EqualValues(t, 1, h2.Get(name).mu.current.TotalCount())
	require.EqualValues(t, 1, h1.Get(name).mu.current.TotalCount())
	// But when we observe via Tick(), they get merged, i.e. the sharding is
	// purely internal.
	r.Tick(func(tick Tick) {
		require.Equal(t, name, tick.Name)
		require.EqualValues(t, 2, tick.Hist.TotalCount())
		require.EqualValues(t, 2, tick.Cumulative.TotalCount())
	})
}

func TestRegistrySameMetricFromMultipleHandlesConcurrently(t *testing.T) {
	const num = 1000
	reg := NewRegistry(10*time.Second, "conc")
	h1, h2 := reg.GetHandle(), reg.GetHandle()
	require.NoError(t, ctxgroup.GroupWorkers(context.Background(), num, func(ctx context.Context, i int) error {
		h := h1
		if i%2 == 1 {
			h = h2
		}
		h.Get("op").Record(time.Minute)
		return nil
	}))
	reg.Tick(func(tick Tick) {
		require.EqualValues(t, num, tick.Cumulative.TotalCount())
	})
}
