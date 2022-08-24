// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package goschedstats

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"math"
	"runtime/metrics"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// schedulerLatencySamplePeriod controls how frequently the scheduler's
// latencies are sampled.
//
// TODO(irfansharif): What's the right frequency? Does it need to be adjusted
// during periods of high/low load?
var schedulerLatencySamplePeriod = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"goschedstats.scheduler_latency_sample_period",
	"controls how frequently the scheduler's latencies are sampled",
	2500*time.Millisecond,
)

// SchedulerLatencyCallback is provided the current value of the CPU scheduler's
// p99 latency.
type SchedulerLatencyCallback func(p99 time.Duration, period time.Duration)

// RegisterSchedulerLatencyCallback registers a callback to be run with observed
// scheduling latencies every goschedstats.scheduler_latency_sample_period.
func RegisterSchedulerLatencyCallback(cb SchedulerLatencyCallback) (id int64) {
	schedulerLatencyCallbackInfo.mu.Lock()
	defer schedulerLatencyCallbackInfo.mu.Unlock()

	id = schedulerLatencyCallbackInfo.id
	schedulerLatencyCallbackInfo.id++
	schedulerLatencyCallbackInfo.callbacks = append(schedulerLatencyCallbackInfo.callbacks, schedulerLatencyCallbackWithID{
		SchedulerLatencyCallback: cb,
		id:                       id,
	})
	return id
}

// UnregisterSchedulerLatencyCallback unregisters the callback to be run with
// observed scheduling latencies.
func UnregisterSchedulerLatencyCallback(id int64) {
	schedulerLatencyCallbackInfo.mu.Lock()
	defer schedulerLatencyCallbackInfo.mu.Unlock()

	newCBs := []schedulerLatencyCallbackWithID(nil)
	for i := range schedulerLatencyCallbackInfo.callbacks {
		if schedulerLatencyCallbackInfo.callbacks[i].id == id {
			continue
		}
		newCBs = append(newCBs, schedulerLatencyCallbackInfo.callbacks[i])
	}
	if len(newCBs)+1 != len(schedulerLatencyCallbackInfo.callbacks) {
		err := errors.AssertionFailedf("unexpected unregister: new count %d, old count %d",
			len(newCBs), len(schedulerLatencyCallbackInfo.callbacks))
		if buildutil.CrdbTestBuild {
			log.Fatalf(context.Background(), "%v", err)
		} else {
			log.Errorf(context.Background(), "%v", err)
		}
	}
	schedulerLatencyCallbackInfo.callbacks = newCBs
}

type schedulerLatencyCallbackWithID struct {
	SchedulerLatencyCallback
	id int64
}

var schedulerLatencyCallbackInfo struct {
	mu        syncutil.Mutex
	id        int64
	callbacks []schedulerLatencyCallbackWithID
}

// SchedulerLatencyStatsTicker contains the local state maintained across
// scheduler latency stats collection ticks.
type SchedulerLatencyStatsTicker struct {
	lastSample *metrics.Float64Histogram
}

// SampleSchedulerLatencyOnTick samples scheduler latency stats as the ticker
// has ticked. It invokes all callbacks registered with this package.
func (s *SchedulerLatencyStatsTicker) SampleSchedulerLatencyOnTick(period time.Duration) {
	schedulerLatencyCallbackInfo.mu.Lock()
	cbs := schedulerLatencyCallbackInfo.callbacks
	schedulerLatencyCallbackInfo.mu.Unlock()

	cur := sampleSchedulerLatencies()
	if s.lastSample == nil {
		s.lastSample = cur
		return
	}

	incr := sub(cur, s.lastSample)
	s.lastSample = cur
	latency := time.Duration(int64(percentile(incr, 0.99) * float64(time.Second.Nanoseconds())))
	for i := range cbs {
		cbs[i].SchedulerLatencyCallback(latency, period)
	}
}

// StartStatsTicker spawn a goroutine to periodically sample the scheduler
// latencies and invoke all registered listeners.
func StartStatsTicker(ctx context.Context, st *cluster.Settings, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "scheduler-latency-ticker", func(ctx context.Context) {
		mu := struct {
			syncutil.Mutex
			period time.Duration
		}{}

		mu.period = schedulerLatencySamplePeriod.Get(&st.SV)
		ticker := time.NewTicker(mu.period)
		defer ticker.Stop()

		schedulerLatencySamplePeriod.SetOnChange(&st.SV, func(ctx context.Context) {
			period := schedulerLatencySamplePeriod.Get(&st.SV)
			mu.Lock()
			mu.period = period
			mu.Unlock()
			ticker.Reset(period)
		})

		var slt SchedulerLatencyStatsTicker
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopper.ShouldQuiesce():
				return
			case <-ticker.C:
				mu.Lock()
				period := mu.period
				mu.Unlock()
				slt.SampleSchedulerLatencyOnTick(period)
			}
		}
	})
}

// sampleSchedulerLatencies samples the cumulative (since process start)
// scheduler latency histogram from the Go runtime.
func sampleSchedulerLatencies() *metrics.Float64Histogram {
	m := []metrics.Sample{
		{
			Name: "/sched/latencies:seconds",
		},
	}
	metrics.Read(m)
	v := &m[0].Value
	if v.Kind() != metrics.KindFloat64Histogram {
		panic("unexpected metric type")
	}
	h := v.Float64Histogram()
	return h
}

func clone(h *metrics.Float64Histogram) *metrics.Float64Histogram {
	res := &metrics.Float64Histogram{
		Counts:  make([]uint64, len(h.Counts)),
		Buckets: make([]float64, len(h.Buckets)),
	}
	copy(res.Counts, h.Counts)
	copy(res.Buckets, h.Buckets)
	return res
}

func sub(a, b *metrics.Float64Histogram) *metrics.Float64Histogram {
	res := clone(a)
	for i := 0; i < len(res.Counts); i++ {
		res.Counts[i] -= b.Counts[i]
	}
	return res
}

func percentile(h *metrics.Float64Histogram, p float64) float64 {
	// Counts contains the number of occurrences for each histogram bucket.
	// Given N buckets, Count[n] is the number of occurrences in the range
	// [bucket[n], bucket[n+1]), for 0 <= n < N.
	//
	// TODO(irfansharif): Consider maintaining the total count in the runtime
	// itself to make this cheaper if we're calling this at a high frequency.
	//
	// TODO(irfansharif): Consider adjusting the default bucket count in the
	// runtime to make this cheaper and with a more appropriate amount of
	// resolution.
	var total uint64
	for i := range h.Counts {
		if (i == 0 && math.IsInf(h.Buckets[0], -1)) ||
			(i == len(h.Counts)-1 && math.IsInf(h.Buckets[len(h.Buckets)-1], 1)) {
			// Buckets[0] and Buckets[len(Buckets)-1] are permitted to have
			// values -/+Inf respectively.
			continue
		}
		total += h.Counts[i]
	}

	// Iterate backwards (we're optimizing for higher percentiles) until we find
	// the right bucket.
	var cumulative uint64
	var min, max float64
	for i := len(h.Counts) - 1; i >= 0; i-- {
		if (i == 0 && math.IsInf(h.Buckets[0], -1)) ||
			(i == len(h.Counts)-1 && math.IsInf(h.Buckets[len(h.Buckets)-1], 1)) {
			continue
		}

		min, max = h.Buckets[i], h.Buckets[i+1]
		cumulative += h.Counts[i]
		if p == 1.0 {
			if cumulative > 0 {
				return max
			}
		} else if float64(total-cumulative) <= float64(total)*p {
			break
		}
	}
	if p == 0 {
		return min
	}
	return (min + max) / 2
}
