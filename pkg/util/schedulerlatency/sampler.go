// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schedulerlatency

import (
	"context"
	"fmt"
	"math"
	"runtime/metrics"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// samplePeriod controls the duration between consecutive scheduler latency
// samples.
//
// TODO(irfansharif): What's the right frequency? Does it need to be adjusted
// during periods of high/low load? This needs to be relatively high to drive
// high elastic CPU utilization within the prescribed limit (we only check
// requests in work queues as part of this tick). Might be worth checking for
// grantees more frequently independent of this sample period.
var samplePeriod = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"scheduler_latency.sample_period",
	"controls the duration between consecutive scheduler latency samples",
	100*time.Millisecond,
	func(period time.Duration) error {
		if period < time.Millisecond {
			return fmt.Errorf("minimum sample period is %s, got %s", time.Millisecond, period)
		}
		return nil
	},
)

var sampleDuration = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"scheduler_latency.sample_duration",
	"controls the duration over which each scheduler latency sample is a measurement over",
	2500*time.Millisecond,
	func(duration time.Duration) error {
		if duration < 100*time.Millisecond {
			return fmt.Errorf("minimum sample duration is %s, got %s", 100*time.Millisecond, duration)
		}
		return nil
	},
)

// StartSampler spawn a goroutine to periodically sample the scheduler latencies
// and invoke all registered callbacks.
func StartSampler(ctx context.Context, st *cluster.Settings, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "scheduler-latency-sampler", func(ctx context.Context) {
		settingsValuesMu := struct {
			syncutil.Mutex
			period, duration time.Duration
		}{}

		settingsValuesMu.period = samplePeriod.Get(&st.SV)
		settingsValuesMu.duration = sampleDuration.Get(&st.SV)
		ticker := time.NewTicker(settingsValuesMu.period)
		defer ticker.Stop()

		s := newSampler(settingsValuesMu.period, settingsValuesMu.duration)
		samplePeriod.SetOnChange(&st.SV, func(ctx context.Context) {
			period := samplePeriod.Get(&st.SV)
			settingsValuesMu.Lock()
			defer settingsValuesMu.Unlock()
			settingsValuesMu.period = period
			ticker.Reset(period)
			s.setPeriodAndDuration(settingsValuesMu.period, settingsValuesMu.duration)
		})
		sampleDuration.SetOnChange(&st.SV, func(ctx context.Context) {
			duration := sampleDuration.Get(&st.SV)
			settingsValuesMu.Lock()
			defer settingsValuesMu.Unlock()
			settingsValuesMu.duration = duration
			s.setPeriodAndDuration(settingsValuesMu.period, settingsValuesMu.duration)
		})

		for {
			select {
			case <-ctx.Done():
				return
			case <-stopper.ShouldQuiesce():
				return
			case <-ticker.C:
				settingsValuesMu.Lock()
				period := settingsValuesMu.period
				settingsValuesMu.Unlock()
				s.sampleOnTickAndInvokeCallbacks(period)
			}
		}
	})
}

// sampler contains the local state maintained across scheduler latency samples.
type sampler struct {
	mu struct {
		syncutil.Mutex
		ringBuffer ring.Buffer // contains *metrics.Float64Histogram
	}
}

func newSampler(period, duration time.Duration) *sampler {
	s := &sampler{}
	s.mu.ringBuffer = ring.MakeBuffer(nil)
	s.setPeriodAndDuration(period, duration)
	return s
}

func (s *sampler) setPeriodAndDuration(period, duration time.Duration) {
	s.mu.Lock()
	s.mu.ringBuffer.Discard()
	numSamples := int(duration / period)
	if numSamples < 1 {
		numSamples = 1 // we need at least one sample to compare (also safeguards against integer division)
	}
	s.mu.ringBuffer.Resize(numSamples)
	s.mu.Unlock()
}

// sampleOnTickAndInvokeCallbacks samples scheduler latency stats as the ticker
// has ticked. It invokes all callbacks registered with this package.
func (s *sampler) sampleOnTickAndInvokeCallbacks(period time.Duration) {
	latestCumulative := sample()
	oldestCumulative, ok := s.record(latestCumulative)
	if !ok {
		return
	}
	interval := sub(latestCumulative, oldestCumulative)
	latency := time.Duration(int64(percentile(interval, 0.99) * float64(time.Second.Nanoseconds())))

	globallyRegisteredCallbacks.mu.Lock()
	defer globallyRegisteredCallbacks.mu.Unlock()
	cbs := globallyRegisteredCallbacks.mu.callbacks
	for i := range cbs {
		cbs[i].cb(latency, period)
	}
}

func (s *sampler) record(
	sample *metrics.Float64Histogram,
) (oldest *metrics.Float64Histogram, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.ringBuffer.Len() == s.mu.ringBuffer.Cap() { // no more room, clear out the oldest
		oldest = s.mu.ringBuffer.GetLast().(*metrics.Float64Histogram)
		s.mu.ringBuffer.RemoveLast()
	}
	s.mu.ringBuffer.AddFirst(sample)
	return oldest, oldest != nil
}

// sample the cumulative (since process start) scheduler latency histogram from
// the go runtime.
func sample() *metrics.Float64Histogram {
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

// clone the given histogram.
func clone(h *metrics.Float64Histogram) *metrics.Float64Histogram {
	res := &metrics.Float64Histogram{
		Counts:  make([]uint64, len(h.Counts)),
		Buckets: make([]float64, len(h.Buckets)),
	}
	copy(res.Counts, h.Counts)
	copy(res.Buckets, h.Buckets)
	return res
}

// sub subtracts the counts of one histogram from another, assuming the bucket
// boundaries are the same. For cumulative scheduler latency histograms, this
// can be used to compute an interval histogram.
func sub(a, b *metrics.Float64Histogram) *metrics.Float64Histogram {
	res := clone(a)
	for i := 0; i < len(res.Counts); i++ {
		res.Counts[i] -= b.Counts[i]
	}
	return res
}

// percentile computes a specific percentile value of the given histogram.
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
	var total uint64 // total count across all buckets
	for i := range h.Counts {
		total += h.Counts[i]
	}

	// Iterate backwards (we're optimizing for higher percentiles) until we find
	// the right bucket.
	var cumulative uint64  // cumulative count of all buckets we've iterated through
	var start, end float64 // start and end of current bucket
	for i := len(h.Counts) - 1; i >= 0; i-- {
		start, end = h.Buckets[i], h.Buckets[i+1]
		if i == 0 && math.IsInf(h.Buckets[0], -1) { // -Inf
			// Buckets[0] is permitted to have -Inf; avoid interpolating with
			// infinity if our percentile value lies in this bucket.
			start = end
		}
		if i == len(h.Counts)-1 && math.IsInf(h.Buckets[len(h.Buckets)-1], 1) { // +Inf
			// Buckets[len(Buckets)-1] is permitted to have +Inf; avoid
			// interpolating with infinity if our percentile value lies in this
			// bucket.
			end = start
		}

		if start == end && math.IsInf(start, 0) {
			// Our (single) bucket boundary is [-Inf, +Inf), there's no
			// information.
			return 0.0
		}

		cumulative += h.Counts[i]
		if p == 1.0 {
			if cumulative > 0 {
				break // we've found the highest bucket with a non-zero count (i.e. pmax)
			}
		} else if float64(total-cumulative) <= float64(total)*p {
			break // we've found the bucket where the cumulative count until that point is p% of the total
		}
	}
	return (start + end) / 2 // linear interpolate within the bucket
}
