// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schedulerlatency

import (
	"context"
	"fmt"
	"math"
	"runtime/metrics"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
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
	settings.ApplicationLevel, // used in virtual clusters
	"scheduler_latency.sample_period",
	"controls the duration between consecutive scheduler latency samples",
	100*time.Millisecond,
	settings.WithValidateDuration(func(period time.Duration) error {
		if period < time.Millisecond {
			return fmt.Errorf("minimum sample period is %s, got %s", time.Millisecond, period)
		}
		return nil
	}),
)

var sampleDuration = settings.RegisterDurationSetting(
	settings.ApplicationLevel, // used in virtual clusters
	"scheduler_latency.sample_duration",
	"controls the duration over which each scheduler latency sample is a measurement over",
	2500*time.Millisecond,
	settings.WithValidateDuration(func(duration time.Duration) error {
		if duration < 100*time.Millisecond {
			return fmt.Errorf("minimum sample duration is %s, got %s", 100*time.Millisecond, duration)
		}
		return nil
	}),
)

var schedulerLatency = metric.Metadata{
	Name:        "go.scheduler_latency",
	Help:        "Go scheduling latency",
	Measurement: "Nanoseconds",
	Unit:        metric.Unit_NANOSECONDS,
}

// StartSampler spawn a goroutine to periodically sample the scheduler latencies
// and invoke all registered callbacks.
func StartSampler(
	ctx context.Context,
	st *cluster.Settings,
	stopper *stop.Stopper,
	registry *metric.Registry,
	statsInterval time.Duration,
	listener LatencyObserver,
) error {
	return stopper.RunAsyncTask(ctx, "scheduler-latency-sampler", func(ctx context.Context) {
		settingsValuesMu := struct {
			syncutil.Mutex
			period, duration time.Duration
		}{}

		settingsValuesMu.period = samplePeriod.Get(&st.SV)
		settingsValuesMu.duration = sampleDuration.Get(&st.SV)

		s := newSampler(settingsValuesMu.period, settingsValuesMu.duration, listener)
		_ = stopper.RunAsyncTask(ctx, "export-scheduler-stats", func(ctx context.Context) {
			// cpuSchedulerLatencyBuckets are prometheus histogram buckets
			// suitable for a histogram that records a (second-denominated)
			// quantity where measurements correspond to delays in scheduling
			// goroutines onto processors, i.e. are in the {micro,milli}-second
			// range during normal operation. See TestHistogramBuckets for more
			// details.
			cpuSchedulerLatencyBuckets := reBucketExpAndTrim(
				sample().Buckets,                   // original buckets
				1.1,                                // base
				(50 * time.Microsecond).Seconds(),  // min
				(100 * time.Millisecond).Seconds(), // max
			)

			schedulerLatencyHistogram := newRuntimeHistogram(schedulerLatency, cpuSchedulerLatencyBuckets)
			registry.AddMetric(schedulerLatencyHistogram)

			ticker := time.NewTicker(statsInterval) // compute periodic stats
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-stopper.ShouldQuiesce():
					return
				case <-ticker.C:
					lastIntervalHistogram := s.lastIntervalHistogram()
					if lastIntervalHistogram == nil {
						continue
					}

					schedulerLatencyHistogram.update(lastIntervalHistogram)
				}
			}
		})

		ticker := time.NewTicker(settingsValuesMu.period)
		defer ticker.Stop()
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
				period := func() time.Duration {
					settingsValuesMu.Lock()
					defer settingsValuesMu.Unlock()
					return settingsValuesMu.period
				}()
				s.sampleOnTickAndInvokeCallbacks(period)
			}
		}
	})
}

// sampler contains the local state maintained across scheduler latency samples.
type sampler struct {
	listener LatencyObserver
	mu       struct {
		syncutil.Mutex
		ringBuffer            ring.Buffer[*metrics.Float64Histogram]
		lastIntervalHistogram *metrics.Float64Histogram
	}
}

func newSampler(period, duration time.Duration, listener LatencyObserver) *sampler {
	s := &sampler{listener: listener}
	s.mu.ringBuffer = ring.MakeBuffer(([]*metrics.Float64Histogram)(nil))
	s.setPeriodAndDuration(period, duration)
	return s
}

func (s *sampler) setPeriodAndDuration(period, duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.ringBuffer.Discard()
	numSamples := int(duration / period)
	if numSamples < 1 {
		numSamples = 1 // we need at least one sample to compare (also safeguards against integer division)
	}
	s.mu.ringBuffer.Resize(numSamples)
	s.mu.lastIntervalHistogram = nil
}

// sampleOnTickAndInvokeCallbacks samples scheduler latency stats as the ticker
// has ticked. It invokes all callbacks registered with this package.
func (s *sampler) sampleOnTickAndInvokeCallbacks(period time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	latestCumulative := sample()
	oldestCumulative, ok := s.recordLocked(latestCumulative)
	if !ok {
		return
	}
	s.mu.lastIntervalHistogram = sub(latestCumulative, oldestCumulative)
	p99 := time.Duration(int64(percentile(s.mu.lastIntervalHistogram, 0.99) * float64(time.Second.Nanoseconds())))

	// Perform the callback if there's a listener.
	if s.listener != nil {
		s.listener.SchedulerLatency(p99, period)
	}
}

func (s *sampler) recordLocked(
	sample *metrics.Float64Histogram,
) (oldest *metrics.Float64Histogram, ok bool) {
	if s.mu.ringBuffer.Len() == s.mu.ringBuffer.Cap() { // no more room, clear out the oldest
		oldest = s.mu.ringBuffer.GetLast()
		s.mu.ringBuffer.RemoveLast()
	}
	s.mu.ringBuffer.AddFirst(sample)
	return oldest, oldest != nil
}

func (s *sampler) lastIntervalHistogram() *metrics.Float64Histogram {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.lastIntervalHistogram
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
		panic(fmt.Sprintf("unexpected metric type: %d (v=%+v m=%+v)", v.Kind(), v, m))
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
//
// TODO(irfansharif): Deduplicate this with the quantile computation in
// util/metrics? Here we're using the raw histogram at the highest resolution
// and with zero translation between types; there we rebucket the histogram and
// translate to another type.
func percentile(h *metrics.Float64Histogram, p float64) float64 {
	// Counts contains the number of occurrences for each histogram bucket.
	// Given N buckets, Count[n] is the number of occurrences in the range
	// [bucket[n], bucket[n+1]), for 0 <= n < N.
	//
	// TODO(irfansharif): Consider adjusting the default bucket count in the
	// runtime to make this cheaper and with a more appropriate amount of
	// resolution. The defaults can be seen through TestHistogramBuckets:
	//
	//   bucket[  0] width=0s boundary=[-Inf, 0s)
	//   bucket[  1] width=1ns boundary=[0s, 1ns)
	//   bucket[  2] width=1ns boundary=[1ns, 2ns)
	//   bucket[  3] width=1ns boundary=[2ns, 3ns)
	//   bucket[  4] width=1ns boundary=[3ns, 4ns)
	//   ...
	//   bucket[270] width=16.384µs boundary=[737.28µs, 753.664µs)
	//   bucket[271] width=16.384µs boundary=[753.664µs, 770.048µs)
	//   bucket[272] width=278.528µs boundary=[770.048µs, 1.048576ms)
	//   bucket[273] width=32.768µs boundary=[1.048576ms, 1.081344ms)
	//   bucket[274] width=32.768µs boundary=[1.081344ms, 1.114112ms)
	//   ...
	//   bucket[717] width=1h13m18.046511104s boundary=[53h45m14.046488576s, 54h58m32.09299968s)
	//   bucket[718] width=1h13m18.046511104s boundary=[54h58m32.09299968s, 56h11m50.139510784s)
	//   bucket[719] width=1h13m18.046511104s boundary=[56h11m50.139510784s, 57h25m8.186021888s)
	//   bucket[720] width=57h25m8.186021888s boundary=[57h25m8.186021888s, +Inf)
	//
	var total uint64 // total count across all buckets
	for i := range h.Counts {
		total += h.Counts[i]
	}

	// Linear approximation of the target value corresponding to percentile, p:
	//
	// Goal: We want to linear approximate the "p * total"-th value from the histogram.
	//      - p * total is the ordinal rank (or simply, rank) of the target value
	//        within the histogram bounds.
	//
	// Step 1: Find the bucket[i] which contains the target value.
	//      - This will be the largest bucket[i] where the left-bound cumulative
	//        count of bucket[i] <= p*total.
	//
	// Step 2: Determine the rank within the bucket (subset of histogram).
	//      - Since the bucket is a subset of the original data set, we can simply
	//        subtract left-bound cumulative to get the "subset_rank".
	//
	// Step 3: Use the "subset_rank" to interpolate the target value.
	//      - So we want to interpolate the "subset_rank"-th value of the subset_count
	//        that lies between [bucket_start, bucket_end). This is:
	//        bucket_start + (bucket_start - bucket_end) * linear_interpolator
	//        Where linear_interpolator = subset_rank / subset_count

	// (Step 1) Iterate backwards (we're optimizing for higher percentiles) until
	// we find the first bucket for which total-cumulative <= rank, which will be
	// by design the largest bucket that meets that condition.
	var cumulative uint64  // cumulative count of all buckets we've iterated through
	var start, end float64 // start and end of current bucket
	var i int              // index of current bucket
	for i = len(h.Counts) - 1; i >= 0; i-- {
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

	// (Step 2) Find the target rank within bucket boundaries.
	subsetRank := float64(total)*p - float64(total-cumulative)

	// (Step 3) Using rank and the percentile to calculate the approximated value.
	subsetPercentile := subsetRank / float64(h.Counts[i])
	return start + (end-start)*subsetPercentile
}
