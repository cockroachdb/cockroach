// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// BenchmarkBatchingSinkLingerTradeoff measures batchingSink's
// min_flush_frequency tradeoff across a low-rate and a bursty workload.
// Baseline for the no-linger sink rework (#170198). The signal is in the
// custom metrics from b.ReportMetric, not Go's ns/op — the emit loop
// sleeps inside the timed region.
//
// We assume every sink's flush cost has a fixed base plus a small
// per-event term; without that cost the throughput half of the tradeoff
// doesn't appear, because the sink keeps up with any offered rate.
func BenchmarkBatchingSinkLingerTradeoff(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	// Per-flush cost is a fixed base plus a small per-event term — a
	// flush of 1000 events pays the base once, while 1000 single-event
	// flushes pay it 1000 times.
	const flushBase = 100 * time.Millisecond
	const flushPerEvent = 50 * time.Microsecond

	workloads := []struct {
		name      string
		emitGap   time.Duration
		burstSize int
		burstGap  time.Duration
	}{
		{name: "low_rate", emitGap: 50 * time.Millisecond},
		{name: "bursty", burstSize: 4000, burstGap: 50 * time.Millisecond},
	}
	minFreqs := []time.Duration{1 * time.Millisecond, 100 * time.Millisecond}

	for _, w := range workloads {
		for _, freq := range minFreqs {
			name := fmt.Sprintf("%s/min_freq=%s", w.name, freq)
			b.Run(name, func(b *testing.B) {
				runLingerWorkload(b, freq, flushBase, flushPerEvent,
					w.emitGap, w.burstSize, w.burstGap)
			})
		}
	}
}

func runLingerWorkload(
	b *testing.B,
	minFlushFrequency, flushBase, flushPerEvent, emitGap time.Duration,
	burstSize int,
	burstGap time.Duration,
) {
	ctx := context.Background()
	stub := &stubSinkClient{flushBase: flushBase, flushPerEvent: flushPerEvent}
	sink := makeBatchingSink(
		ctx,
		sinkTypeWebhook,
		stub,
		minFlushFrequency,
		retry.Options{},
		// Eight workers is mid-range for production, which uses
		// GOMAXPROCS clamped to [1, 32].
		8, /* numWorkers */
		nil /* topicNamer */, func() *admission.Pacer { return nil },
		timeutil.DefaultTimeSource{},
		nilMetricsRecorderBuilder(true),
		cluster.MakeTestingClusterSettings(),
	)
	defer func() { _ = sink.Close() }()
	topic := topic("bench_topic")

	b.ResetTimer()
	start := timeutil.Now()
	for i := 0; i < b.N; i++ {
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, uint64(timeutil.Now().UnixNano()))
		require.NoError(b, sink.EmitRow(ctx, topic,
			[]byte(fmt.Sprintf("k%d", i)), value,
			nil /* csvColumnHeader */, zeroTS, zeroTS, zeroAlloc, nil /* headers */))

		if emitGap > 0 {
			time.Sleep(emitGap)
		}
		if burstSize > 0 && (i+1)%burstSize == 0 {
			time.Sleep(burstGap)
		}
	}
	require.NoError(b, sink.Flush(ctx))
	elapsed := timeutil.Since(start)
	b.StopTimer()

	events, batches, latencies := stub.snapshot()
	p50 := percentile(latencies, 0.50)
	p99 := percentile(latencies, 0.99)
	var avgBatch float64
	if batches > 0 {
		avgBatch = float64(events) / float64(batches)
	}

	b.ReportMetric(float64(events)/elapsed.Seconds(), "events/sec")
	b.ReportMetric(avgBatch, "events/batch")
	b.ReportMetric(float64(p50.Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(p99.Nanoseconds()), "p99-ns")
}

type stubSinkClient struct {
	flushBase     time.Duration
	flushPerEvent time.Duration

	mu struct {
		syncutil.Mutex
		latencies  []time.Duration
		batchSizes []int
	}
}

func (c *stubSinkClient) MakeBatchBuffer(string) BatchBuffer {
	return &stubBatchBuffer{}
}

func (c *stubSinkClient) FlushResolvedPayload(
	context.Context, []byte, func(func(topic string) error) error, retry.Options,
) error {
	return nil
}

func (c *stubSinkClient) Flush(_ context.Context, payload SinkPayload) error {
	emitTimes := payload.([]time.Time)
	time.Sleep(c.flushBase + c.flushPerEvent*time.Duration(len(emitTimes)))
	now := timeutil.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range emitTimes {
		c.mu.latencies = append(c.mu.latencies, now.Sub(t))
	}
	c.mu.batchSizes = append(c.mu.batchSizes, len(emitTimes))
	return nil
}

func (c *stubSinkClient) Close() error                            { return nil }
func (c *stubSinkClient) CheckConnection(_ context.Context) error { return nil }

func (c *stubSinkClient) snapshot() (events, batches int, latencies []time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.mu.batchSizes {
		events += s
	}
	return events, len(c.mu.batchSizes), slices.Clone(c.mu.latencies)
}

type stubBatchBuffer struct {
	emitTimes []time.Time
}

// Append decodes the producer-embedded emit timestamp (first 8 bytes,
// big-endian nanos) so recorded latency includes the batchingSink's
// internal queueing, not just worker-side time.
func (b *stubBatchBuffer) Append(_ context.Context, _, value []byte, _ attributes) {
	nanos := binary.BigEndian.Uint64(value[:8])
	b.emitTimes = append(b.emitTimes, time.Unix(0, int64(nanos)))
}

// ShouldFlush returns false so min_flush_frequency is the sole flush trigger.
func (b *stubBatchBuffer) ShouldFlush() bool { return false }

func (b *stubBatchBuffer) Close() (SinkPayload, error) {
	return b.emitTimes, nil
}

func percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	sorted := slices.Clone(latencies)
	slices.Sort(sorted)
	return sorted[int(float64(len(sorted)-1)*p)]
}

var (
	_ SinkClient  = (*stubSinkClient)(nil)
	_ BatchBuffer = (*stubBatchBuffer)(nil)
)
