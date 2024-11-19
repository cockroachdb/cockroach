// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdcutils

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// Throttler is a changefeed IO throttler.
type Throttler struct {
	name           string
	messageLimiter *quotapool.RateLimiter
	byteLimiter    *quotapool.RateLimiter
	flushLimiter   *quotapool.RateLimiter
	metrics        *Metrics
}

// AcquireMessageQuota acquires quota for a message with the specified size.
// Blocks until such quota is available.
func (t *Throttler) AcquireMessageQuota(ctx context.Context, sz int) error {
	if t.messageLimiter.AdmitN(1) && t.byteLimiter.AdmitN(int64(sz)) {
		return nil
	}

	// Slow case.
	var span *tracing.Span
	ctx, span = tracing.ChildSpan(ctx, fmt.Sprintf("quota-wait-%s", t.name))
	defer span.Finish()

	if err := waitQuota(ctx, 1, t.messageLimiter, t.metrics.MessagesPushbackNanos); err != nil {
		return err
	}
	return waitQuota(ctx, int64(sz), t.byteLimiter, t.metrics.BytesPushbackNanos)
}

// AcquireFlushQuota acquires quota for a message with the specified size.
// Blocks until such quota is available.
func (t *Throttler) AcquireFlushQuota(ctx context.Context) error {
	if t.flushLimiter.AdmitN(1) {
		return nil
	}

	// Slow case.
	var span *tracing.Span
	ctx, span = tracing.ChildSpan(ctx, fmt.Sprintf("quota-wait-flush-%s", t.name))
	defer span.Finish()
	return waitQuota(ctx, 1, t.flushLimiter, t.metrics.FlushPushbackNanos)
}

func (t *Throttler) updateConfig(config changefeedbase.SinkThrottleConfig) {
	setLimits := func(rl *quotapool.RateLimiter, rate, burst float64) {
		// set rateBudget to unlimited if rate is 0.
		rateBudget := quotapool.Inf()
		if rate > 0 {
			rateBudget = quotapool.Limit(rate)
		}
		// set burstBudget to be at least the rate.
		burstBudget := int64(burst)
		if burst < rate {
			burstBudget = int64(rate)
		}
		rl.UpdateLimit(rateBudget, burstBudget)
	}

	setLimits(t.messageLimiter, config.MessageRate, config.MessageBurst)
	setLimits(t.byteLimiter, config.ByteRate, config.ByteBurst)
	setLimits(t.flushLimiter, config.FlushRate, config.FlushBurst)
}

// NewThrottler creates a new throttler with the specified configuration.
func NewThrottler(name string, config changefeedbase.SinkThrottleConfig, m *Metrics) *Throttler {
	logSlowAcquisition := quotapool.OnSlowAcquisition(500*time.Millisecond, quotapool.LogSlowAcquisition)
	t := &Throttler{
		name: name,
		messageLimiter: quotapool.NewRateLimiter(
			fmt.Sprintf("%s-messages", name), 0, 0, logSlowAcquisition,
		),
		byteLimiter: quotapool.NewRateLimiter(
			fmt.Sprintf("%s-bytes", name), 0, 0, logSlowAcquisition,
		),
		flushLimiter: quotapool.NewRateLimiter(
			fmt.Sprintf("%s-flushes", name), 0, 0, logSlowAcquisition,
		),
		metrics: m,
	}
	t.updateConfig(config)
	return t
}

var nodeSinkThrottle = struct {
	sync.Once
	*Throttler
}{}

// NodeLevelThrottler returns node level Throttler for changefeeds.
func NodeLevelThrottler(sv *settings.Values, metrics *Metrics) *Throttler {
	getConfig := func() (config changefeedbase.SinkThrottleConfig) {
		configStr := changefeedbase.NodeSinkThrottleConfig.Get(sv)
		if configStr != "" {
			if err := json.Unmarshal([]byte(configStr), &config); err != nil {
				log.Errorf(context.Background(),
					"failed to parse node throttle config %q: err=%v; throttling disabled", configStr, err)
			}
		}
		return
	}

	// Initialize node level throttler once.
	nodeSinkThrottle.Do(func() {
		if nodeSinkThrottle.Throttler != nil {
			panic("unexpected state")
		}
		nodeSinkThrottle.Throttler = NewThrottler("cf.node.throttle", getConfig(), metrics)
		// Update node throttler configs when settings change.
		changefeedbase.NodeSinkThrottleConfig.SetOnChange(sv, func(ctx context.Context) {
			nodeSinkThrottle.Throttler.updateConfig(getConfig())
		})
	})

	return nodeSinkThrottle.Throttler
}

// Metrics is a metric.Struct for kvfeed metrics.
type Metrics struct {
	BytesPushbackNanos    *metric.Counter
	MessagesPushbackNanos *metric.Counter
	FlushPushbackNanos    *metric.Counter
}

// MakeMetrics constructs a Metrics struct with the provided histogram window.
func MakeMetrics(histogramWindow time.Duration) Metrics {
	makeMetric := func(n string) metric.Metadata {
		return metric.Metadata{
			Name:        fmt.Sprintf("changefeed.%s.messages_pushback_nanos", n),
			Help:        fmt.Sprintf("Total time spent throttled for %s quota", n),
			Measurement: "Nanoseconds",
			Unit:        metric.Unit_NANOSECONDS,
		}
	}

	return Metrics{
		BytesPushbackNanos:    metric.NewCounter(makeMetric("bytes")),
		MessagesPushbackNanos: metric.NewCounter(makeMetric("messages")),
		FlushPushbackNanos:    metric.NewCounter(makeMetric("flush")),
	}
}

var _ metric.Struct = (*Metrics)(nil)

// MetricStruct makes Metrics a metric.Struct.
func (m Metrics) MetricStruct() {}

func waitQuota(
	ctx context.Context, n int64, limit *quotapool.RateLimiter, c *metric.Counter,
) error {
	start := timeutil.Now()
	defer func() {
		c.Inc(int64(timeutil.Since(start)))
	}()
	return limit.WaitN(ctx, n)
}
