// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
)

var (
	metaRangeFeedCatchUpScanNanos = metric.Metadata{
		Name:        "kv.rangefeed.catchup_scan_nanos",
		Help:        "Time spent in RangeFeed catchup scan",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRangeFeedOverBudgetEvents = metric.Metadata{
		Name:        "kv.rangefeed.over_budget_events",
		Help:        "Number of RangeFeed events that were sent through despite each exceeding whole range budget",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeFeedOverBudgetAllocation = metric.Metadata{
		Name:        "kv.rangefeed.allocated_over_budget",
		Help:        "Amount of bytes currently used by RangeFeeds for sending single events exceeding whole range budget",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeFeedExhausted = metric.Metadata{
		Name:        "kv.rangefeed.budget_allocation_failed",
		Help:        "Number of times RangeFeed failed because memory budget was exceeded",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics are for production monitoring of RangeFeeds.
type Metrics struct {
	RangeFeedCatchUpScanNanos     *metric.Counter
	RangeFeedOverBudgetEvents     *metric.Counter
	RangeFeedOverBudgetAllocation *metric.Gauge
	RangeFeedBudgetExhausted      *metric.Counter

	RangeFeedSlowClosedTimestampLogN  log.EveryN
	RangeFeedSlowClosedTimestampNudge singleflight.Group
	// RangeFeedSlowClosedTimestampNudgeSem bounds the amount of work that can be
	// spun up on behalf of the RangeFeed nudger. We don't expect to hit this
	// limit, but it's here to limit the effect on stability in case something
	// unexpected happens.
	RangeFeedSlowClosedTimestampNudgeSem chan struct{}
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// NewMetrics makes the metrics for RangeFeeds monitoring.
func NewMetrics() *Metrics {
	return &Metrics{
		RangeFeedCatchUpScanNanos:            metric.NewCounter(metaRangeFeedCatchUpScanNanos),
		RangeFeedOverBudgetEvents:            metric.NewCounter(metaRangeFeedOverBudgetEvents),
		RangeFeedOverBudgetAllocation:        metric.NewGauge(metaRangeFeedOverBudgetAllocation),
		RangeFeedBudgetExhausted:             metric.NewCounter(metaRangeFeedExhausted),
		RangeFeedSlowClosedTimestampLogN:     log.Every(5 * time.Second),
		RangeFeedSlowClosedTimestampNudgeSem: make(chan struct{}, 1024),
	}
}

// FeedBudgetPoolMetrics holds metrics for RangeFeed budgets for the purpose
// or registration in a metric registry.
type FeedBudgetPoolMetrics struct {
	SystemMaxBytesHist  *metric.Histogram
	SystemCurBytesCount *metric.Gauge
	SharedMaxBytesHist  *metric.Histogram
	SharedCurBytesCount *metric.Gauge
}

// MetricStruct implements metrics.Struct interface.
func (FeedBudgetPoolMetrics) MetricStruct() {}

// See pkg/sql/mem_metrics.go
// log10int64times1000 = log10(math.MaxInt64) * 1000, rounded up somewhat
const log10int64times1000 = 19 * 1000

// NewFeedBudgetMetrics creates new metrics for RangeFeed budgets.
func NewFeedBudgetMetrics(histogramWindow time.Duration) *FeedBudgetPoolMetrics {
	makeMemMetricMetadata := func(name, help string) metric.Metadata {
		return metric.Metadata{
			Name:        "kv.rangefeed.mem_" + name,
			Help:        help,
			Measurement: "Memory",
			Unit:        metric.Unit_BYTES,
		}
	}

	return &FeedBudgetPoolMetrics{
		SystemMaxBytesHist: metric.NewHistogram(makeMemMetricMetadata("system_max",
			"Max memory usage by rangefeeds on system ranges"), histogramWindow, log10int64times1000, 3),
		SystemCurBytesCount: metric.NewGauge(makeMemMetricMetadata("system_cur",
			"Current memory usage by rangefeeds on system ranges")),
		SharedMaxBytesHist: metric.NewHistogram(makeMemMetricMetadata("shared_max",
			"Max memory usage by rangefeeds"), histogramWindow, log10int64times1000, 3),
		SharedCurBytesCount: metric.NewGauge(makeMemMetricMetadata("shared_cur",
			"Current memory usage by rangefeeds")),
	}
}
