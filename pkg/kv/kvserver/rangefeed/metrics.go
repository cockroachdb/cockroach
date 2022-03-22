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
	metaRangeFeedExhausted = metric.Metadata{
		Name:        "kv.rangefeed.budget_allocation_failed",
		Help:        "Number of times RangeFeed failed because memory budget was exceeded",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeFeedBudgetBlocked = metric.Metadata{
		Name:        "kv.rangefeed.budget_allocation_blocked",
		Help:        "Number of times RangeFeed waited for budget availability",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics are for production monitoring of RangeFeeds.
type Metrics struct {
	RangeFeedCatchUpScanNanos *metric.Counter
	RangeFeedBudgetExhausted  *metric.Counter
	RangeFeedBudgetBlocked    *metric.Counter

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
		RangeFeedBudgetExhausted:             metric.NewCounter(metaRangeFeedExhausted),
		RangeFeedBudgetBlocked:               metric.NewCounter(metaRangeFeedBudgetBlocked),
		RangeFeedSlowClosedTimestampLogN:     log.Every(5 * time.Second),
		RangeFeedSlowClosedTimestampNudgeSem: make(chan struct{}, 1024),
	}
}

// FeedBudgetPoolMetrics holds metrics for RangeFeed budgets for the purpose
// or registration in a metric registry.
type FeedBudgetPoolMetrics struct {
	SystemBytesCount *metric.Gauge
	SharedBytesCount *metric.Gauge
}

// MetricStruct implements metrics.Struct interface.
func (FeedBudgetPoolMetrics) MetricStruct() {}

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
		SystemBytesCount: metric.NewGauge(makeMemMetricMetadata("system",
			"Memory usage by rangefeeds on system ranges")),
		SharedBytesCount: metric.NewGauge(makeMemMetricMetadata("shared",
			"Memory usage by rangefeeds")),
	}
}
