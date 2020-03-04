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
	metaRangeFeedCatchupScanNanos = metric.Metadata{
		Name:        "kv.rangefeed.catchup_scan_nanos",
		Help:        "Time spent in RangeFeed catchup scan",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// Metrics are for production monitoring of RangeFeeds.
type Metrics struct {
	RangeFeedCatchupScanNanos *metric.Counter

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
		RangeFeedCatchupScanNanos:            metric.NewCounter(metaRangeFeedCatchupScanNanos),
		RangeFeedSlowClosedTimestampLogN:     log.Every(5 * time.Second),
		RangeFeedSlowClosedTimestampNudgeSem: make(chan struct{}, 1024),
	}
}
