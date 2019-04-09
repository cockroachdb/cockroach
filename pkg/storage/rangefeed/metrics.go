// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rangefeed

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var (
	metaRangeFeedCatchupScanNanos = metric.Metadata{
		Name:        "kv.rangefeed.catchup_scan_nanos",
		Help:        "Time spent in RangeFeed catchup scan",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRangeFeedClosedTimestampBehind = metric.Metadata{
		Name:        "kv.rangefeed.closed_timestamp_behind",
		Help:        "Number of times RangeFeed received an old closed timestamp",
		Measurement: "Slow Closed Timestamps",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics are for production monitoring of RangeFeeds.
type Metrics struct {
	RangeFeedCatchupScanNanos      *metric.Counter
	RangeFeedClosedTimestampBehind *metric.Counter

	RangeFeedClosedTimestampBehindLogN log.EveryN
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// NewMetrics makes the metrics for RangeFeeds monitoring.
func NewMetrics() *Metrics {
	return &Metrics{
		RangeFeedCatchupScanNanos:      metric.NewCounter(metaRangeFeedCatchupScanNanos),
		RangeFeedClosedTimestampBehind: metric.NewCounter(metaRangeFeedClosedTimestampBehind),

		RangeFeedClosedTimestampBehindLogN: log.Every(5 * time.Second),
	}
}
