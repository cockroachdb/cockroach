// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvfeed

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	metaRangefeedValues = metric.Metadata{
		Name:        "changefeed.kvfeed.rangefeed_values",
		Help:        "Number of RangeFeedValue events received by the kvfeed from rangefeeds",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
		Category:    metric.Metadata_CHANGEFEEDS,
	}
	metaRangefeedCheckpoints = metric.Metadata{
		Name:        "changefeed.kvfeed.rangefeed_checkpoints",
		Help:        "Number of RangeFeedCheckpoint events received by the kvfeed from rangefeeds",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
		Category:    metric.Metadata_CHANGEFEEDS,
	}
	metaRangefeedSkippedCheckpoints = metric.Metadata{
		Name:        "changefeed.kvfeed.rangefeed_skipped_checkpoints",
		Help:        "Number of RangeFeedCheckpoint events skipped due to being below the frontier",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
		Category:    metric.Metadata_CHANGEFEEDS,
	}
	metaRangefeedDeleteRanges = metric.Metadata{
		Name:        "changefeed.kvfeed.rangefeed_delete_ranges",
		Help:        "Number of RangeFeedDeleteRange events received by the kvfeed from rangefeeds",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
		Category:    metric.Metadata_CHANGEFEEDS,
	}
)

// Metrics are for production monitoring of the kvfeed.
type Metrics struct {
	RangefeedValues             *metric.Counter
	RangefeedCheckpoints        *metric.Counter
	RangefeedSkippedCheckpoints *metric.Counter
	RangefeedDeleteRanges       *metric.Counter
}

// MetricStruct implements the metric.Struct interface.
func (Metrics) MetricStruct() {}

var _ metric.Struct = Metrics{}

// MakeMetrics creates a new Metrics instance for the kvfeed.
func MakeMetrics() Metrics {
	return Metrics{
		RangefeedValues:             metric.NewCounter(metaRangefeedValues),
		RangefeedCheckpoints:        metric.NewCounter(metaRangefeedCheckpoints),
		RangefeedSkippedCheckpoints: metric.NewCounter(metaRangefeedSkippedCheckpoints),
		RangefeedDeleteRanges:       metric.NewCounter(metaRangefeedDeleteRanges),
	}
}
