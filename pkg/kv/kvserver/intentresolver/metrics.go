// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package intentresolver

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	// Intent resolver metrics.
	metaIntentResolverAsyncThrottled = metric.Metadata{
		Name:        "intentresolver.async.throttled",
		Help:        "Number of intent resolution attempts not run asynchronously due to throttling",
		Measurement: "Intent Resolutions",
		Unit:        metric.Unit_COUNT,
	}
	metaFinalizedTxnCleanupFailed = metric.Metadata{
		Name:        "intentresolver.finalized_txns.failed",
		Help:        "Number of finalized transaction resolution failures",
		Measurement: "Intent Resolutions",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics contains the metrics for the IntentResolver.
type Metrics struct {
	IntentResolverAsyncThrottled *metric.Counter

	// Counters tracking intent cleanup failures.
	FinalizedTxnCleanupFailed *metric.Counter
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

func makeMetrics() Metrics {
	return Metrics{
		IntentResolverAsyncThrottled: metric.NewCounter(metaIntentResolverAsyncThrottled),
		FinalizedTxnCleanupFailed:    metric.NewCounter(metaFinalizedTxnCleanupFailed),
	}
}
