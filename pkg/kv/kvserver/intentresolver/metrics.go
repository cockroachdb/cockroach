// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		Name: "intentresolver.finalized_txns.failed",
		Help: "Number of finalized transaction cleanup failures. Transaction " +
			"cleanup refers to the process of resolving all of a transactions intents " +
			"and then garbage collecting its transaction record.",
		Measurement: "Intent Resolutions",
		Unit:        metric.Unit_COUNT,
	}
	metaIntentCleanupFailed = metric.Metadata{
		Name: "intentresolver.intents.failed",
		Help: "Number of intent resolution failures. The unit of measurement " +
			"is a single intent, so if a batch of intent resolution requests fails, " +
			"the metric will be incremented for each request in the batch.",
		Measurement: "Intent Resolutions",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics contains the metrics for the IntentResolver.
type Metrics struct {
	IntentResolverAsyncThrottled *metric.Counter

	// Counter tracking intent + transaction record cleanup failures.
	FinalizedTxnCleanupFailed *metric.Counter

	// Counter tracking intent cleanup failures.
	IntentResolutionFailed *metric.Counter
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

func makeMetrics() Metrics {
	return Metrics{
		IntentResolverAsyncThrottled: metric.NewCounter(metaIntentResolverAsyncThrottled),
		FinalizedTxnCleanupFailed:    metric.NewCounter(metaFinalizedTxnCleanupFailed),
		IntentResolutionFailed:       metric.NewCounter(metaIntentCleanupFailed),
	}
}
