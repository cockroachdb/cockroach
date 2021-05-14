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
	metaGCIntentCleanupFailed = metric.Metadata{
		Name:        "intentresolver.gc.intents.failed",
		Help:        "Number of cleanup intent failures during GC",
		Measurement: "Intent Resolutions",
		Unit:        metric.Unit_COUNT,
	}
	metaGCTxnIntentsCleanupFailed = metric.Metadata{
		Name:        "intentresolver.gc.transaction.intents.failed",
		Help:        "Number of intent cleanup failures for local transactions during GC",
		Measurement: "Intent Resolutions",
		Unit:        metric.Unit_COUNT,
	}
	metaConflictingIntentsCleanupFailed = metric.Metadata{
		Name:        "intentresolver.conflicting.intents.failed",
		Help:        "Number of intent resolution failures by conflicting transactions",
		Measurement: "Intent Resolutions",
		Unit:        metric.Unit_COUNT,
	}
	metaFinalizedTxnCleanupFailed = metric.Metadata{
		Name:        "intentresolver.finalized.txns.failed",
		Help:        "Number of finalized transaction resolution failures",
		Measurement: "Intent Resolutions",
		Unit:        metric.Unit_COUNT,
	}
	metaFinalizedTxnCleanupTimedOut = metric.Metadata{
		Name:        "intentresolver.finalized.txns.timedout",
		Help:        "Number of finalized transaction resolution timeouts",
		Measurement: "Intent Resolutions",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics contains the metrics for the IntentResolver.
type Metrics struct {
	IntentResolverAsyncThrottled *metric.Counter

	// Counters tracking intent cleanup.
	GCIntentCleanupFailed           *metric.Counter
	GCTxnIntentsCleanupFailed       *metric.Counter
	ConflictingIntentsCleanupFailed *metric.Counter
	FinalizedTxnCleanupFailed       *metric.Counter
	FinalizedTxnCleanupTimedOut     *metric.Counter
}

func makeMetrics() Metrics {
	// Intent resolver metrics.
	return Metrics{
		IntentResolverAsyncThrottled:    metric.NewCounter(metaIntentResolverAsyncThrottled),
		GCIntentCleanupFailed:           metric.NewCounter(metaGCIntentCleanupFailed),
		GCTxnIntentsCleanupFailed:       metric.NewCounter(metaGCTxnIntentsCleanupFailed),
		ConflictingIntentsCleanupFailed: metric.NewCounter(metaConflictingIntentsCleanupFailed),
		FinalizedTxnCleanupFailed:       metric.NewCounter(metaFinalizedTxnCleanupFailed),
		FinalizedTxnCleanupTimedOut:     metric.NewCounter(metaFinalizedTxnCleanupTimedOut),
	}
}
