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
	metaTransactionRecordCleanupDropped = metric.Metadata{
		Name:        "intentresolver.txnrecord.dropped",
		Help:        "Number of txn record attempts that were dropped by the intent resolver due top throttling",
		Measurement: "Intent Resolutions",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics contains the metrics for the IntentResolver.
type Metrics struct {
	// Intent resolver metrics.
	IntentResolverAsyncThrottled *metric.Counter
	TxnRecordCleanupDropped      *metric.Counter
}

func makeMetrics() Metrics {
	// Intent resolver metrics.
	return Metrics{
		IntentResolverAsyncThrottled: metric.NewCounter(metaIntentResolverAsyncThrottled),
		TxnRecordCleanupDropped:      metric.NewCounter(metaTransactionRecordCleanupDropped),
	}
}
