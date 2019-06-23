// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package txnrecovery

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Metrics holds all metrics relating to a transaction recovery Manager.
type Metrics struct {
	AttemptsPending      *metric.Gauge
	Attempts             *metric.Counter
	SuccessesAsCommitted *metric.Counter
	SuccessesAsAborted   *metric.Counter
	SuccessesAsPending   *metric.Counter
	Failures             *metric.Counter
}

// makeMetrics creates a new Metrics instance with all related metric fields.
func makeMetrics() Metrics {
	return Metrics{
		AttemptsPending: metric.NewGauge(
			metric.Metadata{
				Name:        "txnrecovery.attempts.pending",
				Help:        "Number of transaction recovery attempts currently in-flight",
				Measurement: "Recovery Attempts",
				Unit:        metric.Unit_COUNT,
			},
		),

		Attempts: metric.NewCounter(
			metric.Metadata{
				Name:        "txnrecovery.attempts.total",
				Help:        "Number of transaction recovery attempts executed",
				Measurement: "Recovery Attempts",
				Unit:        metric.Unit_COUNT,
			},
		),

		SuccessesAsCommitted: metric.NewCounter(
			metric.Metadata{
				Name:        "txnrecovery.successes.committed",
				Help:        "Number of transaction recovery attempts that committed a transaction",
				Measurement: "Recovery Attempts",
				Unit:        metric.Unit_COUNT,
			},
		),

		SuccessesAsAborted: metric.NewCounter(
			metric.Metadata{
				Name:        "txnrecovery.successes.aborted",
				Help:        "Number of transaction recovery attempts that aborted a transaction",
				Measurement: "Recovery Attempts",
				Unit:        metric.Unit_COUNT,
			},
		),

		SuccessesAsPending: metric.NewCounter(
			metric.Metadata{
				Name:        "txnrecovery.successes.pending",
				Help:        "Number of transaction recovery attempts that left a transaction pending",
				Measurement: "Recovery Attempts",
				Unit:        metric.Unit_COUNT,
			},
		),

		Failures: metric.NewCounter(
			metric.Metadata{
				Name:        "txnrecovery.failures",
				Help:        "Number of transaction recovery attempts that failed",
				Measurement: "Recovery Attempts",
				Unit:        metric.Unit_COUNT,
			},
		),
	}
}
