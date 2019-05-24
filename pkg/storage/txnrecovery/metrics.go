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
