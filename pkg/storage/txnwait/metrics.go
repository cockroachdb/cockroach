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

package txnwait

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// Metrics contains all the txnqueue related metrics.
type Metrics struct {
	PusheeWaiting  *metric.Gauge
	PusherWaiting  *metric.Gauge
	QueryWaiting   *metric.Gauge
	PusherSlow     *metric.Gauge
	PusherWaitTime *metric.Histogram
	QueryWaitTime  *metric.Histogram
	DeadlocksTotal *metric.Counter
}

// NewMetrics creates a new Metrics instance with all related metric fields.
func NewMetrics(histogramWindowInterval time.Duration) *Metrics {
	return &Metrics{
		PusheeWaiting: metric.NewGauge(
			metric.Metadata{
				Name:        "txnwaitqueue.pushee.waiting",
				Help:        "Number of pushees on the txn wait queue",
				Measurement: "Waiting Pushees",
				Unit:        metric.Unit_COUNT,
			},
		),

		PusherWaiting: metric.NewGauge(
			metric.Metadata{
				Name:        "txnwaitqueue.pusher.waiting",
				Help:        "Number of pushers on the txn wait queue",
				Measurement: "Waiting Pushers",
				Unit:        metric.Unit_COUNT,
			},
		),

		QueryWaiting: metric.NewGauge(
			metric.Metadata{
				Name:        "txnwaitqueue.query.waiting",
				Help:        "Number of transaction status queries waiting for an updated transaction record",
				Measurement: "Waiting Queries",
				Unit:        metric.Unit_COUNT,
			},
		),

		PusherSlow: metric.NewGauge(
			metric.Metadata{
				Name:        "txnwaitqueue.pusher.slow",
				Help:        "The total number of cases where a pusher waited more than the excessive wait threshold",
				Measurement: "Slow Pushers",
				Unit:        metric.Unit_COUNT,
			},
		),

		PusherWaitTime: metric.NewHistogram(
			metric.Metadata{
				Name:        "txnwaitqueue.pusher.wait_time",
				Help:        "Histogram of durations spent in queue by pushers",
				Measurement: "Pusher wait time",
				Unit:        metric.Unit_NANOSECONDS,
			},
			histogramWindowInterval,
			time.Hour.Nanoseconds(),
			1,
		),

		QueryWaitTime: metric.NewHistogram(
			metric.Metadata{
				Name:        "txnwaitqueue.query.wait_time",
				Help:        "Histogram of durations spent in queue by queries",
				Measurement: "Query wait time",
				Unit:        metric.Unit_NANOSECONDS,
			},
			histogramWindowInterval,
			time.Hour.Nanoseconds(),
			1,
		),

		DeadlocksTotal: metric.NewCounter(
			metric.Metadata{
				Name:        "txnwaitqueue.deadlocks_total",
				Help:        "Number of deadlocks detected by the txn wait queue",
				Measurement: "Deadlocks",
				Unit:        metric.Unit_COUNT,
			},
		),
	}
}
