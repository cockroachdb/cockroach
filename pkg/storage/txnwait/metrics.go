// Copyright 2017 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"time"
)

// Only one set of metrics for all queues.
type Metrics struct {
	Pushees *metric.Gauge
	Pushers *metric.Gauge
	Queries *metric.Gauge
	PusherExcessiveWaitTotal *metric.Counter
	PusherWaitTime *metric.Histogram
	QueryWaitTime *metric.Histogram
	DeadlocksTotal *metric.Counter
}

func NewMetrics(histogramWindowInterval time.Duration) *Metrics {
	return &Metrics{
		Pushees: metric.NewGauge(
			metric.Metadata{
				Name:        "txn_wait_queue.pushees",
				Help:        "Number of pushees on the txn wait queue",
				Measurement: "Pushees",
				Unit:        metric.Unit_COUNT,
			},
		),

		Pushers: metric.NewGauge(
			metric.Metadata{
				Name:        "txn_wait_queue.pushers",
				Help:        "Number of pushers on the txn wait queue",
				Measurement: "Pushers",
				Unit:        metric.Unit_COUNT,
			},
		),

		Queries: metric.NewGauge(
			metric.Metadata{
				Name:        "txn_wait_queue.queries",
				Help:        "Number of queries on the txn wait queue",
				Measurement: "Queries",
				Unit:        metric.Unit_COUNT,
			},
		),

		PusherExcessiveWaitTotal: metric.NewCounter(
			metric.Metadata{
				Name:        "txn_wait_queue.pusher.excessive_wait_total",
				Help:        "The total number of cases where a pusher waited more than the excessive wait threshold",
				Measurement: "Pusher excessive wait total",
				Unit:        metric.Unit_COUNT,
			},
		),

		PusherWaitTime: metric.NewHistogram(
			metric.Metadata{
				Name:        "txn_wait_queue.pusher.wait_time",
				Help:        "The breakdown of the pusher's wait time when they are removed from the queue",
				Measurement: "Pusher wait time",
				Unit:        metric.Unit_NANOSECONDS,
			},
			histogramWindowInterval,
			time.Hour.Nanoseconds(),
			1,
		),

		QueryWaitTime: metric.NewHistogram(
			metric.Metadata{
				Name:        "txn_wait_queue.query.wait_time",
				Help:        "The breakdown of the query's wait time when they are removed from the queue",
				Measurement: "Query wait time",
				Unit:        metric.Unit_NANOSECONDS,
			},
			histogramWindowInterval,
			time.Hour.Nanoseconds(),
			1,
		),

		DeadlocksTotal: metric.NewCounter(
			metric.Metadata{
				Name:        "txn_wait_queue.deadlocks_total",
				Help:        "Total number of deadlocks",
				Measurement: "Deadlocks",
				Unit:        metric.Unit_COUNT,
			},
		),
	}
}
