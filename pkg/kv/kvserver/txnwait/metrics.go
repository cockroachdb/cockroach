// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	PusherWaitTime metric.IHistogram
	QueryWaitTime  metric.IHistogram
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

		PusherWaitTime: metric.NewHistogram(metric.HistogramOptions{
			Metadata: metric.Metadata{
				Name:        "txnwaitqueue.pusher.wait_time",
				Help:        "Histogram of durations spent in queue by pushers",
				Measurement: "Pusher wait time",
				Unit:        metric.Unit_NANOSECONDS,
			},
			MaxVal:       time.Hour.Nanoseconds(),
			SigFigs:      1,
			Duration:     histogramWindowInterval,
			BucketConfig: metric.LongRunning60mLatencyBuckets,
		}),

		QueryWaitTime: metric.NewHistogram(metric.HistogramOptions{
			Metadata: metric.Metadata{
				Name:        "txnwaitqueue.query.wait_time",
				Help:        "Histogram of durations spent in queue by queries",
				Measurement: "Query wait time",
				Unit:        metric.Unit_NANOSECONDS,
			},
			MaxVal:       time.Hour.Nanoseconds(),
			SigFigs:      1,
			Duration:     histogramWindowInterval,
			BucketConfig: metric.LongRunning60mLatencyBuckets,
		}),

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
