// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Metrics contains concurrency related metrics.
type Metrics struct {
	LockWaitQueueWaiters *metric.Gauge
}

// NewMetrics creates a new Metrics instance with all related metric fields.
func NewMetrics() *Metrics {
	return &Metrics{
		LockWaitQueueWaiters: metric.NewGauge(
			metric.Metadata{
				Name:        "kv.lock_table.wait_queue.waiting",
				Help:        "Number of requests waiting in lock wait-queues in the lock table",
				Measurement: "Waiting Requests",
				Unit:        metric.Unit_COUNT,
			},
		),
	}
}
