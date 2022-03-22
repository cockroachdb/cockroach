// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package txnidcache

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Metrics is a struct that include all metrics related to txn id cache.
type Metrics struct {
	CacheMissCounter *metric.Counter
	CacheReadCounter *metric.Counter
}

var _ metric.Struct = Metrics{}

// MetricStruct implements the metric.Struct interface.
func (Metrics) MetricStruct() {}

// NewMetrics returns a new instance of Metrics.
func NewMetrics() Metrics {
	return Metrics{
		CacheMissCounter: metric.NewCounter(metric.Metadata{
			Name:        "sql.contention.txn_id_cache.miss",
			Help:        "Number of cache misses",
			Measurement: "Cache miss",
			Unit:        metric.Unit_COUNT,
		}),
		CacheReadCounter: metric.NewCounter(metric.Metadata{
			Name:        "sql.contention.txn_id_cache.read",
			Help:        "Number of cache read",
			Measurement: "Cache read",
			Unit:        metric.Unit_COUNT,
		}),
	}
}
