// Copyright 2021 The Cockroach Authors.
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

// Metrics is a structs that include all metrics related to contention
// subsystem.
type Metrics struct {
	DiscardedResolvedTxnIDCount *metric.Counter
	EvictedTxnIDCacheCount      *metric.Counter
}

var _ metric.Struct = Metrics{}

// MetricStruct implements the metric.Struct interface.
func (Metrics) MetricStruct() {}

// NewMetrics returns a new instance of Metrics.
func NewMetrics() Metrics {
	return Metrics{
		DiscardedResolvedTxnIDCount: metric.NewCounter(metric.Metadata{
			Name:        "sql.contention.txn_id_cache.discarded_count",
			Help:        "Number of discarded resolved transaction IDs",
			Measurement: "Discarded Resolved Transaction IDs",
			Unit:        metric.Unit_COUNT,
		}),
		EvictedTxnIDCacheCount: metric.NewCounter(metric.Metadata{
			Name:        "sql.contention.txn_id_cache.evicted_count",
			Help:        "Number of evicted resolved transaction IDs",
			Measurement: "Evicted Resolved Transaction IDs",
			Unit:        metric.Unit_COUNT,
		}),
	}
}
