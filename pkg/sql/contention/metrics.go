// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contention

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Metrics is a structs that include all metrics related to contention
// subsystem.
type Metrics struct {
	TxnIDCacheSize *metric.Gauge
}

var _ metric.Struct = Metrics{}

// MetricStruct implements the metric.Struct interface.
func (Metrics) MetricStruct() {}

// NewMetrics returns a new instance of Metrics.
func NewMetrics() Metrics {
	return Metrics{
		TxnIDCacheSize: metric.NewGauge(metric.Metadata{
			Name:        "sql.contention.txn_id_cache.size",
			Help:        "Current memory usage for TxnID Cache",
			Measurement: "Memory",
			Unit:        metric.Unit_BYTES,
		}),
	}
}
