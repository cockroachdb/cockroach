// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ptreconcile

import (
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// Metrics encapsulates the metrics exported by the Reconciler.
type Metrics struct {
	ReconcilationRuns    *metric.Counter
	RecordsProcessed     *metric.Counter
	RecordsRemoved       *metric.Counter
	ReconciliationErrors *metric.Counter
}

func makeMetrics() Metrics {
	return Metrics{
		ReconcilationRuns:    metric.NewCounter(metaReconciliationRuns),
		RecordsProcessed:     metric.NewCounter(metaRecordsProcessed),
		RecordsRemoved:       metric.NewCounter(metaRecordsRemoved),
		ReconciliationErrors: metric.NewCounter(metaReconciliationErrors),
	}
}

var _ metric.Struct = (*Metrics)(nil)

// MetricStruct makes Metrics a metric.Struct.
func (m *Metrics) MetricStruct() {}

var (
	metaReconciliationRuns = metric.Metadata{
		Name:        "kv.protectedts.reconciliation.num_runs",
		Help:        "number of successful reconciliation runs on this node",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaRecordsProcessed = metric.Metadata{
		Name:        "kv.protectedts.reconciliation.records_processed",
		Help:        "number of records processed without error during reconciliation on this node",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaRecordsRemoved = metric.Metadata{
		Name:        "kv.protectedts.reconciliation.records_removed",
		Help:        "number of records removed during reconciliation runs on this node",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaReconciliationErrors = metric.Metadata{
		Name:        "kv.protectedts.reconciliation.errors",
		Help:        "number of errors encountered during reconciliation runs on this node",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
)
