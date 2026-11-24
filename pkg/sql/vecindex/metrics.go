// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	metaSuccessfulSplits = metric.Metadata{
		Name:        "sql.vecindex.successful_splits",
		Help:        "Total number of vector index partitions split without error",
		Measurement: "Splits",
		Unit:        metric.Unit_COUNT,
	}

	metaPendingSplitsMerges = metric.Metadata{
		Name:        "sql.vecindex.pending_splits_merges",
		Help:        "Total number of vector index splits and merges waiting to be processed",
		Measurement: "Pending Splits/Merges",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics contain useful metrics for building and mantaining vector indexes.
type Metrics struct {
	SuccessfulSplits    *metric.Counter
	PendingSplitsMerges *metric.Gauge
}

var _ metric.Struct = (*Metrics)(nil)

// MetricStruct indicates that Metrics is a metric.Struct.
func (m *Metrics) MetricStruct() {}

func (m *Metrics) Init() {
	m.SuccessfulSplits = metric.NewCounter(metaSuccessfulSplits)
	m.PendingSplitsMerges = metric.NewGauge(metaPendingSplitsMerges)
}

func (m *Metrics) IncSuccessfulSplits() {
	m.SuccessfulSplits.Inc(1)
}

func (m *Metrics) SetPendingSplitsMerges(count int) {
	m.PendingSplitsMerges.Update(int64(count))
}
