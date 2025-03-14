// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	metaSuccessSplits = metric.Metadata{
		Name:        "sql.vecindex.successful_splits",
		Help:        "Total number of vector index partitions split without error",
		Measurement: "Vector Index Splits",
		Unit:        metric.Unit_COUNT,
	}

	metaFixupsAdded = metric.Metadata{
		Name:        "sql.vecindex.fixups_added",
		Help:        "Total number of vector index fixups that have been added to the processor",
		Measurement: "Vector Index Fixups Added",
		Unit:        metric.Unit_COUNT,
	}

	metaFixupsProcessed = metric.Metadata{
		Name:        "sql.vecindex.fixups_processed",
		Help:        "Total number of vector index fixups that have been processed",
		Measurement: "Vector Index Fixups Processed",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics contain useful metrics for building and mantaining vector indexes.
type Metrics struct {
	SuccessSplits   *metric.Counter
	FixupsAdded     *metric.Counter
	FixupsProcessed *metric.Counter
}

var _ metric.Struct = (*Metrics)(nil)

// MetricStruct indicates that Metrics is a metric.Struct.
func (m *Metrics) MetricStruct() {}

func (m *Metrics) Init() {
	m.SuccessSplits = metric.NewCounter(metaSuccessSplits)
	m.FixupsAdded = metric.NewCounter(metaFixupsAdded)
	m.FixupsProcessed = metric.NewCounter(metaFixupsProcessed)
}

func (m *Metrics) IncSuccessSplits() {
	m.SuccessSplits.Inc(1)
}

func (m *Metrics) IncFixupsAdded() {
	m.FixupsAdded.Inc(1)
}

func (m *Metrics) IncFixupsProcessed() {
	m.FixupsProcessed.Inc(1)
}
