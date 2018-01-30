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

package distsqlrun

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// DistSQLMetrics contains pointers to the metrics for
// monitoring DistSQL processing.
type DistSQLMetrics struct {
	QueriesActive *metric.Gauge
	QueriesTotal  *metric.Counter
	FlowsActive   *metric.Gauge
	FlowsTotal    *metric.Counter
	MaxBytesHist  *metric.Histogram
	CurBytesCount *metric.Gauge
}

// MetricStruct implements the metrics.Struct interface.
func (DistSQLMetrics) MetricStruct() {}

var _ metric.Struct = DistSQLMetrics{}

var (
	metaQueriesActive = metric.Metadata{
		Name: "sql.distsql.queries.active",
		Help: "Number of distributed SQL queries currently active"}
	metaQueriesTotal = metric.Metadata{
		Name: "sql.distsql.queries.total",
		Help: "Number of distributed SQL queries executed"}
	metaFlowsActive = metric.Metadata{
		Name: "sql.distsql.flows.active",
		Help: "Number of distributed SQL flows currently active"}
	metaFlowsTotal = metric.Metadata{
		Name: "sql.distsql.flows.total",
		Help: "Number of distributed SQL flows executed"}
	metaMemMaxBytes = metric.Metadata{
		Name: "sql.mem.distsql.max",
		Help: "Memory usage per sql statement for distsql"}
	metaMemCurBytes = metric.Metadata{
		Name: "sql.mem.distsql.current",
		Help: "Current sql statement memory usage for distsql"}
)

// See pkg/sql/mem_metrics.go
// log10int64times1000 = log10(math.MaxInt64) * 1000, rounded up somewhat
const log10int64times1000 = 19 * 1000

// MakeDistSQLMetrics instantiates the metrics holder for DistSQL monitoring.
func MakeDistSQLMetrics(histogramWindow time.Duration) DistSQLMetrics {
	return DistSQLMetrics{
		QueriesActive: metric.NewGauge(metaQueriesActive),
		QueriesTotal:  metric.NewCounter(metaQueriesTotal),
		FlowsActive:   metric.NewGauge(metaFlowsActive),
		FlowsTotal:    metric.NewCounter(metaFlowsTotal),
		MaxBytesHist:  metric.NewHistogram(metaMemMaxBytes, histogramWindow, log10int64times1000, 3),
		CurBytesCount: metric.NewGauge(metaMemCurBytes),
	}
}

// QueryStart registers the start of a new DistSQL query.
func (m *DistSQLMetrics) QueryStart() {
	m.QueriesActive.Inc(1)
	m.QueriesTotal.Inc(1)
}

// QueryStop registers the end of a DistSQL query.
func (m *DistSQLMetrics) QueryStop() {
	m.QueriesActive.Dec(1)
}

// FlowStart registers the start of a new DistSQL flow.
func (m *DistSQLMetrics) FlowStart() {
	m.FlowsActive.Inc(1)
	m.FlowsTotal.Inc(1)
}

// FlowStop registers the end of a DistSQL flow.
func (m *DistSQLMetrics) FlowStop() {
	m.FlowsActive.Dec(1)
}
