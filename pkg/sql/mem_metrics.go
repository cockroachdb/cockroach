// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// MemoryMetrics contains pointers to the metrics object
// for one of the SQL endpoints:
// - "client" for connections received via pgwire.
// - "admin" for connections received via the admin RPC.
// - "internal" for activities related to leases, schema changes, etc.
type MemoryMetrics struct {
	MaxBytesHist         *metric.Histogram
	CurBytesCount        *metric.Gauge
	TxnMaxBytesHist      *metric.Histogram
	TxnCurBytesCount     *metric.Gauge
	SessionMaxBytesHist  *metric.Histogram
	SessionCurBytesCount *metric.Gauge
}

// MetricStruct implements the metrics.Struct interface.
func (MemoryMetrics) MetricStruct() {}

var _ metric.Struct = MemoryMetrics{}

// TODO(knz): Until #10014 is addressed, the UI graphs don't have a
// log scale on the Y axis and the histograms are thus displayed using
// a manual log scale: we store the logarithm in the value in the DB
// and plot that logarithm in the UI.
//
// We could, but do not, store the full value in the DB and compute
// the log in the UI, because the current histogram implementation
// does not deal well with large maxima (#10015).
//
// Since the DB stores an integer, we scale the values by 1000 so that
// a modicum of precision is restored when exponentiating the value.
//

// log10int64times1000 = log10(math.MaxInt64) * 1000, rounded up somewhat
const log10int64times1000 = 19 * 1000

// MakeMemMetrics instantiates the metric objects for an SQL endpoint.
func MakeMemMetrics(endpoint string, histogramWindow time.Duration) MemoryMetrics {
	prefix := "sql.mem." + endpoint
	MetaMemMaxBytes := metric.Metadata{
		Name: prefix + ".max",
		Help: "Memory usage per sql statement for " + endpoint}
	MetaMemCurBytes := metric.Metadata{
		Name: prefix + ".current",
		Help: "Current sql statement memory usage for " + endpoint}
	MetaMemMaxTxnBytes := metric.Metadata{
		Name: prefix + ".txn.max",
		Help: "Memory usage per sql transaction for " + endpoint}
	MetaMemTxnCurBytes := metric.Metadata{
		Name: prefix + ".txn.current",
		Help: "Current sql transaction memory usage for " + endpoint}
	MetaMemMaxSessionBytes := metric.Metadata{
		Name: prefix + ".session.max",
		Help: "Memory usage per sql session for " + endpoint}
	MetaMemSessionCurBytes := metric.Metadata{
		Name: prefix + ".session.current",
		Help: "Current sql session memory usage for " + endpoint}
	return MemoryMetrics{
		MaxBytesHist:         metric.NewHistogram(MetaMemMaxBytes, histogramWindow, log10int64times1000, 3),
		CurBytesCount:        metric.NewGauge(MetaMemCurBytes),
		TxnMaxBytesHist:      metric.NewHistogram(MetaMemMaxTxnBytes, histogramWindow, log10int64times1000, 3),
		TxnCurBytesCount:     metric.NewGauge(MetaMemTxnCurBytes),
		SessionMaxBytesHist:  metric.NewHistogram(MetaMemMaxSessionBytes, histogramWindow, log10int64times1000, 3),
		SessionCurBytesCount: metric.NewGauge(MetaMemSessionCurBytes),
	}
}
