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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

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
	CurBytesCount        *metric.Counter
	TxnMaxBytesHist      *metric.Histogram
	TxnCurBytesCount     *metric.Counter
	SessionMaxBytesHist  *metric.Histogram
	SessionCurBytesCount *metric.Counter
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
func MakeMemMetrics(endpoint string) MemoryMetrics {
	prefix := "sql.mon." + endpoint
	MetaMemMaxBytes := metric.Metadata{Name: prefix + ".max"}
	MetaMemCurBytes := metric.Metadata{Name: prefix + ".cur"}
	MetaMemMaxTxnBytes := metric.Metadata{Name: prefix + ".txn.max"}
	MetaMemTxnCurBytes := metric.Metadata{Name: prefix + ".txn.cur"}
	MetaMemMaxSessionBytes := metric.Metadata{Name: prefix + ".session.max"}
	MetaMemSessionCurBytes := metric.Metadata{Name: prefix + ".session.cur"}
	return MemoryMetrics{
		MaxBytesHist:         metric.NewHistogram(MetaMemMaxBytes, time.Minute, log10int64times1000, 3),
		CurBytesCount:        metric.NewCounter(MetaMemCurBytes),
		TxnMaxBytesHist:      metric.NewHistogram(MetaMemMaxTxnBytes, time.Minute, log10int64times1000, 3),
		TxnCurBytesCount:     metric.NewCounter(MetaMemTxnCurBytes),
		SessionMaxBytesHist:  metric.NewHistogram(MetaMemMaxSessionBytes, time.Minute, log10int64times1000, 3),
		SessionCurBytesCount: metric.NewCounter(MetaMemSessionCurBytes),
	}
}
