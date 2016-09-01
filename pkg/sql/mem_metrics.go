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
// for one of the SQL endpoints.
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

// Fully-qualified names for metrics.
var (
	MetaInternalMemMaxBytes        = metric.Metadata{Name: "sql.mon.internal.max"}
	MetaInternalMemCurBytes        = metric.Metadata{Name: "sql.mon.internal.cur"}
	MetaInternalMemMaxTxnBytes     = metric.Metadata{Name: "sql.mon.internal.txn.max"}
	MetaInternalMemTxnCurBytes     = metric.Metadata{Name: "sql.mon.internal.txn.cur"}
	MetaInternalMemMaxSessionBytes = metric.Metadata{Name: "sql.mon.internal.session.max"}
	MetaInternalMemSessionCurBytes = metric.Metadata{Name: "sql.mon.internal.session.cur"}
	MetaAdminMemMaxBytes           = metric.Metadata{Name: "sql.mon.admin.max"}
	MetaAdminMemCurBytes           = metric.Metadata{Name: "sql.mon.admin.cur"}
	MetaAdminMemMaxTxnBytes        = metric.Metadata{Name: "sql.mon.admin.txn.max"}
	MetaAdminMemTxnCurBytes        = metric.Metadata{Name: "sql.mon.admin.txn.cur"}
	MetaAdminMemMaxSessionBytes    = metric.Metadata{Name: "sql.mon.admin.session.max"}
	MetaAdminMemSessionCurBytes    = metric.Metadata{Name: "sql.mon.admin.session.cur"}
	MetaClientMemMaxBytes          = metric.Metadata{Name: "sql.mon.client.max"}
	MetaClientMemCurBytes          = metric.Metadata{Name: "sql.mon.client.cur"}
	MetaClientMemMaxTxnBytes       = metric.Metadata{Name: "sql.mon.client.txn.max"}
	MetaClientMemTxnCurBytes       = metric.Metadata{Name: "sql.mon.client.txn.cur"}
	MetaClientMemMaxSessionBytes   = metric.Metadata{Name: "sql.mon.client.session.max"}
	MetaClientMemSessionCurBytes   = metric.Metadata{Name: "sql.mon.client.session.cur"}
)

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

// MakeInternalMemMetrics instantiates the metric objects for the internal SQL executor.
func MakeInternalMemMetrics() MemoryMetrics {
	return MemoryMetrics{
		MaxBytesHist:         metric.NewHistogram(MetaInternalMemMaxBytes, time.Minute, log10int64times1000, 3),
		CurBytesCount:        metric.NewCounter(MetaInternalMemCurBytes),
		TxnMaxBytesHist:      metric.NewHistogram(MetaInternalMemMaxTxnBytes, time.Minute, log10int64times1000, 3),
		TxnCurBytesCount:     metric.NewCounter(MetaInternalMemTxnCurBytes),
		SessionMaxBytesHist:  metric.NewHistogram(MetaInternalMemMaxSessionBytes, time.Minute, log10int64times1000, 3),
		SessionCurBytesCount: metric.NewCounter(MetaInternalMemSessionCurBytes),
	}
}

// MakeAdminMemMetrics instantiates the metric objects for the SQL executor for admin RPC requests.
func MakeAdminMemMetrics() MemoryMetrics {
	return MemoryMetrics{
		MaxBytesHist:         metric.NewHistogram(MetaAdminMemMaxBytes, time.Minute, log10int64times1000, 3),
		CurBytesCount:        metric.NewCounter(MetaAdminMemCurBytes),
		TxnMaxBytesHist:      metric.NewHistogram(MetaAdminMemMaxTxnBytes, time.Minute, log10int64times1000, 3),
		TxnCurBytesCount:     metric.NewCounter(MetaAdminMemTxnCurBytes),
		SessionMaxBytesHist:  metric.NewHistogram(MetaAdminMemMaxSessionBytes, time.Minute, log10int64times1000, 3),
		SessionCurBytesCount: metric.NewCounter(MetaAdminMemSessionCurBytes),
	}
}

// MakeClientMemMetrics instantiates the metric objects for the SQL executor for regular clients.
func MakeClientMemMetrics() MemoryMetrics {
	return MemoryMetrics{
		MaxBytesHist:         metric.NewHistogram(MetaClientMemMaxBytes, time.Minute, log10int64times1000, 3),
		CurBytesCount:        metric.NewCounter(MetaClientMemCurBytes),
		TxnMaxBytesHist:      metric.NewHistogram(MetaClientMemMaxTxnBytes, time.Minute, log10int64times1000, 3),
		TxnCurBytesCount:     metric.NewCounter(MetaClientMemTxnCurBytes),
		SessionMaxBytesHist:  metric.NewHistogram(MetaClientMemMaxSessionBytes, time.Minute, log10int64times1000, 3),
		SessionCurBytesCount: metric.NewCounter(MetaClientMemSessionCurBytes),
	}
}
