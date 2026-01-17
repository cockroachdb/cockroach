// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ptstorage

import (
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// Metrics holds metrics for the protected timestamp storage.
type Metrics struct {
	ProtectSuccess         *metric.Counter
	ProtectFailed          *metric.Counter
	ReleaseSuccess         *metric.Counter
	ReleaseFailed          *metric.Counter
	UpdateTimestampSuccess *metric.Counter
	UpdateTimestampFailed  *metric.Counter
	GetRecordSuccess       *metric.Counter
	GetRecordFailed        *metric.Counter
}

var _ metric.Struct = (*Metrics)(nil)

// MetricStruct makes Metrics a metric.Struct.
func (m *Metrics) MetricStruct() {}

func makeMetrics() Metrics {
	return Metrics{
		ProtectSuccess:         metric.NewCounter(metaProtectSuccess),
		ProtectFailed:          metric.NewCounter(metaProtectFailed),
		ReleaseSuccess:         metric.NewCounter(metaReleaseSuccess),
		ReleaseFailed:          metric.NewCounter(metaReleaseFailed),
		UpdateTimestampSuccess: metric.NewCounter(metaUpdateTimestampSuccess),
		UpdateTimestampFailed:  metric.NewCounter(metaUpdateTimestampFailed),
		GetRecordSuccess:       metric.NewCounter(metaGetRecordSuccess),
		GetRecordFailed:        metric.NewCounter(metaGetRecordFailed),
	}
}

var (
	metaProtectSuccess = metric.Metadata{
		Name:        "kv.protectedts.protect.success",
		Help:        "number of successful Protect operations creating protected timestamp records",
		Measurement: "Operations",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaProtectFailed = metric.Metadata{
		Name:        "kv.protectedts.protect.failed",
		Help:        "number of failed Protect operations (ErrExists, validation errors, or execution errors)",
		Measurement: "Operations",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaReleaseSuccess = metric.Metadata{
		Name:        "kv.protectedts.release.success",
		Help:        "number of successful Release operations removing protected timestamp records",
		Measurement: "Operations",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaReleaseFailed = metric.Metadata{
		Name:        "kv.protectedts.release.failed",
		Help:        "number of failed Release operations (ErrNotExists or execution errors)",
		Measurement: "Operations",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaUpdateTimestampSuccess = metric.Metadata{
		Name:        "kv.protectedts.update_timestamp.success",
		Help:        "number of successful UpdateTimestamp operations",
		Measurement: "Operations",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaUpdateTimestampFailed = metric.Metadata{
		Name:        "kv.protectedts.update_timestamp.failed",
		Help:        "number of failed UpdateTimestamp operations (ErrNotExists or execution errors)",
		Measurement: "Operations",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaGetRecordSuccess = metric.Metadata{
		Name:        "kv.protectedts.get_record.success",
		Help:        "number of successful GetRecord operations",
		Measurement: "Operations",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaGetRecordFailed = metric.Metadata{
		Name:        "kv.protectedts.get_record.failed",
		Help:        "number of failed GetRecord operations (ErrNotExists, parsing errors, or execution errors)",
		Measurement: "Operations",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
)
