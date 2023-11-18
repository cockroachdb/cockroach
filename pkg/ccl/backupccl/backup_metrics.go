// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

type BackupMetrics struct {
	LastKMSInaccessibleErrorTime *metric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (b BackupMetrics) MetricStruct() {}

// MakeBackupMetrics instantiates the metrics for backup.
func MakeBackupMetrics(time.Duration) metric.Struct {
	m := &BackupMetrics{
		LastKMSInaccessibleErrorTime: metric.NewGauge(metric.Metadata{
			Name:        "backup.last-failed-time.kms-inaccessible",
			Help:        "The unix timestamp of the most recent failure of backup due to errKMSInaccessible by a backup specified as maintaining this metric",
			Measurement: "Jobs",
			Unit:        metric.Unit_TIMESTAMP_SEC,
		}),
	}
	return m
}

func init() {
	jobs.MakeBackupMetricsHook = MakeBackupMetrics
}
