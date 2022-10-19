// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package backupccl

import (
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var (
	metaFileSSTSinkFlushedBytes = metric.Metadata{
		Name:        "backup.file_sst_sink.flushed.bytes",
		Help:        "Bytes flushed by the file SST sink",
		Measurement: "Bytes",
		Unit:        metric.Unit_COUNT,
	}
	metaFileSSTSinkFlushedFiles = metric.Metadata{
		Name:        "backup.file_sst_sink.flushed.files",
		Help:        "Files flushed by the file SST sink",
		Measurement: "Flushes",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics are for monitoring of backup jobs.
type Metrics struct {
	// FileSSTSinkFlushedBytes tracks the number of bytes that have been flushed
	// across file SST sinks that are responsible for writing SSTs to
	// ExternalStorage during backup.
	FileSSTSinkFlushedBytes *metric.Counter
	// FileSSTSinkFlushedFiles tracks the number of SST files that have been
	// flushed across file SST sinks that are responsible for writing SSTs to
	// ExternalStorage during backup.
	FileSSTSinkFlushedFiles *metric.Counter
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// MakeMetrics makes the metrics for stream ingestion job monitoring.
func MakeMetrics() metric.Struct {
	m := &Metrics{
		FileSSTSinkFlushedBytes: metric.NewCounter(metaFileSSTSinkFlushedBytes),
		FileSSTSinkFlushedFiles: metric.NewCounter(metaFileSSTSinkFlushedFiles),
	}
	return m
}

func init() {
	jobs.MakeBackupMetricsHook = MakeMetrics
}
