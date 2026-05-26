// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfra

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// BulkMergeMetrics contains metrics for monitoring distributed merge
// operations used by index backfill and IMPORT.
type BulkMergeMetrics struct {
	IndexBackfillCount       *metric.Counter
	ImportCount              *metric.Counter
	RPCMemoryReservedBytes   metric.IHistogram
	MapPhaseSSTs             metric.IHistogram
	FirstIterationOutputSSTs metric.IHistogram
}

// MetricStruct implements the metric.Struct interface.
func (BulkMergeMetrics) MetricStruct() {}

var _ metric.Struct = BulkMergeMetrics{}

var (
	metaDistMergeIndexCount = metric.Metadata{
		Name:         "sql.dist_merge.index.count",
		Help:         "Number of distributed merge operations for index backfill",
		Measurement:  "Operations",
		Unit:         metric.Unit_COUNT,
		LabeledName:  "sql.dist_merge.count",
		StaticLabels: metric.MakeLabelPairs(metric.LabelType, "index"),
	}
	metaDistMergeImportCount = metric.Metadata{
		Name:         "sql.dist_merge.import.count",
		Help:         "Number of distributed merge operations for import",
		Measurement:  "Operations",
		Unit:         metric.Unit_COUNT,
		LabeledName:  "sql.dist_merge.count",
		StaticLabels: metric.MakeLabelPairs(metric.LabelType, "import"),
	}
	metaDistMergeRPCMemory = metric.Metadata{
		Name:        "sql.dist_merge.rpc_memory_reserved.bytes",
		Help:        "Memory reserved for RPC transport buffers in the final merge iteration",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaDistMergeMapPhaseSSTs = metric.Metadata{
		Name:        "sql.dist_merge.map_phase.sst_count",
		Help:        "Number of SSTs from the map phase input to the first merge iteration",
		Measurement: "SSTs",
		Unit:        metric.Unit_COUNT,
	}
	metaDistMergeFirstIterOutputSSTs = metric.Metadata{
		Name:        "sql.dist_merge.first_iter.output_sst_count",
		Help:        "Number of SSTs produced by the first (local) merge iteration",
		Measurement: "SSTs",
		Unit:        metric.Unit_COUNT,
	}
)

// MakeBulkMergeMetrics initializes distributed merge metrics with the given
// histogram window for time-based bucketing.
func MakeBulkMergeMetrics(histogramWindow time.Duration) BulkMergeMetrics {
	return BulkMergeMetrics{
		IndexBackfillCount: metric.NewCounter(metaDistMergeIndexCount),
		ImportCount:        metric.NewCounter(metaDistMergeImportCount),
		RPCMemoryReservedBytes: metric.NewHistogram(metric.HistogramOptions{
			Metadata:     metaDistMergeRPCMemory,
			Duration:     histogramWindow,
			BucketConfig: metric.MemoryUsage64MBBuckets,
		}),
		MapPhaseSSTs: metric.NewHistogram(metric.HistogramOptions{
			Metadata:     metaDistMergeMapPhaseSSTs,
			Duration:     histogramWindow,
			BucketConfig: metric.Count1KBuckets,
		}),
		FirstIterationOutputSSTs: metric.NewHistogram(metric.HistogramOptions{
			Metadata:     metaDistMergeFirstIterOutputSSTs,
			Duration:     histogramWindow,
			BucketConfig: metric.Count1KBuckets,
		}),
	}
}
