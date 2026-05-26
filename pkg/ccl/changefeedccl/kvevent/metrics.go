// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvevent

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var (
	metaChangefeedBufferEntriesIn = metric.Metadata{
		Name:        "changefeed.buffer_entries.in",
		Help:        "Total entries entering the buffer between raft and changefeed sinks",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
		Category:    metric.Metadata_CHANGEFEEDS,
	}
	metaChangefeedBufferEntriesOut = metric.Metadata{
		Name:        "changefeed.buffer_entries.out",
		Help:        "Total entries leaving the buffer between raft and changefeed sinks",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
		Category:    metric.Metadata_CHANGEFEEDS,
	}
	metaChangefeedBufferEntriesReleased = metric.Metadata{
		Name:        "changefeed.buffer_entries.released",
		Help:        "Total entries processed, emitted and acknowledged by the sinks",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
		Category:    metric.Metadata_CHANGEFEEDS,
	}
	metaChangefeedBufferMemAcquired = metric.Metadata{
		Name:        "changefeed.buffer_entries_mem.acquired",
		Help:        "Total amount of memory acquired for entries as they enter the system",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
		Category:    metric.Metadata_CHANGEFEEDS,
	}
	metaChangefeedBufferMemReleased = metric.Metadata{
		Name:        "changefeed.buffer_entries_mem.released",
		Help:        "Total amount of memory released by the entries after they have been emitted",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
		Category:    metric.Metadata_CHANGEFEEDS,
	}
	metaChangefeedBufferPushbackNanos = metric.Metadata{
		Name:        "changefeed.buffer_pushback_nanos",
		Help:        "Total time spent waiting while the buffer was full",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
		Category:    metric.Metadata_CHANGEFEEDS,
	}
	metaChangefeedAllocatedMemory = metric.Metadata{
		Name:        "changefeed.buffer_entries.allocated_mem",
		Help:        "Current quota pool memory allocation",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
		Category:    metric.Metadata_CHANGEFEEDS,
	}
)

// Metrics is a metric.Struct for kvfeed metrics.
type Metrics struct {
	// RangefeedBufferMetrics tracks metrics for the buffer between the rangefeed and the kvfeed.
	RangefeedBufferMetrics PerBufferMetrics
	// AggregatorBufferMetrics tracks metrics for the buffer between the kvfeed and the sink.
	AggregatorBufferMetrics PerBufferMetrics
	// CommonBufferMetrics tracks metrics that are common to both the rangefeed and aggregator buffers.
	CommonBufferMetrics CommonBufferMetrics
}

// CommonBufferMetrics tracks metrics that are common to both the rangefeed and aggregator buffers.
type CommonBufferMetrics struct {
	BufferEntriesMemAcquired *metric.Counter
	BufferEntriesMemReleased *metric.Counter
	AllocatedMem             *metric.Gauge
}

func (CommonBufferMetrics) MetricStruct() {}

var _ metric.Struct = (*CommonBufferMetrics)(nil)

// PerBufferMetrics tracks metrics for a single buffer.
type PerBufferMetrics struct {
	BufferType            bufferType
	BufferEntriesIn       *metric.Counter
	BufferEntriesOut      *metric.Counter
	BufferEntriesReleased *metric.Counter
	BufferPushbackNanos   *metric.Counter
	BufferEntriesByType   [numEventTypes]*metric.Counter
	// CommonBufferMetrics tracks metrics that are common to both the rangefeed
	// and aggregator buffers. This is a pointer to the common metrics field of
	// the Metrics struct.
	*CommonBufferMetrics
}

var _ metric.Struct = (*PerBufferMetrics)(nil)

func (PerBufferMetrics) MetricStruct() {}

type bufferType string

const (
	rangefeedBuffer  bufferType = "rangefeed"
	aggregatorBuffer bufferType = "aggregator"
)

// We don't want to rely on labels for this since tsdb doesn't support them, So make separate metrics for each buffer type.
func (bt bufferType) alterMeta(meta metric.Metadata) metric.Metadata {
	meta.LabeledName = meta.Name
	meta.Name = fmt.Sprintf("%s.%s", meta.Name, bt)
	meta.StaticLabels = metric.MakeLabelPairs("buffer_type", string(bt))
	switch bt {
	case rangefeedBuffer:
		meta.Help = fmt.Sprintf("%s - between the rangefeed and the kvfeed", meta.Help)
	case aggregatorBuffer:
		meta.Help = fmt.Sprintf("%s - between the kvfeed and the sink", meta.Help)
	}
	return meta
}

// SafeValue implements redact.SafeValue.
func (bt bufferType) SafeValue() {}

// MakeMetrics constructs a Metrics struct with the provided histogram window.
func MakeMetrics(histogramWindow time.Duration) Metrics {
	eventTypeMeta := func(et Type) metric.Metadata {
		eventTypeName := func() string {
			switch et {
			case TypeFlush:
				return "flush"
			case TypeKV:
				return "kv"
			default:
				return "resolved"
			}
		}()
		return metric.Metadata{
			Name:        fmt.Sprintf("changefeed.buffer_entries.%s", eventTypeName),
			Help:        fmt.Sprintf("Number of %s elements added to the buffer", eventTypeName),
			Measurement: "Events",
			Unit:        metric.Unit_COUNT,
			Category:    metric.Metadata_CHANGEFEEDS,
		}
	}
	commonBufferMetrics := CommonBufferMetrics{
		BufferEntriesMemAcquired: metric.NewCounter(metaChangefeedBufferMemAcquired),
		BufferEntriesMemReleased: metric.NewCounter(metaChangefeedBufferMemReleased),
		AllocatedMem:             metric.NewGauge(metaChangefeedAllocatedMemory),
	}
	m := Metrics{
		CommonBufferMetrics: commonBufferMetrics,
		RangefeedBufferMetrics: PerBufferMetrics{
			BufferType:            rangefeedBuffer,
			BufferEntriesIn:       metric.NewCounter(rangefeedBuffer.alterMeta(metaChangefeedBufferEntriesIn)),
			BufferEntriesOut:      metric.NewCounter(rangefeedBuffer.alterMeta(metaChangefeedBufferEntriesOut)),
			BufferEntriesReleased: metric.NewCounter(rangefeedBuffer.alterMeta(metaChangefeedBufferEntriesReleased)),
			BufferPushbackNanos:   metric.NewCounter(rangefeedBuffer.alterMeta(metaChangefeedBufferPushbackNanos)),
			BufferEntriesByType: [numEventTypes]*metric.Counter{
				metric.NewCounter(rangefeedBuffer.alterMeta(eventTypeMeta(TypeFlush))),
				metric.NewCounter(rangefeedBuffer.alterMeta(eventTypeMeta(TypeKV))),
				metric.NewCounter(rangefeedBuffer.alterMeta(eventTypeMeta(TypeResolved))),
			},
			CommonBufferMetrics: &commonBufferMetrics,
		},
		AggregatorBufferMetrics: PerBufferMetrics{
			BufferType:            aggregatorBuffer,
			BufferEntriesIn:       metric.NewCounter(aggregatorBuffer.alterMeta(metaChangefeedBufferEntriesIn)),
			BufferEntriesOut:      metric.NewCounter(aggregatorBuffer.alterMeta(metaChangefeedBufferEntriesOut)),
			BufferEntriesReleased: metric.NewCounter(aggregatorBuffer.alterMeta(metaChangefeedBufferEntriesReleased)),
			BufferPushbackNanos:   metric.NewCounter(aggregatorBuffer.alterMeta(metaChangefeedBufferPushbackNanos)),
			BufferEntriesByType: [numEventTypes]*metric.Counter{
				metric.NewCounter(aggregatorBuffer.alterMeta(eventTypeMeta(TypeFlush))),
				metric.NewCounter(aggregatorBuffer.alterMeta(eventTypeMeta(TypeKV))),
				metric.NewCounter(aggregatorBuffer.alterMeta(eventTypeMeta(TypeResolved))),
			},
			CommonBufferMetrics: &commonBufferMetrics,
		},
	}
	return m
}

var _ (metric.Struct) = (*Metrics)(nil)

// MetricStruct makes Metrics a metric.Struct.
func (m Metrics) MetricStruct() {}
