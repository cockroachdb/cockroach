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
	}
	metaChangefeedBufferEntriesOut = metric.Metadata{
		Name:        "changefeed.buffer_entries.out",
		Help:        "Total entries leaving the buffer between raft and changefeed sinks",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedBufferEntriesReleased = metric.Metadata{
		Name:        "changefeed.buffer_entries.released",
		Help:        "Total entries processed, emitted and acknowledged by the sinks",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedBufferMemAcquired = metric.Metadata{
		Name:        "changefeed.buffer_entries_mem.acquired",
		Help:        "Total amount of memory acquired for entries as they enter the system",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedBufferMemReleased = metric.Metadata{
		Name:        "changefeed.buffer_entries_mem.released",
		Help:        "Total amount of memory released by the entries after they have been emitted",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedBufferPushbackNanos = metric.Metadata{
		Name:        "changefeed.buffer_pushback_nanos",
		Help:        "Total time spent waiting while the buffer was full",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaChangefeedAllocatedMemory = metric.Metadata{
		Name:        "changefeed.buffer_entries.allocated_mem",
		Help:        "Current quota pool memory allocation",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
)

// Metrics is a metric.Struct for kvfeed metrics.
type Metrics struct {
	// RangefeedBufferMetrics tracks metrics for the buffer between the rangefeed and the kvfeed.
	// It's exposed to get registered as a metric.Struct.
	RangefeedBufferMetrics PerBufferMetrics
	// AggregatorBufferMetrics tracks metrics for the buffer between the kvfeed and the sink.
	// It's exposed to get registered as a metric.Struct.
	AggregatorBufferMetrics PerBufferMetrics

	// RangefeedBufferMetricsWithCompat tracks metrics for the buffer between
	// the rangefeed and the kvfeed, while forwarding the old metrics for compatibility.
	// This should be the version used by the kvfeed, and it's exposed for that purpose.
	RangefeedBufferMetricsWithCompat PerBufferMetricsWithCompat
	// AggregatorBufferMetricsWithCompat tracks metrics for the buffer between
	// the kvfeed and the sink, while forwarding the old metrics for compatibility.
	// This should be the version used by the kvfeed, and it's exposed for that purpose.
	AggregatorBufferMetricsWithCompat PerBufferMetricsWithCompat

	// CombinedBufferMetrics tracks the combined metrics for both buffers, for
	// historical reasons. It's exposed to get registered as a metric.Struct.
	CombinedBufferMetrics PerBufferMetrics
}

// PerBufferMetricsWithCompat is a compatibility layer between the new
// per-buffer metrics and the old metrics. Note that it is not a metric.Struct.
type PerBufferMetricsWithCompat struct {
	BufferType               bufferType
	BufferEntriesIn          *forwardingCounter
	BufferEntriesOut         *forwardingCounter
	BufferEntriesReleased    *forwardingCounter
	BufferPushbackNanos      *forwardingCounter
	BufferEntriesMemAcquired *forwardingCounter
	BufferEntriesMemReleased *forwardingCounter
	AllocatedMem             *forwardingGauge
	BufferEntriesByType      [numEventTypes]*forwardingCounter
}

// forwardingCounter is a wrapper around multiple `*metric.Counter`s that
// forwards Inc/Dec calls to all of them. It's used here to implement the
// compatibility layer between the new and old metrics.
type forwardingCounter struct {
	sinks []*metric.Counter
}

func (fc *forwardingCounter) Inc(n int64) {
	for _, c := range fc.sinks {
		c.Inc(n)
	}
}

func (fc *forwardingCounter) Count() int64 {
	if len(fc.sinks) == 0 {
		return 0
	}
	// This method is only used in tests, so just return data from an arbitrary
	// sink.
	return fc.sinks[0].Count()
}

// forwardingGauge is a wrapper around multiple `*metric.Gauge`s that
// forwards Inc/Dec calls to all of them. It's used here to implement the
// compatibility layer between the new and old metrics.
type forwardingGauge struct {
	sinks []*metric.Gauge
}

func (fc *forwardingGauge) Inc(n int64) {
	for _, c := range fc.sinks {
		c.Inc(n)
	}
}

func (fc *forwardingGauge) Dec(n int64) {
	for _, c := range fc.sinks {
		c.Dec(n)
	}
}

func (fc *forwardingGauge) Value() int64 {
	if len(fc.sinks) == 0 {
		return 0
	}
	// This method is only used in tests, so just return data from an arbitrary
	// sink.
	return fc.sinks[0].Value()
}

type PerBufferMetrics struct {
	BufferType               bufferType
	BufferEntriesIn          *metric.Counter
	BufferEntriesOut         *metric.Counter
	BufferEntriesReleased    *metric.Counter
	BufferPushbackNanos      *metric.Counter
	BufferEntriesMemAcquired *metric.Counter
	BufferEntriesMemReleased *metric.Counter
	AllocatedMem             *metric.Gauge
	BufferEntriesByType      [numEventTypes]*metric.Counter
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
	meta.Name = fmt.Sprintf("%s.%s", meta.Name, bt)
	switch bt {
	case rangefeedBuffer:
		meta.Help = fmt.Sprintf("%s - between the rangefeed and the kvfeed", meta.Help)
	case aggregatorBuffer:
		meta.Help = fmt.Sprintf("%s - between the kvfeed and the sink", meta.Help)
	}
	return meta
}

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
		}
	}
	m := Metrics{
		RangefeedBufferMetrics: PerBufferMetrics{
			BufferType:               rangefeedBuffer,
			BufferEntriesIn:          metric.NewCounter(rangefeedBuffer.alterMeta(metaChangefeedBufferEntriesIn)),
			BufferEntriesOut:         metric.NewCounter(rangefeedBuffer.alterMeta(metaChangefeedBufferEntriesOut)),
			BufferEntriesReleased:    metric.NewCounter(rangefeedBuffer.alterMeta(metaChangefeedBufferEntriesReleased)),
			BufferEntriesMemAcquired: metric.NewCounter(rangefeedBuffer.alterMeta(metaChangefeedBufferMemAcquired)),
			BufferEntriesMemReleased: metric.NewCounter(rangefeedBuffer.alterMeta(metaChangefeedBufferMemReleased)),
			BufferPushbackNanos:      metric.NewCounter(rangefeedBuffer.alterMeta(metaChangefeedBufferPushbackNanos)),
			AllocatedMem:             metric.NewGauge(rangefeedBuffer.alterMeta(metaChangefeedAllocatedMemory)),
			BufferEntriesByType: [numEventTypes]*metric.Counter{
				metric.NewCounter(rangefeedBuffer.alterMeta(eventTypeMeta(TypeFlush))),
				metric.NewCounter(rangefeedBuffer.alterMeta(eventTypeMeta(TypeKV))),
				metric.NewCounter(rangefeedBuffer.alterMeta(eventTypeMeta(TypeResolved))),
			},
		},
		AggregatorBufferMetrics: PerBufferMetrics{
			BufferType:               aggregatorBuffer,
			BufferEntriesIn:          metric.NewCounter(aggregatorBuffer.alterMeta(metaChangefeedBufferEntriesIn)),
			BufferEntriesOut:         metric.NewCounter(aggregatorBuffer.alterMeta(metaChangefeedBufferEntriesOut)),
			BufferEntriesReleased:    metric.NewCounter(aggregatorBuffer.alterMeta(metaChangefeedBufferEntriesReleased)),
			BufferEntriesMemAcquired: metric.NewCounter(aggregatorBuffer.alterMeta(metaChangefeedBufferMemAcquired)),
			BufferEntriesMemReleased: metric.NewCounter(aggregatorBuffer.alterMeta(metaChangefeedBufferMemReleased)),
			BufferPushbackNanos:      metric.NewCounter(aggregatorBuffer.alterMeta(metaChangefeedBufferPushbackNanos)),
			AllocatedMem:             metric.NewGauge(aggregatorBuffer.alterMeta(metaChangefeedAllocatedMemory)),
			BufferEntriesByType: [numEventTypes]*metric.Counter{
				metric.NewCounter(aggregatorBuffer.alterMeta(eventTypeMeta(TypeFlush))),
				metric.NewCounter(aggregatorBuffer.alterMeta(eventTypeMeta(TypeKV))),
				metric.NewCounter(aggregatorBuffer.alterMeta(eventTypeMeta(TypeResolved))),
			},
		},
		CombinedBufferMetrics: PerBufferMetrics{
			BufferType:               aggregatorBuffer,
			BufferEntriesIn:          metric.NewCounter(metaChangefeedBufferEntriesIn),
			BufferEntriesOut:         metric.NewCounter(metaChangefeedBufferEntriesOut),
			BufferEntriesReleased:    metric.NewCounter(metaChangefeedBufferEntriesReleased),
			BufferEntriesMemAcquired: metric.NewCounter(metaChangefeedBufferMemAcquired),
			BufferEntriesMemReleased: metric.NewCounter(metaChangefeedBufferMemReleased),
			BufferPushbackNanos:      metric.NewCounter(metaChangefeedBufferPushbackNanos),
			AllocatedMem:             metric.NewGauge(metaChangefeedAllocatedMemory),
			BufferEntriesByType: [numEventTypes]*metric.Counter{
				metric.NewCounter(eventTypeMeta(TypeFlush)),
				metric.NewCounter(eventTypeMeta(TypeKV)),
				metric.NewCounter(eventTypeMeta(TypeResolved)),
			},
		},
	}

	m.AggregatorBufferMetricsWithCompat = PerBufferMetricsWithCompat{
		BufferType:               aggregatorBuffer,
		BufferEntriesIn:          &forwardingCounter{sinks: []*metric.Counter{m.AggregatorBufferMetrics.BufferEntriesIn, m.CombinedBufferMetrics.BufferEntriesIn}},
		BufferEntriesOut:         &forwardingCounter{sinks: []*metric.Counter{m.AggregatorBufferMetrics.BufferEntriesOut, m.CombinedBufferMetrics.BufferEntriesOut}},
		BufferEntriesReleased:    &forwardingCounter{sinks: []*metric.Counter{m.AggregatorBufferMetrics.BufferEntriesReleased, m.CombinedBufferMetrics.BufferEntriesReleased}},
		BufferEntriesMemAcquired: &forwardingCounter{sinks: []*metric.Counter{m.AggregatorBufferMetrics.BufferEntriesMemAcquired, m.CombinedBufferMetrics.BufferEntriesMemAcquired}},
		BufferEntriesMemReleased: &forwardingCounter{sinks: []*metric.Counter{m.AggregatorBufferMetrics.BufferEntriesMemReleased, m.CombinedBufferMetrics.BufferEntriesMemReleased}},
		BufferPushbackNanos:      &forwardingCounter{sinks: []*metric.Counter{m.AggregatorBufferMetrics.BufferPushbackNanos, m.CombinedBufferMetrics.BufferPushbackNanos}},
		AllocatedMem:             &forwardingGauge{sinks: []*metric.Gauge{m.AggregatorBufferMetrics.AllocatedMem, m.CombinedBufferMetrics.AllocatedMem}},
		BufferEntriesByType: [numEventTypes]*forwardingCounter{
			{sinks: []*metric.Counter{m.AggregatorBufferMetrics.BufferEntriesByType[0], m.CombinedBufferMetrics.BufferEntriesByType[0]}},
			{sinks: []*metric.Counter{m.AggregatorBufferMetrics.BufferEntriesByType[1], m.CombinedBufferMetrics.BufferEntriesByType[1]}},
			{sinks: []*metric.Counter{m.AggregatorBufferMetrics.BufferEntriesByType[2], m.CombinedBufferMetrics.BufferEntriesByType[2]}},
		},
	}
	m.RangefeedBufferMetricsWithCompat = PerBufferMetricsWithCompat{
		BufferType:               rangefeedBuffer,
		BufferEntriesIn:          &forwardingCounter{sinks: []*metric.Counter{m.RangefeedBufferMetrics.BufferEntriesIn, m.CombinedBufferMetrics.BufferEntriesIn}},
		BufferEntriesOut:         &forwardingCounter{sinks: []*metric.Counter{m.RangefeedBufferMetrics.BufferEntriesOut, m.CombinedBufferMetrics.BufferEntriesOut}},
		BufferEntriesReleased:    &forwardingCounter{sinks: []*metric.Counter{m.RangefeedBufferMetrics.BufferEntriesReleased, m.CombinedBufferMetrics.BufferEntriesReleased}},
		BufferEntriesMemAcquired: &forwardingCounter{sinks: []*metric.Counter{m.RangefeedBufferMetrics.BufferEntriesMemAcquired, m.CombinedBufferMetrics.BufferEntriesMemAcquired}},
		BufferEntriesMemReleased: &forwardingCounter{sinks: []*metric.Counter{m.RangefeedBufferMetrics.BufferEntriesMemReleased, m.CombinedBufferMetrics.BufferEntriesMemReleased}},
		BufferPushbackNanos:      &forwardingCounter{sinks: []*metric.Counter{m.RangefeedBufferMetrics.BufferPushbackNanos, m.CombinedBufferMetrics.BufferPushbackNanos}},
		AllocatedMem:             &forwardingGauge{sinks: []*metric.Gauge{m.RangefeedBufferMetrics.AllocatedMem, m.CombinedBufferMetrics.AllocatedMem}},
		BufferEntriesByType: [numEventTypes]*forwardingCounter{
			{sinks: []*metric.Counter{m.RangefeedBufferMetrics.BufferEntriesByType[0], m.CombinedBufferMetrics.BufferEntriesByType[0]}},
			{sinks: []*metric.Counter{m.RangefeedBufferMetrics.BufferEntriesByType[1], m.CombinedBufferMetrics.BufferEntriesByType[1]}},
			{sinks: []*metric.Counter{m.RangefeedBufferMetrics.BufferEntriesByType[2], m.CombinedBufferMetrics.BufferEntriesByType[2]}},
		},
	}
	return m
}

var _ (metric.Struct) = (*Metrics)(nil)

// MetricStruct makes Metrics a metric.Struct.
func (m Metrics) MetricStruct() {}
