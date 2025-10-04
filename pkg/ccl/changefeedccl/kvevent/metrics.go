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
	// These metrics don't make sense to be per-buffer, so we expose them here.
	// This is because we only allocate and release memory once across both
	// buffers (for most events).
	BufferEntriesMemAcquired *metric.Counter
	BufferEntriesMemReleased *metric.Counter
	AllocatedMem             *metric.Gauge

	// RangefeedBufferMetricsForRegistration tracks metrics for the buffer between the rangefeed and the kvfeed.
	// It's exposed to get registered as a metric.Struct.
	RangefeedBufferMetricsForRegistration PerBufferMetrics
	// AggregatorBufferMetricsForRegistration tracks metrics for the buffer between the kvfeed and the sink.
	// It's exposed to get registered as a metric.Struct.
	AggregatorBufferMetricsForRegistration PerBufferMetrics
	// CombinedBufferMetricsForRegistration tracks the combined metrics for both buffers, for
	// historical reasons. It's exposed to get registered as a metric.Struct.
	CombinedBufferMetricsForRegistration PerBufferMetrics

	// RangefeedBufferMetrics tracks metrics for the buffer between
	// the rangefeed and the kvfeed, while forwarding the old metrics for compatibility.
	// This should be the version used by the kvfeed, and it's exposed for that purpose.
	RangefeedBufferMetrics BufferMetrics
	// AggregatorBufferMetrics tracks metrics for the buffer between
	// the kvfeed and the sink, while forwarding the old metrics for compatibility.
	// This should be the version used by the kvfeed, and it's exposed for that purpose.
	AggregatorBufferMetrics BufferMetrics
}

// BufferMetrics is the struct passed to BlockingBuffers. It forwards per-buffer
// metrics where appropriate and contains pointers to shared metrics otherwise.
// Note that it is not a metric.Struct, to prevent duplicate registrations.
type BufferMetrics struct {
	BufferType            bufferType
	BufferEntriesIn       *forwardingCounter
	BufferEntriesOut      *forwardingCounter
	BufferEntriesReleased *forwardingCounter
	BufferPushbackNanos   *forwardingCounter
	BufferEntriesByType   [numEventTypes]*forwardingCounter

	// Pointers to shared metrics.
	BufferEntriesMemAcquired counterPointer
	BufferEntriesMemReleased counterPointer
	AllocatedMem             gaugePointer
}

// counterPointer is a wrapper around a *metric.Counter that does not implement
// the metric.Iterable interface, and will not cause the metric auto
// registration system to complain about its existence.
type counterPointer struct {
	metric *metric.Counter
}

func (mp *counterPointer) Inc(n int64) {
	mp.metric.Inc(n)
}

func (mp *counterPointer) Count() int64 {
	return mp.metric.Count()
}

// gaugePointer is a wrapper around a *metric.Gauge that does not implement
// the metric.Iterable interface, and will not cause the metric auto
// registration system to complain about its existence.
type gaugePointer struct {
	metric *metric.Gauge
}

func (mp *gaugePointer) Inc(i int64) {
	mp.metric.Inc(i)
}

func (mp *gaugePointer) Dec(i int64) {
	mp.metric.Dec(i)
}

func (mp *gaugePointer) Value() int64 {
	return mp.metric.Value()
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

type PerBufferMetrics struct {
	BufferType            bufferType
	BufferEntriesIn       *metric.Counter
	BufferEntriesOut      *metric.Counter
	BufferEntriesReleased *metric.Counter
	BufferPushbackNanos   *metric.Counter
	BufferEntriesByType   [numEventTypes]*metric.Counter
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
		BufferEntriesMemAcquired: metric.NewCounter(metaChangefeedBufferMemAcquired),
		BufferEntriesMemReleased: metric.NewCounter(metaChangefeedBufferMemReleased),
		AllocatedMem:             metric.NewGauge(metaChangefeedAllocatedMemory),

		RangefeedBufferMetricsForRegistration: PerBufferMetrics{
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
		},
		AggregatorBufferMetricsForRegistration: PerBufferMetrics{
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
		},
		CombinedBufferMetricsForRegistration: PerBufferMetrics{
			BufferType:            aggregatorBuffer,
			BufferEntriesIn:       metric.NewCounter(metaChangefeedBufferEntriesIn),
			BufferEntriesOut:      metric.NewCounter(metaChangefeedBufferEntriesOut),
			BufferEntriesReleased: metric.NewCounter(metaChangefeedBufferEntriesReleased),
			BufferPushbackNanos:   metric.NewCounter(metaChangefeedBufferPushbackNanos),
			BufferEntriesByType: [numEventTypes]*metric.Counter{
				metric.NewCounter(eventTypeMeta(TypeFlush)),
				metric.NewCounter(eventTypeMeta(TypeKV)),
				metric.NewCounter(eventTypeMeta(TypeResolved)),
			},
		},
	}

	m.AggregatorBufferMetrics = BufferMetrics{
		BufferType:               aggregatorBuffer,
		BufferEntriesIn:          &forwardingCounter{sinks: []*metric.Counter{m.AggregatorBufferMetricsForRegistration.BufferEntriesIn, m.CombinedBufferMetricsForRegistration.BufferEntriesIn}},
		BufferEntriesOut:         &forwardingCounter{sinks: []*metric.Counter{m.AggregatorBufferMetricsForRegistration.BufferEntriesOut, m.CombinedBufferMetricsForRegistration.BufferEntriesOut}},
		BufferEntriesReleased:    &forwardingCounter{sinks: []*metric.Counter{m.AggregatorBufferMetricsForRegistration.BufferEntriesReleased, m.CombinedBufferMetricsForRegistration.BufferEntriesReleased}},
		BufferEntriesMemAcquired: counterPointer{metric: m.BufferEntriesMemAcquired},
		BufferEntriesMemReleased: counterPointer{metric: m.BufferEntriesMemReleased},
		BufferPushbackNanos:      &forwardingCounter{sinks: []*metric.Counter{m.AggregatorBufferMetricsForRegistration.BufferPushbackNanos, m.CombinedBufferMetricsForRegistration.BufferPushbackNanos}},
		AllocatedMem:             gaugePointer{metric: m.AllocatedMem},
		BufferEntriesByType: [numEventTypes]*forwardingCounter{
			{sinks: []*metric.Counter{m.AggregatorBufferMetricsForRegistration.BufferEntriesByType[0], m.CombinedBufferMetricsForRegistration.BufferEntriesByType[0]}},
			{sinks: []*metric.Counter{m.AggregatorBufferMetricsForRegistration.BufferEntriesByType[1], m.CombinedBufferMetricsForRegistration.BufferEntriesByType[1]}},
			{sinks: []*metric.Counter{m.AggregatorBufferMetricsForRegistration.BufferEntriesByType[2], m.CombinedBufferMetricsForRegistration.BufferEntriesByType[2]}},
		},
	}
	m.RangefeedBufferMetrics = BufferMetrics{
		BufferType:               rangefeedBuffer,
		BufferEntriesIn:          &forwardingCounter{sinks: []*metric.Counter{m.RangefeedBufferMetricsForRegistration.BufferEntriesIn, m.CombinedBufferMetricsForRegistration.BufferEntriesIn}},
		BufferEntriesOut:         &forwardingCounter{sinks: []*metric.Counter{m.RangefeedBufferMetricsForRegistration.BufferEntriesOut, m.CombinedBufferMetricsForRegistration.BufferEntriesOut}},
		BufferEntriesReleased:    &forwardingCounter{sinks: []*metric.Counter{m.RangefeedBufferMetricsForRegistration.BufferEntriesReleased, m.CombinedBufferMetricsForRegistration.BufferEntriesReleased}},
		BufferEntriesMemAcquired: counterPointer{metric: m.BufferEntriesMemAcquired},
		BufferEntriesMemReleased: counterPointer{metric: m.BufferEntriesMemReleased},
		BufferPushbackNanos:      &forwardingCounter{sinks: []*metric.Counter{m.RangefeedBufferMetricsForRegistration.BufferPushbackNanos, m.CombinedBufferMetricsForRegistration.BufferPushbackNanos}},
		AllocatedMem:             gaugePointer{metric: m.AllocatedMem},
		BufferEntriesByType: [numEventTypes]*forwardingCounter{
			{sinks: []*metric.Counter{m.RangefeedBufferMetricsForRegistration.BufferEntriesByType[0], m.CombinedBufferMetricsForRegistration.BufferEntriesByType[0]}},
			{sinks: []*metric.Counter{m.RangefeedBufferMetricsForRegistration.BufferEntriesByType[1], m.CombinedBufferMetricsForRegistration.BufferEntriesByType[1]}},
			{sinks: []*metric.Counter{m.RangefeedBufferMetricsForRegistration.BufferEntriesByType[2], m.CombinedBufferMetricsForRegistration.BufferEntriesByType[2]}},
		},
	}
	return m
}

var _ (metric.Struct) = (*Metrics)(nil)

// MetricStruct makes Metrics a metric.Struct.
func (m Metrics) MetricStruct() {}
