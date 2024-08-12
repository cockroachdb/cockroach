// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
	RangefeedBufferMetrics PerBufferMetrics
	KVFeedBufferMetrics    PerBufferMetrics
}

type PerBufferMetrics struct {
	Which                    bufferType
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
	rangefeedBuffer bufferType = "rangefeed"
	kvFeedBuffer    bufferType = "kvfeed"
)

// MakeMetrics constructs a Metrics struct with the provided histogram window.
func MakeMetrics(histogramWindow time.Duration) Metrics {
	// We don't want to rely on labels for this since tsdb doesn't support them, So make separate metrics for each buffer type.
	metaForBuf := func(bufType bufferType, meta metric.Metadata) metric.Metadata {
		meta.Name = fmt.Sprintf("%s.%s", meta.Name, bufType)
		return meta
	}

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
	return Metrics{
		RangefeedBufferMetrics: PerBufferMetrics{
			Which:                    rangefeedBuffer,
			BufferEntriesIn:          metric.NewCounter(metaForBuf(rangefeedBuffer, metaChangefeedBufferEntriesIn)),
			BufferEntriesOut:         metric.NewCounter(metaForBuf(rangefeedBuffer, metaChangefeedBufferEntriesOut)),
			BufferEntriesReleased:    metric.NewCounter(metaForBuf(rangefeedBuffer, metaChangefeedBufferEntriesReleased)),
			BufferEntriesMemAcquired: metric.NewCounter(metaForBuf(rangefeedBuffer, metaChangefeedBufferMemAcquired)),
			BufferEntriesMemReleased: metric.NewCounter(metaForBuf(rangefeedBuffer, metaChangefeedBufferMemReleased)),
			BufferPushbackNanos:      metric.NewCounter(metaForBuf(rangefeedBuffer, metaChangefeedBufferPushbackNanos)),
			AllocatedMem:             metric.NewGauge(metaForBuf(rangefeedBuffer, metaChangefeedAllocatedMemory)),
			BufferEntriesByType: [numEventTypes]*metric.Counter{
				metric.NewCounter(metaForBuf(rangefeedBuffer, eventTypeMeta(TypeFlush))),
				metric.NewCounter(metaForBuf(rangefeedBuffer, eventTypeMeta(TypeKV))),
				metric.NewCounter(metaForBuf(rangefeedBuffer, eventTypeMeta(TypeResolved))),
			},
		},
		KVFeedBufferMetrics: PerBufferMetrics{
			Which:                    kvFeedBuffer,
			BufferEntriesIn:          metric.NewCounter(metaForBuf(kvFeedBuffer, metaChangefeedBufferEntriesIn)),
			BufferEntriesOut:         metric.NewCounter(metaForBuf(kvFeedBuffer, metaChangefeedBufferEntriesOut)),
			BufferEntriesReleased:    metric.NewCounter(metaForBuf(kvFeedBuffer, metaChangefeedBufferEntriesReleased)),
			BufferEntriesMemAcquired: metric.NewCounter(metaForBuf(kvFeedBuffer, metaChangefeedBufferMemAcquired)),
			BufferEntriesMemReleased: metric.NewCounter(metaForBuf(kvFeedBuffer, metaChangefeedBufferMemReleased)),
			BufferPushbackNanos:      metric.NewCounter(metaForBuf(kvFeedBuffer, metaChangefeedBufferPushbackNanos)),
			AllocatedMem:             metric.NewGauge(metaForBuf(kvFeedBuffer, metaChangefeedAllocatedMemory)),
			BufferEntriesByType: [numEventTypes]*metric.Counter{
				metric.NewCounter(metaForBuf(kvFeedBuffer, eventTypeMeta(TypeFlush))),
				metric.NewCounter(metaForBuf(kvFeedBuffer, eventTypeMeta(TypeKV))),
				metric.NewCounter(metaForBuf(kvFeedBuffer, eventTypeMeta(TypeResolved))),
			},
		},
	}
}

var _ (metric.Struct) = (*Metrics)(nil)

// MetricStruct makes Metrics a metric.Struct.
func (m Metrics) MetricStruct() {}
