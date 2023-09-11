// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package streamproducer

import (
	"fmt"
	math "math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/gogo/protobuf/proto"
	"go.opentelemetry.io/otel/attribute"
)

var _ bulk.TracingAggregatorEvent = &EventStreamPerformanceStats{}
var _ jobs.ProtobinExecutionDetailFile = &EventStreamPerformanceStats{}

func (b *Bar) String() string {
	return fmt.Sprintf("%v, %v, %v\n", b.From, b.To, b.Count)
}

func (h *HistogramData) String() string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("min: %.6f\n", float64(h.Min)/float64(time.Second)))
	b.WriteString(fmt.Sprintf("max: %.6f\n", float64(h.Max)/float64(time.Second)))
	b.WriteString(fmt.Sprintf("p5: %.6f\n", float64(h.P5)/float64(time.Second)))
	b.WriteString(fmt.Sprintf("p50: %.6f\n", float64(h.P50)/float64(time.Second)))
	b.WriteString(fmt.Sprintf("p90: %.6f\n", float64(h.P90)/float64(time.Second)))
	b.WriteString(fmt.Sprintf("p99: %.6f\n", float64(h.P99)/float64(time.Second)))
	b.WriteString(fmt.Sprintf("p99_9: %.6f\n", float64(h.P99_9)/float64(time.Second)))
	b.WriteString(fmt.Sprintf("mean: %.6f\n", h.Mean/float32(time.Second)))
	b.WriteString(fmt.Sprintf("count: %d\n", h.Count))

	return b.String()
}

func (m *EventStreamPerformanceStats) ToText() []byte {
	return []byte(m.String())
}

func (m *EventStreamPerformanceStats) Render() []attribute.KeyValue {
	return nil
}

func (m *EventStreamPerformanceStats) Identity() bulk.TracingAggregatorEvent {
	return &EventStreamPerformanceStats{
		LastRecvTime:  hlc.Timestamp{WallTime: math.MinInt64},
		LastFlushTime: hlc.Timestamp{WallTime: math.MinInt64},
		LastSendTime:  hlc.Timestamp{WallTime: math.MinInt64},
	}
}

func timeString(b *strings.Builder, key string, time time.Duration) {
	b.WriteString(fmt.Sprintf("%s: %s\n", key, string(humanizeutil.Duration(time))))
}

// String implements the stringer interface.
func (m *EventStreamPerformanceStats) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("name: %s\n", m.Name))
	totalEventsReceived := m.KvEvents + m.SstEvents + m.CheckpointEvents + m.DeleteRangeEvents
	b.WriteString(fmt.Sprintf("kv_events: %d\n", m.KvEvents))
	b.WriteString(fmt.Sprintf("sst_events: %d\n", m.SstEvents))
	b.WriteString(fmt.Sprintf("checkpoint_events: %d\n", m.CheckpointEvents))
	b.WriteString(fmt.Sprintf("delete_range_events: %d\n", m.DeleteRangeEvents))
	b.WriteString(fmt.Sprintf("total_events: %d\n", totalEventsReceived))

	b.WriteString(fmt.Sprintf("num_trimmed_ssts: %d\n", m.NumTrimmedSsts))
	b.WriteString(fmt.Sprintf("num_flushed_batches: %d\n", m.NumFlushedBatches))
	b.WriteString(fmt.Sprintf("num_flushed_checkpoints: %d\n", m.NumFlushedCheckpoints))

	if totalEventsReceived > 0 {
		recvdWaitTimePerEvent := m.EventReceiveWait.Seconds() / float64(totalEventsReceived)
		b.WriteString(fmt.Sprintf("recvd_wait_per_event: %.6f seconds per event\n", recvdWaitTimePerEvent))
	}

	totalStreamEventsFlushed := m.NumFlushedBatches + m.NumFlushedCheckpoints
	if totalStreamEventsFlushed > 0 {
		flushWaitPerEvent := m.FlushWait.Seconds() / float64(totalStreamEventsFlushed)
		b.WriteString(fmt.Sprintf("flush_wait_per_event: %.6f seconds per event\n", flushWaitPerEvent))

		sendWaitPerEvent := m.EventSendWait.Seconds() / float64(totalStreamEventsFlushed)
		b.WriteString(fmt.Sprintf("send_wait_per_event: %.6f seocnds per event\n", sendWaitPerEvent))
	}

	timeString(&b, "event_receive_wait", m.EventReceiveWait)
	timeString(&b, "stream_event_flush_wait", m.FlushWait)
	timeString(&b, "stream_event_send_wait", m.EventSendWait)

	b.WriteString(fmt.Sprintf("last_recv_time: %s\n", m.LastRecvTime.GoTime()))
	b.WriteString(fmt.Sprintf("last_stream_event_flush_time: %s\n", m.LastFlushTime.GoTime()))
	b.WriteString(fmt.Sprintf("last_stream_event_send_time: %s\n\n", m.LastSendTime.GoTime()))
	b.WriteString(fmt.Sprintf("last_span_complete_time: %s\n", m.LastSpanCompleteTime.GoTime()))

	b.WriteString(fmt.Sprintf("rangefeed_event_wait: \n%s\n", m.RangefeedEventWait.String()))
	b.WriteString(fmt.Sprintf("flush_event_wait: \n%s\n", m.FlushEventWait.String()))
	b.WriteString(fmt.Sprintf("send_event_wait: \n%s\n", m.SendEventWait.String()))
	b.WriteString(fmt.Sprintf("since_next_wait: \n%s\n", m.SinceNextWait.String()))
	b.WriteString(fmt.Sprintf("since_last_batch_flush: \n%s\n", m.SinceLastBatchFlush.String()))
	b.WriteString(fmt.Sprintf("since_last_checkpoint_flush: \n%s\n", m.SinceLastCheckpointFlush.String()))

	return b.String()
}

func (m *EventStreamPerformanceStats) Combine(other bulk.TracingAggregatorEvent) {
	otherStats, ok := other.(*EventStreamPerformanceStats)
	if !ok {
		panic(fmt.Sprintf("`other` is not of type EventStreamPerformanceStats: %T", other))
	}

	m.Name = otherStats.Name
	m.KvEvents += otherStats.KvEvents
	m.SstEvents += otherStats.SstEvents
	m.CheckpointEvents += otherStats.CheckpointEvents
	m.DeleteRangeEvents += otherStats.DeleteRangeEvents
	m.NumTrimmedSsts += otherStats.NumTrimmedSsts
	m.NumFlushedBatches += otherStats.NumFlushedBatches
	m.NumFlushedCheckpoints += otherStats.NumFlushedCheckpoints

	m.EventReceiveWait += otherStats.EventReceiveWait
	m.FlushWait += otherStats.FlushWait
	m.EventSendWait += otherStats.EventSendWait

	if m.LastRecvTime.Less(otherStats.LastRecvTime) {
		m.LastRecvTime = otherStats.LastRecvTime
	}
	if m.LastFlushTime.Less(otherStats.LastFlushTime) {
		m.LastFlushTime = otherStats.LastFlushTime
	}
	if m.LastSendTime.Less(otherStats.LastSendTime) {
		m.LastSendTime = otherStats.LastSendTime
	}
	if m.LastSpanCompleteTime.Less(otherStats.LastSpanCompleteTime) {
		m.LastSpanCompleteTime = otherStats.LastSpanCompleteTime
	}

	m.RangefeedEventWait = otherStats.RangefeedEventWait
	m.SendEventWait = otherStats.SendEventWait
	m.FlushEventWait = otherStats.FlushEventWait
	m.SinceNextWait = otherStats.SinceNextWait
	m.SinceLastCheckpointFlush = otherStats.SinceLastCheckpointFlush
	m.SinceLastBatchFlush = otherStats.SinceLastBatchFlush

	m.RangefeedEventBars = otherStats.RangefeedEventBars
	m.FlushEventBars = otherStats.FlushEventBars
	m.SendEventBars = otherStats.SendEventBars
}

func (m *EventStreamPerformanceStats) ProtoName() string {
	return proto.MessageName(m)
}
