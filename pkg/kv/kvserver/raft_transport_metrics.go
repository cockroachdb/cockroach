// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// RaftTransportMetrics is the set of metrics for a given RaftTransport.
type RaftTransportMetrics struct {
	SendQueueSize  *metric.Gauge
	SendQueueBytes *metric.Gauge

	MessagesDropped *metric.Counter
	MessagesSent    *metric.Counter
	MessagesRcvd    *metric.Counter

	ReverseSent *metric.Counter
	ReverseRcvd *metric.Counter
}

func (t *RaftTransport) initMetrics() {
	t.metrics = &RaftTransportMetrics{
		SendQueueSize: metric.NewFunctionalGauge(metric.Metadata{
			Name: "raft.transport.send-queue-size",
			Help: `Number of pending outgoing messages in the Raft Transport queue.

The queue is composed of multiple bounded channels associated with different
peers. The overall size of tens of thousands could indicate issues streaming
messages to at least one peer. Use this metric in conjunction with
send-queue-bytes.`,
			Measurement: "Messages",
			Unit:        metric.Unit_COUNT,
		}, t.queueMessageCount),

		SendQueueBytes: metric.NewFunctionalGauge(metric.Metadata{
			Name: "raft.transport.send-queue-bytes",
			Help: `The total byte size of pending outgoing messages in the queue.

The queue is composed of multiple bounded channels associated with different
peers. A size higher than the average baseline could indicate issues streaming
messages to at least one peer. Use this metric together with send-queue-size, to
have a fuller picture.`,
			Measurement: "Bytes",
			Unit:        metric.Unit_BYTES,
		}, t.queueByteSize),

		MessagesDropped: metric.NewCounter(metric.Metadata{
			Name:        "raft.transport.sends-dropped",
			Help:        "Number of Raft message sends dropped by the Raft Transport",
			Measurement: "Messages",
			Unit:        metric.Unit_COUNT,
		}),

		MessagesSent: metric.NewCounter(metric.Metadata{
			Name:        "raft.transport.sent",
			Help:        "Number of Raft messages sent by the Raft Transport",
			Measurement: "Messages",
			Unit:        metric.Unit_COUNT,
		}),

		MessagesRcvd: metric.NewCounter(metric.Metadata{
			Name:        "raft.transport.rcvd",
			Help:        "Number of Raft messages received by the Raft Transport",
			Measurement: "Messages",
			Unit:        metric.Unit_COUNT,
		}),

		ReverseSent: metric.NewCounter(metric.Metadata{
			Name: "raft.transport.reverse-sent",
			Help: `Messages sent in the reverse direction of a stream.

These messages should be rare. They are mostly informational, and are not actual
responses to Raft messages. Responses are sent over another stream.`,
			Measurement: "Messages",
			Unit:        metric.Unit_COUNT,
		}),

		ReverseRcvd: metric.NewCounter(metric.Metadata{
			Name: "raft.transport.reverse-rcvd",
			Help: `Messages received from the reverse direction of a stream.

These messages should be rare. They are mostly informational, and are not actual
responses to Raft messages. Responses are received over another stream.`,
			Measurement: "Messages",
			Unit:        metric.Unit_COUNT,
		}),
	}
}
