// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storeliveness

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// TransportMetrics includes all Store Liveness Transport metrics.
type TransportMetrics struct {
	SendQueueSize  *metric.Gauge
	SendQueueBytes *metric.Gauge
	SendQueueIdle  *metric.Counter

	MessagesSent     *metric.Counter
	MessagesReceived *metric.Counter
	MessagesDropped  *metric.Counter
}

func newTransportMetrics() *TransportMetrics {
	return &TransportMetrics{
		SendQueueSize:    metric.NewGauge(metaSendQueueSize),
		SendQueueBytes:   metric.NewGauge(metaSendQueueBytes),
		SendQueueIdle:    metric.NewCounter(metaSendQueueIdle),
		MessagesSent:     metric.NewCounter(metaMessagesSent),
		MessagesReceived: metric.NewCounter(metaMessagesReceived),
		MessagesDropped:  metric.NewCounter(metaMessagesDropped),
	}
}

var (
	metaSendQueueSize = metric.Metadata{
		Name: "storeliveness.transport.send-queue-size",
		Help: "Number of pending outgoing messages in all " +
			"Store Liveness Transport per-store send queues",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaSendQueueBytes = metric.Metadata{
		Name: "storeliveness.transport.send-queue-bytes",
		Help: "Total byte size of pending outgoing messages in all " +
			"Store Liveness Transport per-store send queues",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaSendQueueIdle = metric.Metadata{
		Name: "storeliveness.transport.send-queue-idle",
		Help: "Number of Store Liveness Transport per-store send queues " +
			"that have become idle due to no recently-sent messages",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaMessagesSent = metric.Metadata{
		Name: "storeliveness.transport.sent",
		Help: "Number of Store Liveness messages sent by the " +
			"Store Liveness Transport",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaMessagesReceived = metric.Metadata{
		Name: "storeliveness.transport.received",
		Help: "Number of Store Liveness messages received by the " +
			"Store Liveness Transport",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaMessagesDropped = metric.Metadata{
		Name: "storeliveness.transport.dropped",
		Help: "Number of Store Liveness messages dropped by the " +
			"Store Liveness Transport",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
)
