// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// minCallbackDurationToRecord is the minimum duration for which we record
// callback processing durations. This skews the histogram, but avoids recording
// very short durations which are not interesting and which will be dominated by
// the overhead of recording the duration itself.
const minCallbackDurationToRecord = 10 * time.Millisecond

// TransportMetrics includes all Store Liveness Transport metrics.
type TransportMetrics struct {
	SendQueueSize  *metric.Gauge
	SendQueueBytes *metric.Gauge
	SendQueueIdle  *metric.Counter

	MessagesSent           *metric.Counter
	MessagesReceived       *metric.Counter
	MessagesSendDropped    *metric.Counter
	MessagesReceiveDropped *metric.Counter

	BatchesSent      *metric.Counter
	MessagesPerBatch metric.IHistogram
	BatchSizeBytes   metric.IHistogram
	BatchDuration    metric.IHistogram
}

func newTransportMetrics() *TransportMetrics {
	return &TransportMetrics{
		SendQueueSize:          metric.NewGauge(metaSendQueueSize),
		SendQueueBytes:         metric.NewGauge(metaSendQueueBytes),
		SendQueueIdle:          metric.NewCounter(metaSendQueueIdle),
		MessagesSent:           metric.NewCounter(metaMessagesSent),
		MessagesReceived:       metric.NewCounter(metaMessagesReceived),
		MessagesSendDropped:    metric.NewCounter(metaMessagesSendDropped),
		MessagesReceiveDropped: metric.NewCounter(metaMessagesReceiveDropped),
		BatchesSent:            metric.NewCounter(metaBatchesSent),
		MessagesPerBatch: metric.NewHistogram(
			metric.HistogramOptions{
				Mode:         metric.HistogramModePreferHdrLatency,
				Metadata:     metaMessagesPerBatch,
				Duration:     base.DefaultHistogramWindowInterval(),
				BucketConfig: metric.Count1KBuckets,
			},
		),
		BatchSizeBytes: metric.NewHistogram(
			metric.HistogramOptions{
				Mode:         metric.HistogramModePreferHdrLatency,
				Metadata:     metaBatchSizeBytes,
				Duration:     base.DefaultHistogramWindowInterval(),
				BucketConfig: metric.DataSize16MBBuckets,
			},
		),
		BatchDuration: metric.NewHistogram(
			metric.HistogramOptions{
				Mode:         metric.HistogramModePreferHdrLatency,
				Metadata:     metaBatchDuration,
				Duration:     base.DefaultHistogramWindowInterval(),
				BucketConfig: metric.IOLatencyBuckets,
			},
		),
	}
}

// SupportManagerMetrics includes all Store Liveness SupportManager metrics.
type SupportManagerMetrics struct {
	HeartbeatSuccesses          *metric.Counter
	HeartbeatFailures           *metric.Counter
	MessageHandleSuccesses      *metric.Counter
	MessageHandleFailures       *metric.Counter
	SupportWithdrawSuccesses    *metric.Counter
	SupportWithdrawFailures     *metric.Counter
	CallbacksProcessingDuration metric.IHistogram
	SupportFromStores           *metric.Gauge
	SupportForStores            *metric.Gauge

	ReceiveQueueSize  *metric.Gauge
	ReceiveQueueBytes *metric.Gauge
}

func newSupportManagerMetrics() *SupportManagerMetrics {
	return &SupportManagerMetrics{
		HeartbeatSuccesses:       metric.NewCounter(metaHeartbeatSuccesses),
		HeartbeatFailures:        metric.NewCounter(metaHeartbeatFailures),
		MessageHandleSuccesses:   metric.NewCounter(metaMessageHandleSuccesses),
		MessageHandleFailures:    metric.NewCounter(metaMessageHandleFailures),
		SupportWithdrawSuccesses: metric.NewCounter(metaSupportWithdrawSuccesses),
		SupportWithdrawFailures:  metric.NewCounter(metaSupportWithdrawFailures),
		CallbacksProcessingDuration: metric.NewHistogram(
			metric.HistogramOptions{
				Mode:         metric.HistogramModePreferHdrLatency,
				Metadata:     metaCallbacksProcessingDuration,
				Duration:     base.DefaultHistogramWindowInterval(),
				BucketConfig: metric.IOLatencyBuckets,
			},
		),
		SupportFromStores: metric.NewGauge(metaSupportFromStores),
		SupportForStores:  metric.NewGauge(metaSupportForStores),
		ReceiveQueueSize:  metric.NewGauge(metaReceiveQueueSize),
		ReceiveQueueBytes: metric.NewGauge(metaReceiveQueueBytes),
	}
}

// HeartbeatCoordinatorMetrics includes all HeartbeatCoordinator metrics.
type HeartbeatCoordinatorMetrics struct {
	// Message counts
	MessagesEnqueued      *metric.Counter
	MessagesSent          *metric.Counter
	MessagesSentImmediate *metric.Counter // Responses that bypass smearing

	// Queue metrics
	ActiveQueues          *metric.Gauge
	TotalMessagesInQueues *metric.Gauge

	// Timing metrics
	TickDuration  *metric.Gauge
	SmearDuration *metric.Gauge

	// Error metrics
	SendErrors  *metric.Counter
	PacerErrors *metric.Counter
}

func newHeartbeatCoordinatorMetrics() *HeartbeatCoordinatorMetrics {
	return &HeartbeatCoordinatorMetrics{
		MessagesEnqueued:      metric.NewCounter(metaHeartbeatCoordinatorMessagesEnqueued),
		MessagesSent:          metric.NewCounter(metaHeartbeatCoordinatorMessagesSent),
		MessagesSentImmediate: metric.NewCounter(metaHeartbeatCoordinatorMessagesSentImmediate),
		ActiveQueues:          metric.NewGauge(metaHeartbeatCoordinatorActiveQueues),
		TotalMessagesInQueues: metric.NewGauge(metaHeartbeatCoordinatorTotalMessagesInQueues),
		TickDuration:          metric.NewGauge(metaHeartbeatCoordinatorTickDuration),
		SmearDuration:         metric.NewGauge(metaHeartbeatCoordinatorSmearDuration),
		SendErrors:            metric.NewCounter(metaHeartbeatCoordinatorSendErrors),
		PacerErrors:           metric.NewCounter(metaHeartbeatCoordinatorPacerErrors),
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
	metaMessagesSendDropped = metric.Metadata{
		Name: "storeliveness.transport.send_dropped",
		Help: "Number of Store Liveness messages dropped by the " +
			"Store Liveness Transport on the sender side",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaMessagesReceiveDropped = metric.Metadata{
		Name: "storeliveness.transport.receive_dropped",
		Help: "Number of Store Liveness messages dropped by the " +
			"Store Liveness Transport on the receiver side",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatSuccesses = metric.Metadata{
		Name: "storeliveness.heartbeat.successes",
		Help: "Number of Store Liveness heartbeats sent out by the " +
			"Store Liveness Support Manager",
		Measurement: "Heartbeats",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatFailures = metric.Metadata{
		Name: "storeliveness.heartbeat.failures",
		Help: "Number of Store Liveness heartbeats that failed to be sent out by the " +
			"Store Liveness Support Manager",
		Measurement: "Heartbeats",
		Unit:        metric.Unit_COUNT,
	}
	metaMessageHandleSuccesses = metric.Metadata{
		Name: "storeliveness.message_handle.successes",
		Help: "Number of incoming Store Liveness messages handled by the " +
			"Store Liveness Support Manager",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaMessageHandleFailures = metric.Metadata{
		Name: "storeliveness.message_handle.failures",
		Help: "Number of incoming Store Liveness messages that failed to be handled by the " +
			"Store Liveness Support Manager",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaSupportWithdrawSuccesses = metric.Metadata{
		Name: "storeliveness.support_withdraw.successes",
		Help: "Number of times the Store Liveness Support Manager has successfully withdrawn " +
			"support for another store",
		Measurement: "Support Withdrawals",
		Unit:        metric.Unit_COUNT,
	}
	metaSupportWithdrawFailures = metric.Metadata{
		Name: "storeliveness.support_withdraw.failures",
		Help: "Number of times the Store Liveness Support Manager has encountered an error " +
			"while withdrawing support for another store",
		Measurement: "Support Withdrawals",
		Unit:        metric.Unit_COUNT,
	}
	metaSupportFromStores = metric.Metadata{
		Name: "storeliveness.support_from.stores",
		Help: "Number of stores that the Store Liveness Support Manager is requesting " +
			"support from by sending heartbeats",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
	}
	metaSupportForStores = metric.Metadata{
		Name: "storeliveness.support_for.stores",
		Help: "Number of stores that the Store Liveness Support Manager has ever " +
			"provided support for",
		Measurement: "Stores",
		Unit:        metric.Unit_COUNT,
	}
	metaReceiveQueueSize = metric.Metadata{
		Name: "storeliveness.transport.receive-queue-size",
		Help: "Number of pending incoming messages from the " +
			"Store Liveness Transport",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaReceiveQueueBytes = metric.Metadata{
		Name: "storeliveness.transport.receive-queue-bytes",
		Help: "Total byte size of pending incoming messages from " +
			"Store Liveness Transport",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	metaCallbacksProcessingDuration = metric.Metadata{
		Name:        "storeliveness.callbacks.processing_duration",
		Help:        "Duration of support withdrawal callback processing",
		Measurement: "Duration",
		Unit:        metric.Unit_NANOSECONDS,
	}

	// HeartbeatCoordinator metric metadata
	metaHeartbeatCoordinatorMessagesEnqueued = metric.Metadata{
		Name:        "storeliveness.heartbeat_coordinator.messages_enqueued",
		Help:        "Total number of heartbeat messages enqueued for smearing",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatCoordinatorMessagesSent = metric.Metadata{
		Name:        "storeliveness.heartbeat_coordinator.messages_sent",
		Help:        "Total number of heartbeat messages sent via transport",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatCoordinatorMessagesSentImmediate = metric.Metadata{
		Name:        "storeliveness.heartbeat_coordinator.messages_sent_immediate",
		Help:        "Total number of heartbeat response messages sent immediately (bypassing smearing)",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatCoordinatorActiveQueues = metric.Metadata{
		Name:        "storeliveness.heartbeat_coordinator.active_queues",
		Help:        "Current number of active destination queues",
		Measurement: "Queues",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatCoordinatorTotalMessagesInQueues = metric.Metadata{
		Name:        "storeliveness.heartbeat_coordinator.total_messages_in_queues",
		Help:        "Current total number of messages across all queues",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatCoordinatorTickDuration = metric.Metadata{
		Name:        "storeliveness.heartbeat_coordinator.tick_duration_ms",
		Help:        "Heartbeat tick duration in milliseconds",
		Measurement: "Duration",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatCoordinatorSmearDuration = metric.Metadata{
		Name:        "storeliveness.heartbeat_coordinator.smear_duration_ms",
		Help:        "Heartbeat smear duration in milliseconds",
		Measurement: "Duration",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatCoordinatorSendErrors = metric.Metadata{
		Name:        "storeliveness.heartbeat_coordinator.send_errors",
		Help:        "Total number of errors sending heartbeat messages",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatCoordinatorPacerErrors = metric.Metadata{
		Name:        "storeliveness.heartbeat_coordinator.pacer_errors",
		Help:        "Total number of errors in the pacer",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}

	metaBatchesSent = metric.Metadata{
		Name:        "storeliveness.transport.batches-sent",
		Help:        "Number of message batches sent by the Store Liveness Transport",
		Measurement: "Batches",
		Unit:        metric.Unit_COUNT,
	}
	metaMessagesPerBatch = metric.Metadata{
		Name:        "storeliveness.transport.messages-per-batch",
		Help:        "Number of messages per batch sent by the Store Liveness Transport",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaBatchSizeBytes = metric.Metadata{
		Name:        "storeliveness.transport.batch-size-bytes",
		Help:        "Size in bytes of batches sent by the Store Liveness Transport",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaBatchDuration = metric.Metadata{
		Name:        "storeliveness.transport.batch-duration",
		Help:        "Duration spent collecting messages into batches by the Store Liveness Transport",
		Measurement: "Duration",
		Unit:        metric.Unit_NANOSECONDS,
	}
)
