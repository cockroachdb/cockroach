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

// Raft transport metrics.
var (
	metaRaftClientQueueSize = metric.Metadata{
		Name:        "raft.client.queue-size",
		Help:        "Number of Raft requests in the outgoing queue",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}

	metaRaftClientSentMessages = metric.Metadata{
		Name:        "raft.client.sent-messages",
		Help:        "Number of Raft requests sent",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}

	metaRaftClientRecvMessages = metric.Metadata{
		Name:        "raft.client.recv-messages",
		Help:        "Number of Raft replies received",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}

	metaRaftClientDroppedMessages = metric.Metadata{
		Name:        "raft.client.dropped-messages",
		Help:        "Number of Raft requests dropped",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}

	metaRaftServerSentMessages = metric.Metadata{
		Name:        "raft.server.sent-messages",
		Help:        "Number of Raft replies sent",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}

	metaRaftServerRecvMessages = metric.Metadata{
		Name:        "raft.server.recv-messages",
		Help:        "Number of Raft requests received",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
)

// RaftTransportMetrics is the set of metrics for a given Raft transport.
type RaftTransportMetrics struct {
	registry *metric.Registry

	RaftClientQueueSize       *metric.Gauge
	RaftClientSentMessages    *metric.Counter
	RaftClientRecvMessages    *metric.Counter
	RaftClientDroppedMessages *metric.Counter
	RaftServerSentMessages    *metric.Counter
	RaftServerRecvMessages    *metric.Counter
}

func newRaftTransportMetrics() *RaftTransportMetrics {
	reg := metric.NewRegistry()
	m := &RaftTransportMetrics{
		registry:                  reg,
		RaftClientQueueSize:       metric.NewGauge(metaRaftClientQueueSize),
		RaftClientSentMessages:    metric.NewCounter(metaRaftClientSentMessages),
		RaftClientRecvMessages:    metric.NewCounter(metaRaftClientRecvMessages),
		RaftClientDroppedMessages: metric.NewCounter(metaRaftClientDroppedMessages),
		RaftServerSentMessages:    metric.NewCounter(metaRaftServerSentMessages),
		RaftServerRecvMessages:    metric.NewCounter(metaRaftServerRecvMessages),
	}
	reg.AddMetricStruct(m)
	return m
}
