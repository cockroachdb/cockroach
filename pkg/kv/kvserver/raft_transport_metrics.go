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
	SendQueueSize *metric.Gauge
}

func (t *RaftTransport) initMetrics() {
	t.metrics = &RaftTransportMetrics{
		SendQueueSize: metric.NewFunctionalGauge(metric.Metadata{
			Name: "raft.transport.send-queue-size",
			Help: `Number of pending outgoing messages in the Raft Transport queue.

The queue is bounded in size, so instead of unbounded growth one would observe a
ceiling value in the tens of thousands.`,
			Measurement: "Messages",
			Unit:        metric.Unit_COUNT,
		}, t.queuedMessageCount),
	}
}
