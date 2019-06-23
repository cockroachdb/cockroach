// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gossip

import (
	"bytes"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// Metrics contains gossip metrics used per node and server.
type Metrics struct {
	ConnectionsRefused *metric.Counter
	BytesReceived      *metric.Counter
	BytesSent          *metric.Counter
	InfosReceived      *metric.Counter
	InfosSent          *metric.Counter
}

func makeMetrics() Metrics {
	return Metrics{
		ConnectionsRefused: metric.NewCounter(MetaConnectionsRefused),
		BytesReceived:      metric.NewCounter(MetaBytesReceived),
		BytesSent:          metric.NewCounter(MetaBytesSent),
		InfosReceived:      metric.NewCounter(MetaInfosReceived),
		InfosSent:          metric.NewCounter(MetaInfosSent),
	}
}

func (m Metrics) String() string {
	return m.Snapshot().String()
}

// Snapshot returns a snapshot of the metrics.
func (m Metrics) Snapshot() MetricSnap {
	return MetricSnap{
		ConnsRefused:  m.ConnectionsRefused.Count(),
		BytesReceived: m.BytesReceived.Count(),
		BytesSent:     m.BytesSent.Count(),
		InfosReceived: m.InfosReceived.Count(),
		InfosSent:     m.InfosSent.Count(),
	}
}

func (m MetricSnap) String() string {
	s := fmt.Sprintf("infos %d/%d sent/received, bytes %dB/%dB sent/received",
		m.InfosSent, m.InfosReceived, m.BytesSent, m.BytesReceived)
	if m.ConnsRefused > 0 {
		s += fmt.Sprintf(", refused %d conns", m.ConnsRefused)
	}
	return s
}

func (c OutgoingConnStatus) String() string {
	return fmt.Sprintf("%d: %s (%s: %s)",
		c.NodeID, c.Address, roundSecs(time.Duration(c.AgeNanos)), c.MetricSnap)
}

func (c ClientStatus) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "gossip client (%d/%d cur/max conns)\n", len(c.ConnStatus), c.MaxConns)
	for _, conn := range c.ConnStatus {
		fmt.Fprintf(&buf, "  %s\n", conn)
	}
	return buf.String()
}

func (c ConnStatus) String() string {
	return fmt.Sprintf("%d: %s (%s)", c.NodeID, c.Address, roundSecs(time.Duration(c.AgeNanos)))
}

func (s ServerStatus) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "gossip server (%d/%d cur/max conns, %s)\n",
		len(s.ConnStatus), s.MaxConns, s.MetricSnap)
	for _, conn := range s.ConnStatus {
		fmt.Fprintf(&buf, "  %s\n", conn)
	}
	return buf.String()
}

func (c Connectivity) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "gossip connectivity\n")
	if c.SentinelNodeID != 0 {
		fmt.Fprintf(&buf, "  n%d [sentinel];\n", c.SentinelNodeID)
	}
	if len(c.ClientConns) > 0 {
		fmt.Fprintf(&buf, " ")
		for _, conn := range c.ClientConns {
			fmt.Fprintf(&buf, " n%d -> n%d;", conn.SourceID, conn.TargetID)
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}
