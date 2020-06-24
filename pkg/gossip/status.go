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
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/redact"
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
	return redact.StringWithoutMarkers(m.Snapshot())
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
	return redact.StringWithoutMarkers(m)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (m MetricSnap) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("infos %d/%d sent/received, bytes %dB/%dB sent/received",
		m.InfosSent, m.InfosReceived,
		m.BytesSent, m.BytesReceived)
	if m.ConnsRefused > 0 {
		w.Printf(", refused %d conns", m.ConnsRefused)
	}
}

func (c OutgoingConnStatus) String() string {
	return redact.StringWithoutMarkers(c)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (c OutgoingConnStatus) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("%d: %s (%s: %s)",
		c.NodeID, c.Address,
		roundSecs(time.Duration(c.AgeNanos)), c.MetricSnap)
}

func (c ClientStatus) String() string {
	return redact.StringWithoutMarkers(c)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (c ClientStatus) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("gossip client (%d/%d cur/max conns)\n",
		len(c.ConnStatus), c.MaxConns)
	for _, conn := range c.ConnStatus {
		w.Printf("  %s\n", conn)
	}
}

func (c ConnStatus) String() string {
	return redact.StringWithoutMarkers(c)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (c ConnStatus) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("%d: %s (%s)", c.NodeID, c.Address,
		roundSecs(time.Duration(c.AgeNanos)))
}

func (s ServerStatus) String() string {
	return redact.StringWithoutMarkers(s)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (s ServerStatus) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("gossip server (%d/%d cur/max conns, %s)\n",
		len(s.ConnStatus), s.MaxConns, s.MetricSnap)
	for _, conn := range s.ConnStatus {
		w.Printf("  %s\n", conn)
	}
}

func (c Connectivity) String() string {
	return redact.StringWithoutMarkers(c)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (c Connectivity) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("gossip connectivity\n")
	if c.SentinelNodeID != 0 {
		w.Printf("  n%d [sentinel];\n", c.SentinelNodeID)
	}
	if len(c.ClientConns) > 0 {
		w.SafeRune(' ')
		for _, conn := range c.ClientConns {
			w.Printf(" n%d -> n%d;", conn.SourceID, conn.TargetID)
		}
		w.SafeRune('\n')
	}
}
