// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
