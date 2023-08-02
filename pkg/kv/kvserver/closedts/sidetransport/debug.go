// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sidetransport

import (
	"context"
	"fmt"
	"html"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// HTML is exposed at /debug/closedts-receiver.
func (s *Receiver) HTML() string {
	sb := &strings.Builder{}

	header := func(s string) {
		fmt.Fprintf(sb, "<h4>%s</h4>", s)
	}

	header("Incoming streams")
	conns := func() []*incomingStream {
		s.mu.RLock()
		defer s.mu.RUnlock()
		cs := make([]*incomingStream, 0, len(s.mu.conns))
		for _, c := range s.mu.conns {
			cs = append(cs, c)
		}
		return cs
	}()
	// Sort by node id.
	sort.Slice(conns, func(i, j int) bool {
		return conns[i].nodeID < conns[j].nodeID
	})
	for _, c := range conns {
		sb.WriteString(c.html() + "<br>")
	}

	header("Closed streams (most recent first; only one per node)")
	closed := func() []streamCloseInfo {
		s.historyMu.Lock()
		defer s.historyMu.Unlock()
		csi := make([]streamCloseInfo, 0, len(s.historyMu.lastClosed))

		for _, c := range s.historyMu.lastClosed {
			csi = append(csi, c)
		}
		return csi
	}()
	// Sort by disconnection time, descending.
	sort.Slice(closed, func(i, j int) bool {
		return closed[i].closeTime.After(closed[j].closeTime)
	})
	now := timeutil.Now()
	for _, c := range closed {
		fmt.Fprintf(sb, "n%d: incoming conn closed at %s (%s ago). err: %s\n",
			c.nodeID, c.closeTime.Truncate(time.Millisecond), now.Sub(c.closeTime).Truncate(time.Second), c.closeErr)
	}

	return strings.ReplaceAll(sb.String(), "\n", "<br>")
}

func (r *incomingStream) html() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	sb := &strings.Builder{}

	bold := func(s string) {
		fmt.Fprintf(sb, "<h4>%s</h4>", s)
	}
	escape := func(s string) {
		sb.WriteString(html.EscapeString(s))
	}

	now := timeutil.Now()
	bold(fmt.Sprintf("n:%d ", r.nodeID))
	fmt.Fprintf(sb, "conn open: %s (%s ago), last received: %s (%s ago), last seq num: %d, closed timestamps: ",
		r.connectedAt.Truncate(time.Second),
		now.Sub(r.connectedAt).Truncate(time.Second),
		r.mu.lastReceived.Truncate(time.Millisecond), now.Sub(r.mu.lastReceived).Truncate(time.Millisecond),
		r.mu.streamState.lastSeqNum)
	escape(r.mu.streamState.String())
	return sb.String()
}

// HTML is exposed at /debug/closedts-sender.
func (s *Sender) HTML() string {
	sb := &strings.Builder{}

	header := func(s string) {
		fmt.Fprintf(sb, "<h4>%s</h4>", s)
	}

	escape := func(s string) string {
		return strings.ReplaceAll(html.EscapeString(s), "\n", "<br>\n")
	}

	header("Closed timestamps sender state")
	func() {
		s.leaseholdersMu.Lock()
		defer s.leaseholdersMu.Unlock()
		fmt.Fprintf(sb, "leaseholders: %d\n", len(s.leaseholdersMu.leaseholders))
	}()

	var lastMsgSeq ctpb.SeqNum
	func() {
		s.trackedMu.Lock()
		defer s.trackedMu.Unlock()
		lastMsgSeq = s.trackedMu.lastSeqNum
		fmt.Fprint(sb, escape(s.trackedMu.streamState.String()))

		failed := 0
		for reason := ReasonUnknown + 1; reason < MaxReason; reason++ {
			failed += s.trackedMu.closingFailures[reason]
		}
		fmt.Fprintf(sb, "Failures to close during last cycle (%d ranges total): ", failed)
		for reason := ReasonUnknown + 1; reason < MaxReason; reason++ {
			if reason > ReasonUnknown+1 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(sb, "%s: %d", reason, s.trackedMu.closingFailures[reason])
		}
	}()

	// List connections
	func() {
		s.connsMu.Lock()
		defer s.connsMu.Unlock()
		header(fmt.Sprintf("Connections (%d)", len(s.connsMu.conns)))
		nids := make([]roachpb.NodeID, 0, len(s.connsMu.conns))
		for nid := range s.connsMu.conns {
			nids = append(nids, nid)
		}
		sort.Slice(nids, func(i, j int) bool {
			return nids[i] < nids[j]
		})
		now := timeutil.Now()
		for _, nid := range nids {
			state := s.connsMu.conns[nid].getState()
			fmt.Fprintf(sb, "n%d: ", nid)
			if state.connected {
				fmt.Fprintf(sb, "connected at: %s (%s ago)\n", state.connectedTime.Truncate(time.Millisecond), now.Sub(state.connectedTime).Truncate(time.Second))
			} else {
				fmt.Fprintf(sb, "disconnected at: %s (%s ago, err: %s)\n", state.lastDisconnectTime.Truncate(time.Millisecond), now.Sub(state.lastDisconnectTime).Truncate(time.Second), state.lastDisconnect)
			}
		}
	}()

	header("Last message")
	lastMsg, ok := s.buf.GetBySeq(context.Background(), lastMsgSeq)
	if !ok {
		fmt.Fprint(sb, "Buffer has been closed.\n")
	} else if lastMsg == nil {
		fmt.Fprint(sb, "Buffer no longer has the message. This is unexpected.\n")
	} else {
		sb.WriteString(escape(lastMsg.String()))
	}

	return strings.ReplaceAll(sb.String(), "\n", "<br>\n")
}
