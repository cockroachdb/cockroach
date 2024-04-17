// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package disk

import (
	"bytes"
	"fmt"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type traceEvent struct {
	time  time.Time
	stats Stats
	err   error
}

// String implements fmt.Stringer.
func (t traceEvent) String() string {
	if t.err != nil {
		return fmt.Sprintf("%s\t\t%s", t.time.Format(time.RFC3339Nano), t.err.Error())
	}
	s := t.stats
	statString := fmt.Sprintf("%s\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t%s\t%d\t%s\t%s\t%d\t%d\t%d\t%s\t%d\t%s",
		s.DeviceName, s.ReadsCount, s.ReadsMerged, s.ReadsSectors, s.ReadsDuration,
		s.WritesCount, s.WritesMerged, s.WritesSectors, s.WritesDuration,
		s.InProgressCount, s.CumulativeDuration, s.WeightedIODuration,
		s.DiscardsCount, s.DiscardsMerged, s.DiscardsSectors, s.DiscardsDuration,
		s.FlushesCount, s.FlushesDuration)
	return fmt.Sprintf("%s\t%s\tnil", t.time.Format(time.RFC3339Nano), statString)
}

// monitorTracer manages a ring buffer containing a history of disk stats.
// The tracer is designed such that higher-level components can apply
// aggregation functions to compute statistics over rolling windows and output
// detailed traces when failures are detected.
type monitorTracer struct {
	capacity int

	mu struct {
		syncutil.Mutex
		trace []traceEvent
		start int
		end   int
		size  int
	}
}

func newMonitorTracer(capacity int) *monitorTracer {
	return &monitorTracer{
		capacity: capacity,
		mu: struct {
			syncutil.Mutex
			trace []traceEvent
			start int
			end   int
			size  int
		}{
			trace: make([]traceEvent, capacity),
			start: 0,
			end:   0,
			size:  0,
		},
	}
}

// RecordEvent appends a traceEvent to the internal ring buffer. The
// implementation assumes that the event time specified during consecutive calls
// are strictly increasing.
func (m *monitorTracer) RecordEvent(event traceEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.trace[m.mu.end] = event
	m.mu.end = (m.mu.end + 1) % m.capacity
	if m.mu.size == m.capacity {
		m.mu.start = (m.mu.start + 1) % m.capacity
	} else {
		m.mu.size++
	}
}

// Latest retrieves the last traceEvent that was queued. If the trace is empty
// we throw an error.
func (m *monitorTracer) Latest() (traceEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mu.size == 0 {
		return traceEvent{}, errors.Errorf("trace is empty")
	}
	// Since m.mu.end points to the next index to write at, we add m.capacity to
	// prevent an arithmetic modulus error in case m.mu.end is zero.
	latestIdx := (m.mu.end - 1 + m.capacity) % m.capacity
	return m.mu.trace[latestIdx], nil
}

// RollingWindow retrieves all traceEvents that occurred after the specified
// time, t. If no event meets this criterion we return an empty slice.
func (m *monitorTracer) RollingWindow(t time.Time) []traceEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	offset, ok := m.ceilSearchLocked(t)
	if !ok {
		return []traceEvent{}
	}
	events := make([]traceEvent, m.mu.size-offset)
	for i := 0; i < m.mu.size-offset; i++ {
		events[i] = m.mu.trace[(m.mu.start+offset+i)%m.capacity]
	}
	return events
}

// String implements fmt.Stringer.
func (m *monitorTracer) String() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mu.size == 0 {
		return ""
	}

	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
	fmt.Fprintln(w, "Time\t"+
		"Device Name\tReads Completed\tReads Merged\tSectors Read\tRead Duration\t"+
		"Writes Completed\tWrites Merged\tSectors Written\tWrite Duration\t"+
		"IO in Progress\tIO Duration\tWeighted IO Duration\t"+
		"Discards Completed\tDiscards Merged\tSectors Discarded\tDiscard Duration\t"+
		"Flushes Completed\tFlush Duration\tError")
	prevStats := m.mu.trace[m.mu.start].stats
	for i := 1; i < m.mu.size; i++ {
		event := m.mu.trace[(m.mu.start+i)%m.capacity]
		delta := event.stats.delta(&prevStats)
		if event.err == nil {
			prevStats = event.stats
		}
		deltaEvent := traceEvent{
			time:  event.time,
			stats: delta,
			err:   event.err,
		}
		fmt.Fprintln(w, deltaEvent)
	}
	_ = w.Flush()

	return buf.String()
}

// ceilSearchLocked retrieves the offset from trace's start for the traceEvent
// that occurred at or after a specified time, t. If all events occurred before
// t, an error is thrown. Note that it is the responsibility of the caller to
// acquire the tracer mutex and the returned offset may become invalid after the
// mutex is released.
func (m *monitorTracer) ceilSearchLocked(t time.Time) (int, bool) {
	if m.mu.size == 0 {
		return -1, false
	}
	// Apply binary search to find the offset of the traceEvent that occurred at
	// or after time t.
	offset, _ := sort.Find(m.mu.size, func(i int) int {
		idx := (m.mu.start + i) % m.capacity
		return t.Compare(m.mu.trace[idx].time)
	})
	if offset == m.mu.size {
		return -1, false
	}
	return offset, true
}
