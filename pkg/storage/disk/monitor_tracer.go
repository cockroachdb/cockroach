// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package disk

import (
	"bytes"
	"fmt"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
		// end denotes the index where the next record should be inserted.
		end int
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
		}{
			trace: make([]traceEvent, capacity),
			start: 0,
			end:   0,
		},
	}
}

// RecordEvent appends a traceEvent to the internal ring buffer. The
// implementation assumes that the event time specified during consecutive calls
// are strictly increasing.
func (m *monitorTracer) RecordEvent(event traceEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.trace[m.mu.end%m.capacity] = event
	// Instead of having `end` wrap around to 0 for indexing the ring buffer, we
	// can reduce complexity by always incrementing and computing the modulus on
	// every access. Since events are recorded at a frequency of
	// COCKROACH_DISK_STATS_POLLING_INTERVAL, the integer will not overflow during
	// our lifetimes.
	m.mu.end++
	if m.sizeLocked() > m.capacity {
		m.mu.start++
	}
}

// Latest retrieves the last traceEvent that was queued. Returns a zero-valued
// traceEvent if none exists.
func (m *monitorTracer) Latest() traceEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sizeLocked() == 0 {
		return traceEvent{}
	}
	latestIdx := (m.mu.end - 1) % m.capacity
	return m.mu.trace[latestIdx]
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
	windowLen := m.sizeLocked() - offset
	events := make([]traceEvent, windowLen)
	for i := 0; i < windowLen; i++ {
		events[i] = *m.traceAtLocked(offset + i)
	}
	return events
}

// String implements fmt.Stringer.
func (m *monitorTracer) String() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sizeLocked() == 0 {
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
	prevStats := m.traceAtLocked(0).stats
	for i := 1; i < m.sizeLocked(); i++ {
		event := *m.traceAtLocked(i)
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

// traceAtLocked returns the traceEvent at the specified offset from the
// m.mu.start. m.mu must be held.
func (m *monitorTracer) traceAtLocked(i int) *traceEvent {
	return &m.mu.trace[(m.mu.start+i)%m.capacity]
}

// ceilSearchLocked retrieves the offset from trace's start for the traceEvent
// that occurred at or after a specified time, t. If all events occurred before
// t, an error is thrown. Note that it is the responsibility of the caller to
// acquire the tracer mutex and the returned offset may become invalid after the
// mutex is released.
func (m *monitorTracer) ceilSearchLocked(t time.Time) (int, bool) {
	if m.sizeLocked() == 0 {
		return -1, false
	}
	// Apply binary search to find the offset of the traceEvent that occurred at
	// or after time t.
	offset, _ := sort.Find(m.sizeLocked(), func(i int) int {
		return t.Compare(m.traceAtLocked(i).time)
	})
	if offset == m.sizeLocked() {
		return -1, false
	}
	return offset, true
}

// sizeLocked retrieves the number of elements in the trace buffer. Note that
// it is the responsibility of the caller to acquire the tracer mutex and the
// returned size may become invalid after the mutex is released.
func (m *monitorTracer) sizeLocked() int {
	return m.mu.end - m.mu.start
}
