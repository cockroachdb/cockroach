// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perftrace

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Collector manages work span capture for a node.
type Collector struct {
	reservoir *Reservoir
	nodeID    int32
	clock     *hlc.Clock
	settings  *cluster.Settings
	// nextSpanID is an atomic counter for generating unique span IDs within this node.
	nextSpanID atomic.Int64
}

// NewCollector creates a new Collector.
func NewCollector(nodeID int32, clock *hlc.Clock, settings *cluster.Settings) *Collector {
	capacity := int(ReservoirSize.Get(&settings.SV))
	c := &Collector{
		reservoir: NewReservoir(capacity),
		nodeID:    nodeID,
		clock:     clock,
		settings:  settings,
	}
	// Seed the span ID counter with a pseudo-random value based on the current
	// timestamp to avoid collisions across node restarts. We use nanoseconds
	// and mix in the node ID for additional uniqueness.
	seed := timeutil.Now().UnixNano() ^ (int64(nodeID) << 32)
	c.nextSpanID.Store(seed)
	return c
}

// StartSpan begins tracking a work span if sampled.
// Returns a SpanHandle that must be finished when the span completes.
// If the span is not sampled, returns a no-op handle.
func (c *Collector) StartSpan(
	spanType SpanType,
	spanName string,
	stmtFingerprintID uint64,
	parentID int64,
) *SpanHandle {
	if !Enabled.Get(&c.settings.SV) {
		return &SpanHandle{shouldCapture: false}
	}

	token := c.reservoir.MaybeSample()
	if !token.ShouldCapture() {
		return &SpanHandle{shouldCapture: false}
	}

	// Generate a unique span ID
	spanID := c.nextSpanID.Add(1)

	return &SpanHandle{
		collector:              c,
		token:                  token,
		shouldCapture:          true,
		id:                     spanID,
		parentID:               parentID,
		stmtFingerprintID:      stmtFingerprintID,
		spanType:               spanType,
		spanName:               spanName,
		startTime:              timeutil.Now(),
		stopWatch:              timeutil.NewStopWatchWithCPU(),
	}
}

// Reservoir returns the underlying reservoir (for flushing).
func (c *Collector) Reservoir() *Reservoir {
	return c.reservoir
}

// NodeID returns the node ID of this collector.
func (c *Collector) NodeID() int32 {
	return c.nodeID
}

// SpanHandle tracks an in-progress span.
type SpanHandle struct {
	collector         *Collector
	token             ReservoirToken
	shouldCapture     bool
	id                int64
	parentID          int64
	stmtFingerprintID uint64
	spanType          SpanType
	spanName          string
	startTime         time.Time
	stopWatch         *timeutil.StopWatch
	contentionTime    time.Duration
}

// ID returns the unique ID of this span.
// Returns 0 if the span is not being captured.
func (h *SpanHandle) ID() int64 {
	if h == nil || !h.shouldCapture {
		return 0
	}
	return h.id
}

// Start begins timing the span. Must be called after StartSpan.
func (h *SpanHandle) Start() {
	if h == nil || !h.shouldCapture {
		return
	}
	h.stopWatch.Start()
}

// SetContentionTime sets the contention time (lock + latch waits) for this span.
func (h *SpanHandle) SetContentionTime(d time.Duration) {
	if h == nil || !h.shouldCapture {
		return
	}
	h.contentionTime = d
}

// AddContentionTime adds to the contention time for this span.
func (h *SpanHandle) AddContentionTime(d time.Duration) {
	if h == nil || !h.shouldCapture {
		return
	}
	h.contentionTime += d
}

// Finish completes the span and records it to the reservoir if sampled.
func (h *SpanHandle) Finish() {
	if h == nil || !h.shouldCapture {
		return
	}

	h.stopWatch.Stop()

	span := WorkSpan{
		ID:                     h.id,
		ParentID:               h.parentID,
		NodeID:                 h.collector.nodeID,
		StatementFingerprintID: h.stmtFingerprintID,
		Timestamp:              h.startTime,
		Duration:               h.stopWatch.Elapsed(),
		CPUTime:                h.stopWatch.ElapsedCPU(),
		ContentionTime:         h.contentionTime,
		SpanType:               h.spanType,
		SpanName:               h.spanName,
	}

	h.collector.reservoir.Record(h.token, span)
}
