// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admit_long

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/dustin/go-humanize"
)

type resourceType uint8

const (
	cpu resourceType = iota
	diskWrite
	numResources
)

type resourceUsageByWork struct {
	startTimeUnixNanos int64
	endTimeUnixNanos   int64
	done               bool
	cumulative         [numResources]int64
	debugging          struct {
		estimated int64
	}
}

// WriteBytes is cumulative.
type WriteBytes struct {
	Bytes     int64
	Debugging struct {
		Estimated int64
	}
}

// Need an interface for querying by AC. Need a reporting interface.

// WorkResourceUsageReporter is provided to the long-lived work to report its
// resource usage. It must be called frequently.
type WorkResourceUsageReporter interface {
	WorkStart()
	MeasureCPU(g int)
	CumulativeWriteBytes(w WriteBytes)
	// WorkDone can call back into WorkRequester.Grant, so WorkRequester must
	// not hold any mutex that is needed by WorkRequester.Grant.
	WorkDone()
	GetIDForDebugging() uint64
}

type internalUsageReporter interface {
	WorkResourceUsageReporter
	getMetrics() resourceUsageByWork
	workRejected()
}

type newInternalUsageReporter func(
	g *workGranterImpl, kind WorkCategoryAndStore, id workID, numGoroutines int) internalUsageReporter

type usageReporterImpl struct {
	g           *workGranterImpl
	kind        WorkCategoryAndStore
	id          workID
	cpuMeasurer []goroutineCPUMeasurer
	mu          struct {
		syncutil.Mutex
		startTimeUnixNanos int64
		endTimeUnixNanos   int64
		done               bool
		writeBytes         WriteBytes
	}
}

var _ WorkResourceUsageReporter = &usageReporterImpl{}

func newUsageReporterImpl(
	g *workGranterImpl, kind WorkCategoryAndStore, id workID, numGoroutines int,
) internalUsageReporter {
	return &usageReporterImpl{
		g:           g,
		kind:        kind,
		id:          id,
		cpuMeasurer: make([]goroutineCPUMeasurer, numGoroutines),
	}
}

func (r *usageReporterImpl) WorkStart() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.startTimeUnixNanos = timeutil.Now().UnixNano()
}

func (r *usageReporterImpl) MeasureCPU(g int) {
	r.cpuMeasurer[g].measure()
}

func (r *usageReporterImpl) CumulativeWriteBytes(w WriteBytes) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// This may be decreasing if w is negative. Which is a correction of earlier
	// estimation error.
	r.mu.writeBytes = w
	if false {
		log.Infof(context.Background(), "CumulativeStats ID: %d at caller1 stats: %s %d (%s) %d",
			uint64(r.id),
			humanize.IBytes(uint64(r.mu.writeBytes.Bytes)), r.mu.writeBytes.Bytes,
			humanize.IBytes(uint64(r.mu.writeBytes.Debugging.Estimated)), r.mu.writeBytes.Debugging.Estimated)
	}
}

func (r *usageReporterImpl) WorkDone() {
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.mu.done = true
		if r.mu.startTimeUnixNanos != 0 {
			r.mu.endTimeUnixNanos = timeutil.Now().UnixNano()
		}
		// Else never started.
	}()
	r.g.workDone(r.kind, r.id, true)
}

func (r *usageReporterImpl) GetIDForDebugging() uint64 {
	return uint64(r.id)
}

func (r *usageReporterImpl) getMetrics() resourceUsageByWork {
	var v resourceUsageByWork
	r.mu.Lock()
	v.startTimeUnixNanos = r.mu.startTimeUnixNanos
	v.endTimeUnixNanos = r.mu.endTimeUnixNanos
	v.done = r.mu.done
	wb := r.mu.writeBytes
	r.mu.Unlock()
	v.cumulative[diskWrite] = wb.Bytes
	v.debugging.estimated = wb.Debugging.Estimated
	for i := range r.cpuMeasurer {
		v.cumulative[cpu] += int64(r.cpuMeasurer[i].get())
	}
	if false && r.id%10 == 0 {
		log.Infof(context.Background(), "CumulativeStats ID: %d at getMetrics stats: %d (%d)",
			uint64(r.id), v.cumulative[diskWrite], v.debugging.estimated)
	}
	return v
}

func (r *usageReporterImpl) workRejected() {
	r.mu.Lock()
	r.mu.done = true
	r.mu.Unlock()
	// A concurrent tick can notice the done=true and remove from the g.mu.work
	// map.
	r.g.workDone(r.kind, r.id, false)
}

type goroutineCPUMeasurer struct {
	mu struct {
		syncutil.Mutex
		start time.Duration
		last  time.Duration
	}
	// The number of iterations since we last measured the running time, and how
	// many iterations until we need to do so again.
	itersSinceLastCheck, itersUntilCheck int
}

func (m *goroutineCPUMeasurer) measure() {
	if m.itersUntilCheck == 0 {
		t := grunning.Time()
		m.mu.Lock()
		defer m.mu.Unlock()
		m.mu.start = t
		m.mu.last = t
		m.itersUntilCheck = 1
		return
	}
	// Since this can be invoked in tight loops where we're sensitive to
	// per-iteration overhead entirely from invoking grunning.Time() frequently,
	// we try to reduce how frequently that needs to happen.
	m.itersSinceLastCheck++
	if m.itersSinceLastCheck < m.itersUntilCheck {
		return
	}
	last := grunning.Time()
	m.mu.Lock()
	defer m.mu.Unlock()
	elapsed := last - m.mu.last
	if elapsed < time.Millisecond {
		m.itersUntilCheck *= 2
	} else if elapsed > 4*time.Millisecond && m.itersUntilCheck > 1 {
		m.itersUntilCheck /= 2
	}
	if elapsed > 0 {
		m.mu.last = last
	}
	m.itersSinceLastCheck = 0
}

func (m *goroutineCPUMeasurer) get() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.last - m.mu.start
}
