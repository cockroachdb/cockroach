// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// TickManager is the gateway-side authority for closing ticks. Each
// call to Flush applies one producer's report:
//
//   - File entries get added to per-tick pending lists.
//   - Checkpoints get forwarded into the manager's own multi-span
//     frontier (initialized at startHLC over the cluster's full span
//     set).
//
// After applying both, any tick whose end has been crossed by the
// aggregate frontier is closed: the manifest is written using whatever
// files have been collected (an empty list yields an empty-Files
// manifest, so coverage has no holes).
//
// The manager's frontier is initialized over the union of all producer
// span sets; producers each cover an assigned subset, and the aggregate
// (= min across spans) advances only once every producer has reached
// the boundary.
type TickManager struct {
	es        cloud.ExternalStorage
	tickWidth time.Duration
	startHLC  hlc.Timestamp

	// frontier tracks the cluster-wide resolved time per span.
	frontier span.Frontier

	// prevFrontier is the frontier value as of the previous Flush.
	// Used to detect when the aggregate frontier has actually moved
	// forward so afterFrontierAdvance fires only on real progress
	// (not on every Flush). Initialized to startHLC.
	prevFrontier hlc.Timestamp

	// afterFrontierAdvance, if non-nil, is invoked synchronously from
	// Flush each time the aggregate frontier advances, with the new
	// frontier value. Failures inside the hook are the hook's
	// responsibility to log and swallow.
	afterFrontierAdvance func(ctx context.Context, frontier hlc.Timestamp)

	// pending accumulates the files and rolled-up stats contributed
	// for a tick that has not yet been closed.
	pending map[hlc.Timestamp]*pendingTick

	// lastClosedMu protects lastClosed so the progress-updater
	// goroutine (see progress.go) can read it concurrently with
	// Flush mutating it. The mutex covers only the field read by
	// the external getter; the rest of TickManager is single-writer
	// (Flush) by construction.
	lastClosedMu struct {
		syncutil.Mutex

		// lastClosed is the upper bound that the next-to-close tick
		// will use as its TickStart. Initially equals startHLC (the
		// log may begin mid-tick); after each close it equals the
		// closed tick's TickEnd.
		lastClosed hlc.Timestamp
	}
}

// NewTickManager constructs a TickManager. spans must be the union
// of all producer span sets; tickWidth must be positive. The
// manager's frontier is initialized at startHLC for every span.
func NewTickManager(
	es cloud.ExternalStorage, spans []roachpb.Span, startHLC hlc.Timestamp, tickWidth time.Duration,
) (*TickManager, error) {
	if len(spans) == 0 {
		return nil, errors.AssertionFailedf("revlogjob: NewTickManager requires at least one span")
	}
	if tickWidth <= 0 {
		return nil, errors.AssertionFailedf("revlogjob: tickWidth must be positive (got %s)", tickWidth)
	}
	f, err := span.MakeFrontierAt(startHLC, spans...)
	if err != nil {
		return nil, errors.Wrap(err, "creating manager frontier")
	}
	m := &TickManager{
		es:           es,
		tickWidth:    tickWidth,
		startHLC:     startHLC,
		frontier:     f,
		prevFrontier: startHLC,
		pending:      make(map[hlc.Timestamp]*pendingTick),
	}
	m.lastClosedMu.lastClosed = startHLC
	return m, nil
}

// SetAfterFrontierAdvance installs a callback fired synchronously
// from Flush whenever the aggregate frontier moves forward. Pass nil
// to clear. The callback runs on the same goroutine as Flush; it
// must not block on I/O that could stall manifest writes. Designed
// for the PTS manager (pts.go), whose advance method dispatches its
// own background work as needed.
func (m *TickManager) SetAfterFrontierAdvance(
	hook func(ctx context.Context, frontier hlc.Timestamp),
) {
	m.afterFrontierAdvance = hook
}

// LastClosed returns the end time of the most-recently-closed tick.
// Before any tick has closed it returns the manager's startHLC. Safe
// to call concurrently with Flush — the progress-updater goroutine
// (see progress.go) uses this from a different goroutine than the
// one driving Flush.
func (m *TickManager) LastClosed() hlc.Timestamp {
	m.lastClosedMu.Lock()
	defer m.lastClosedMu.Unlock()
	return m.lastClosedMu.lastClosed
}

// Flush applies one producer's flush report (TickSink). Files are
// added to per-tick pending lists; checkpoints advance the
// aggregate frontier; any tick whose end has now been crossed is
// closed (manifest written).
//
// TODO(dt): in the multi-producer split, accept a producer ID
// and track per-producer span coverage separately so a single
// producer's checkpoints don't speak for spans it doesn't cover.
func (m *TickManager) Flush(ctx context.Context, msg *revlogpb.Flush) error {
	for _, ff := range msg.Files {
		pt, ok := m.pending[ff.TickEnd]
		if !ok {
			pt = &pendingTick{}
			m.pending[ff.TickEnd] = pt
		}
		pt.files = append(pt.files, ff.File)
		pt.add(ff.Stats)
	}
	for _, cp := range msg.Checkpoints {
		if _, err := m.frontier.Forward(cp.Span, cp.ResolvedTS); err != nil {
			return errors.Wrap(err, "advancing frontier")
		}
	}
	frontier := m.frontier.Frontier()
	for {
		// lastClosed is safe to read without the mutex here because
		// Flush is the only writer; the mutex exists purely so the
		// progress-updater goroutine can read a consistent snapshot.
		next := nextTickEndAfter(m.lastClosedMu.lastClosed, m.tickWidth)
		if frontier.Less(next) {
			break
		}
		if err := m.closeTick(ctx, next); err != nil {
			return err
		}
		m.lastClosedMu.Lock()
		m.lastClosedMu.lastClosed = next
		m.lastClosedMu.Unlock()
	}
	// Fire the hook outside the close loop so it observes the final
	// post-close frontier rather than an intermediate value, and so
	// hook failures cannot leave us with manifests written but the
	// hook side-effects skipped. We deliberately fire on *any*
	// forward movement (even sub-tick movement that didn't close
	// any tick) — the PTS hook in particular wants to track the
	// resolved frontier, not the closed-tick boundary.
	if m.afterFrontierAdvance != nil && m.prevFrontier.Less(frontier) {
		m.prevFrontier = frontier
		m.afterFrontierAdvance(ctx, frontier)
	}
	return nil
}

// closeTick writes the manifest for tickEnd using whatever files
// were collected by Flush. An empty file list is a deliberate
// "this tick is closed and contributed no events" signal so readers
// see no coverage gap between adjacent ticks.
//
// TODO(dt): schema poller hook on tick-close decisions (DDL
// reflected in coverage).
func (m *TickManager) closeTick(ctx context.Context, tickEnd hlc.Timestamp) error {
	pt := m.pending[tickEnd]
	delete(m.pending, tickEnd)
	manifest := revlogpb.Manifest{
		TickStart: m.lastClosedMu.lastClosed,
		TickEnd:   tickEnd,
	}
	if pt != nil {
		manifest.Files = pt.files
		manifest.Stats = pt.stats
	}
	if err := revlog.WriteTickManifest(ctx, m.es, manifest); err != nil {
		return errors.Wrapf(err, "writing manifest for tick %s", tickEnd)
	}
	return nil
}

// pendingTick holds the in-flight files and rolled-up stats for a
// tick that has not yet had its manifest written.
type pendingTick struct {
	files []revlogpb.File
	stats revlogpb.Stats

	// sstBytesUnknown is set if any contributor's Stats.SstBytes was
	// 0 (i.e. the producer couldn't measure the resulting SST). When
	// set, the final Stats.SstBytes is reported as 0 to avoid
	// misleading readers with a partial sum.
	sstBytesUnknown bool
}

// add folds one FlushedFile.Stats contribution into the rollup.
// Producers never flush a file with zero events (see
// Producer.flushTick), so a zero KeyCount / LogicalBytes here means
// nothing was contributed; a zero SstBytes specifically means the
// SST size was not measured.
func (p *pendingTick) add(s revlogpb.Stats) {
	p.stats.KeyCount += s.KeyCount
	p.stats.LogicalBytes += s.LogicalBytes
	if s.SstBytes == 0 {
		p.sstBytesUnknown = true
		p.stats.SstBytes = 0
		return
	}
	if !p.sstBytesUnknown {
		p.stats.SstBytes += s.SstBytes
	}
}
