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
//
// Concurrency: Flush is the only writer of the per-tick state and
// the frontier, but Snapshot (used by the checkpoint loop) and
// LastClosed (used by the per-job-info readers) read them from
// other goroutines. mu serializes all three.
type TickManager struct {
	es        cloud.ExternalStorage
	tickWidth time.Duration
	startHLC  hlc.Timestamp

	// afterFrontierAdvance is set once at construction time (see
	// SetAfterFrontierAdvance) and read without the lock from Flush.
	// Failures inside the hook are the hook's responsibility to log
	// and swallow. The hook MUST NOT call back into TickManager —
	// it runs while a Flush is in flight.
	afterFrontierAdvance func(ctx context.Context, frontier hlc.Timestamp)

	mu struct {
		syncutil.Mutex

		// frontier tracks the cluster-wide resolved time per span.
		frontier span.Frontier

		// prevFrontier is the frontier value as of the previous Flush.
		// Used to detect when the aggregate frontier has actually moved
		// forward so afterFrontierAdvance fires only on real progress
		// (not on every Flush). Initialized to startHLC.
		prevFrontier hlc.Timestamp

		// pending accumulates the files and rolled-up stats contributed
		// for a tick that has not yet been closed.
		pending map[hlc.Timestamp]*pendingTick

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
//
// To rehydrate from a previous incarnation's checkpoint, follow up
// with Rehydrate before any Flush.
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
		es:        es,
		tickWidth: tickWidth,
		startHLC:  startHLC,
	}
	m.mu.frontier = f
	m.mu.prevFrontier = startHLC
	m.mu.pending = make(map[hlc.Timestamp]*pendingTick)
	m.mu.lastClosed = startHLC
	return m, nil
}

// Rehydrate replays a persisted State into the manager: forwards
// the manager's frontier per persisted (span, ts), restores the
// in-flight per-tick file lists, and seeds lastClosed from the
// high-water. Must be called before the first Flush.
//
// Spans absent from the persisted frontier are left at startHLC
// (the natural "no progress yet here" position); spans persisted
// but no longer in the manager's span set are silently dropped
// (the resolveSpans seam is the source of truth for current
// coverage). Stats are not restored — see revlogpb.OpenTicks.
func (m *TickManager) Rehydrate(state State) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if state.Frontier != nil {
		for sp, ts := range state.Frontier.Entries() {
			if _, err := m.mu.frontier.Forward(sp, ts); err != nil {
				return errors.Wrap(err, "rehydrating frontier")
			}
		}
		m.mu.prevFrontier = m.mu.frontier.Frontier()
	}
	for tickEnd, files := range state.OpenTicks {
		m.mu.pending[tickEnd] = &pendingTick{
			files: files,
			// Stats not persisted (see revlogpb.OpenTicks); mark
			// SST bytes unknown so the eventual rolled-up Stats
			// don't misleadingly omit pre-resume contributions.
			sstBytesUnknown: true,
		}
	}
	if state.HighWater.IsSet() {
		m.mu.lastClosed = state.HighWater
	}
	return nil
}

// SetAfterFrontierAdvance installs a callback fired synchronously
// from Flush whenever the aggregate frontier moves forward. Pass nil
// to clear. The callback runs on the same goroutine as Flush; it
// must not block on I/O that could stall manifest writes and must
// not call back into TickManager. Designed for the PTS manager
// (pts.go), whose advance method dispatches its own background
// work as needed.
func (m *TickManager) SetAfterFrontierAdvance(
	hook func(ctx context.Context, frontier hlc.Timestamp),
) {
	m.afterFrontierAdvance = hook
}

// LastClosed returns the end time of the most-recently-closed tick.
// Before any tick has closed it returns the manager's startHLC. Safe
// to call concurrently with Flush.
func (m *TickManager) LastClosed() hlc.Timestamp {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.lastClosed
}

// Snapshot captures the manager's checkpoint state for persistence
// (see Persister). Holds mu briefly to copy frontier entries and
// per-tick file lists; the returned State is independent of the
// manager and may outlive subsequent Flushes.
func (m *TickManager) Snapshot() (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Rebuild the frontier into a fresh one so the caller (and the
	// persister it hands the snapshot to) doesn't share storage with
	// the live manager. The walk is bounded by the number of spans,
	// not the volume of events flushed.
	spans := make([]roachpb.Span, 0)
	tsBySpan := make(map[string]hlc.Timestamp)
	for sp, ts := range m.mu.frontier.Entries() {
		spans = append(spans, sp)
		tsBySpan[sp.String()] = ts
	}
	snapFrontier, err := span.MakeFrontier(spans...)
	if err != nil {
		return State{}, errors.Wrap(err, "snapshotting frontier")
	}
	for _, sp := range spans {
		if ts := tsBySpan[sp.String()]; ts.IsSet() {
			if _, err := snapFrontier.Forward(sp, ts); err != nil {
				return State{}, errors.Wrap(err, "snapshotting frontier")
			}
		}
	}

	openTicks := make(map[hlc.Timestamp][]revlogpb.File, len(m.mu.pending))
	for tickEnd, pt := range m.mu.pending {
		if len(pt.files) == 0 {
			continue
		}
		files := make([]revlogpb.File, len(pt.files))
		copy(files, pt.files)
		openTicks[tickEnd] = files
	}

	return State{
		HighWater: m.mu.lastClosed,
		Frontier:  snapFrontier,
		OpenTicks: openTicks,
	}, nil
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
	frontier, prevFrontier, err := m.applyFlush(ctx, msg)
	if err != nil {
		return err
	}
	// Fire the hook outside the locked region so it observes the
	// final post-close frontier and so its work doesn't serialize
	// against Snapshot / LastClosed readers. We deliberately fire
	// on *any* forward movement (even sub-tick movement that didn't
	// close any tick) — the PTS hook in particular wants to track
	// the resolved frontier, not the closed-tick boundary.
	if m.afterFrontierAdvance != nil && prevFrontier.Less(frontier) {
		m.afterFrontierAdvance(ctx, frontier)
	}
	return nil
}

// applyFlush is Flush's locked-region body. Returns the post-apply
// frontier and the prior frontier so the caller can decide whether
// to fire afterFrontierAdvance outside the lock.
func (m *TickManager) applyFlush(
	ctx context.Context, msg *revlogpb.Flush,
) (frontier, prevFrontier hlc.Timestamp, _ error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, ff := range msg.Files {
		pt, ok := m.mu.pending[ff.TickEnd]
		if !ok {
			pt = &pendingTick{}
			m.mu.pending[ff.TickEnd] = pt
		}
		pt.files = append(pt.files, ff.File)
		pt.add(ff.Stats)
	}
	for _, cp := range msg.Checkpoints {
		if _, err := m.mu.frontier.Forward(cp.Span, cp.ResolvedTS); err != nil {
			return hlc.Timestamp{}, hlc.Timestamp{}, errors.Wrap(err, "advancing frontier")
		}
	}
	frontier = m.mu.frontier.Frontier()
	for {
		next := nextTickEndAfter(m.mu.lastClosed, m.tickWidth)
		if frontier.Less(next) {
			break
		}
		if err := m.closeTickLocked(ctx, next); err != nil {
			return hlc.Timestamp{}, hlc.Timestamp{}, err
		}
		m.mu.lastClosed = next
	}
	prevFrontier = m.mu.prevFrontier
	m.mu.prevFrontier = frontier
	return frontier, prevFrontier, nil
}

// closeTickLocked writes the manifest for tickEnd using whatever
// files were collected by Flush. An empty file list is a deliberate
// "this tick is closed and contributed no events" signal so readers
// see no coverage gap between adjacent ticks. mu must be held.
//
// TODO(dt): schema poller hook on tick-close decisions (DDL
// reflected in coverage).
func (m *TickManager) closeTickLocked(ctx context.Context, tickEnd hlc.Timestamp) error {
	pt := m.mu.pending[tickEnd]
	delete(m.mu.pending, tickEnd)
	manifest := revlogpb.Manifest{
		TickStart: m.mu.lastClosed,
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
	// 0 (i.e. the producer couldn't measure the resulting SST), or
	// if the tick was rehydrated from a persisted snapshot (where
	// per-file stats are not retained). When set, the final
	// Stats.SstBytes is reported as 0 to avoid misleading readers
	// with a partial sum.
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
