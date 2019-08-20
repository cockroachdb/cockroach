// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanlatch

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// A Manager maintains an interval tree of key and key range latches. Latch
// acquisitions affecting keys or key ranges must wait on already-acquired latches
// which overlap their key ranges to be released.
//
// Latch acquisition attempts invoke Manager.Acquire and provide details about
// the spans that they plan to touch and the timestamps they plan to touch them
// at. Acquire inserts the latch into the Manager's tree and waits on
// prerequisite latch attempts that are already tracked by the Manager.
// Manager.Acquire blocks until the latch acquisition completes, at which point
// it returns a Guard, which is scoped to the lifetime of the latch ownership.
//
// When the latches are no longer needed, they are released by invoking
// Manager.Release with the Guard returned when the latches were originally
// acquired. Doing so removes the latches from the Manager's tree and signals to
// dependent latch acquisitions that they no longer need to wait on the released
// latches.
//
// Manager is safe for concurrent use by multiple goroutines. Concurrent access
// is made efficient using a copy-on-write technique to capture immutable
// snapshots of the type's inner btree structures. Using this strategy, tasks
// requiring mutual exclusion are limited to updating the type's trees and
// grabbing snapshots. Notably, scanning for and waiting on prerequisite latches
// is performed outside of the mutual exclusion zone. This means that the work
// performed under lock is linear with respect to the number of spans that a
// latch acquisition declares but NOT linear with respect to the number of other
// latch attempts that it will wait on.
//
// Manager's zero value can be used directly.
type Manager struct {
	mu      syncutil.Mutex
	idAlloc uint64
	scopes  [NumSpanScope]scopedManager

	stopper  *stop.Stopper
	slowReqs *metric.Gauge
}

// scopedManager is a latch manager scoped to either local or global keys.
// See SpanScope.
type scopedManager struct {
	readSet latchList
	trees   [NumSpanAccess]btree
}

// NewManager returns an initialized Manager. Using this constructor is optional as
// the type's zero value is valid to use directly.
func NewManager(stopper *stop.Stopper, slowReqs *metric.Gauge) Manager {
	return Manager{
		stopper:  stopper,
		slowReqs: slowReqs,
	}
}

// latches are stored in the Manager's btrees. They represent the latching
// of a single key span.
type latch struct {
	span roachpb.Span
	ts   hlc.Timestamp

	id         uint64
	done       *signal
	next, prev *latch // readSet linked-list.
}

func (la *latch) String() string {
	return fmt.Sprintf("%s@%s", la.span, la.ts)
}

func (la *latch) inReadSet() bool {
	return la.next != nil
}

// Guard is a handle to a set of acquired latches. It is returned by
// Manager.Acquire and accepted by Manager.Release.
type Guard struct {
	done    signal
	latches [NumSpanScope][NumSpanAccess][]latch
}

func newGuard(spans *SpanSet) *Guard {
	// Guard would be an ideal candidate for object pooling, but without
	// reference counting its latches we can't know whether they're still
	// referenced by other tree snapshots. The latches hold a reference to
	// the signal living on the Guard, so the guard can't be recycled while
	// latches still point to it.
	guard := new(Guard)
	for s := SpanScope(0); s < NumSpanScope; s++ {
		for a := SpanAccess(0); a < NumSpanAccess; a++ {
			latches := spans.GetLatches(a, s)
			n := len(latches)
			if n == 0 {
				continue
			}

			for i := range latches {
				// latch.id set in Manager.insert, under lock.
				latches[i].done = &guard.done
			}
			guard.latches[s][a] = latches
		}
	}
	return guard
}

// Acquire acquires latches from the Manager for each of the provided spans, at
// the specified timestamp. In doing so, it waits for latches over all
// overlapping spans to be released before returning. If the provided context
// is canceled before the method is done waiting for overlapping latches to
// be released, it stops waiting and releases all latches that it has already
// acquired.
//
// It returns a Guard which must be provided to Release.
func (m *Manager) Acquire(
	ctx context.Context, spans *SpanSet,
) (*Guard, error) {
	lg, snap := m.sequence(spans)
	defer snap.close()

	err := m.wait(ctx, lg, snap)
	if err != nil {
		m.Release(lg)
		return nil, err
	}
	return lg, nil
}

// sequence locks the manager, captures an immutable snapshot, inserts latches
// for each of the specified spans into the manager's interval trees, and
// unlocks the manager. The role of the method is to sequence latch acquisition
// attempts.
func (m *Manager) sequence(spans *SpanSet) (*Guard, snapshot) {
	lg := newGuard(spans)

	m.mu.Lock()
	snap := m.snapshotLocked(spans)
	m.insertLocked(lg)
	m.mu.Unlock()
	return lg, snap
}

// snapshot is an immutable view into the latch manager's state.
type snapshot struct {
	trees [NumSpanScope][NumSpanAccess]btree
}

// close closes the snapshot and releases any associated resources.
func (sn *snapshot) close() {
	for s := SpanScope(0); s < NumSpanScope; s++ {
		for a := SpanAccess(0); a < NumSpanAccess; a++ {
			sn.trees[s][a].Reset()
		}
	}
}

// snapshotLocked captures an immutable snapshot of the latch manager. It takes
// a spanlatch set to limit the amount of state captured.
func (m *Manager) snapshotLocked(spans *SpanSet) snapshot {
	var snap snapshot
	for s := SpanScope(0); s < NumSpanScope; s++ {
		sm := &m.scopes[s]
		reading := len(spans.GetLatches(SpanReadOnly, s)) > 0
		writing := len(spans.GetLatches(SpanReadWrite, s)) > 0

		if writing {
			sm.flushReadSetLocked()
			snap.trees[s][SpanReadOnly] = sm.trees[SpanReadOnly].Clone()
		}
		if writing || reading {
			snap.trees[s][SpanReadWrite] = sm.trees[SpanReadWrite].Clone()
		}
	}
	return snap
}

// flushReadSetLocked flushes the read set into the read interval tree.
func (sm *scopedManager) flushReadSetLocked() {
	for sm.readSet.len > 0 {
		latch := sm.readSet.front()
		sm.readSet.remove(latch)
		sm.trees[SpanReadOnly].Set(latch)
	}
}

// insertLocked inserts the latches owned by the provided Guard into the
// Manager.
func (m *Manager) insertLocked(lg *Guard) {
	for s := SpanScope(0); s < NumSpanScope; s++ {
		sm := &m.scopes[s]
		for a := SpanAccess(0); a < NumSpanAccess; a++ {
			latches := lg.latches[s][a]
			for i := range latches {
				latch := &latches[i]
				latch.id = m.nextIDLocked()
				switch a {
				case SpanReadOnly:
					// Add reads to the readSet. They only need to enter
					// the read tree if they're flushed by a write capturing
					// a snapshot.
					sm.readSet.pushBack(latch)
				case SpanReadWrite:
					// Add writes directly to the write tree.
					sm.trees[SpanReadWrite].Set(latch)
				default:
					panic("unknown access")
				}
			}
		}
	}
}

func (m *Manager) nextIDLocked() uint64 {
	m.idAlloc++
	return m.idAlloc
}

// ignoreFn is used for non-interference of earlier reads with later writes.
//
// However, this is only desired for the global scope. Reads and writes to local
// keys are specified to always interfere, regardless of their timestamp. This
// is done to avoid confusion with local keys declared as part of proposer
// evaluated KV.
//
// This is also disabled in the global scope if either of the timestamps are
// empty. In those cases, we consider the latch without a timestamp to be a
// non-MVCC operation that affects all timestamps in the key range.
type ignoreFn func(ts, other hlc.Timestamp) bool

func ignoreLater(ts, other hlc.Timestamp) bool   { return !ts.IsEmpty() && ts.Less(other) }
func ignoreEarlier(ts, other hlc.Timestamp) bool { return !other.IsEmpty() && other.Less(ts) }
func ignoreNothing(ts, other hlc.Timestamp) bool { return false }

// wait waits for all interfering latches in the provided snapshot to complete
// before returning.
func (m *Manager) wait(ctx context.Context, lg *Guard, snap snapshot) error {
	timer := timeutil.NewTimer()
	timer.Reset(base.SlowRequestThreshold)
	defer timer.Stop()

	for s := SpanScope(0); s < NumSpanScope; s++ {
		tr := &snap.trees[s]
		for a := SpanAccess(0); a < NumSpanAccess; a++ {
			latches := lg.latches[s][a]
			for i := range latches {
				latch := &latches[i]
				switch a {
				case SpanReadOnly:
					// Wait for writes at equal or lower timestamps.
					it := tr[SpanReadWrite].MakeIter()
					if err := m.iterAndWait(ctx, timer, &it, latch, ignoreLater); err != nil {
						return err
					}
				case SpanReadWrite:
					// Wait for all other writes.
					//
					// It is cheaper to wait on an already released latch than
					// it is an unreleased latch so we prefer waiting on longer
					// latches first. We expect writes to take longer than reads
					// to release their latches, so we wait on them first.
					it := tr[SpanReadWrite].MakeIter()
					if err := m.iterAndWait(ctx, timer, &it, latch, ignoreNothing); err != nil {
						return err
					}
					// Wait for reads at equal or higher timestamps.
					it = tr[SpanReadOnly].MakeIter()
					if err := m.iterAndWait(ctx, timer, &it, latch, ignoreEarlier); err != nil {
						return err
					}
				default:
					panic("unknown access")
				}
			}
		}
	}
	return nil
}

// iterAndWait uses the provided iterator to wait on all latches that overlap
// with the search latch and which should not be ignored given their timestamp
// and the supplied ignoreFn.
func (m *Manager) iterAndWait(
	ctx context.Context, t *timeutil.Timer, it *iterator, wait *latch, ignore ignoreFn,
) error {
	for it.FirstOverlap(wait); it.Valid(); it.NextOverlap() {
		held := it.Cur()
		if held.done.signaled() {
			continue
		}
		if ignore(wait.ts, held.ts) {
			continue
		}
		if err := m.waitForSignal(ctx, t, wait, held); err != nil {
			return err
		}
	}
	return nil
}

// waitForSignal waits for the latch that is currently held to be signaled.
func (m *Manager) waitForSignal(ctx context.Context, t *timeutil.Timer, wait, held *latch) error {
	for {
		select {
		case <-held.done.signalChan():
			return nil
		case <-t.C:
			t.Read = true
			defer t.Reset(base.SlowRequestThreshold)

			log.Warningf(ctx, "have been waiting %s to acquire latch %s, held by %s",
				base.SlowRequestThreshold, wait, held)
			if m.slowReqs != nil {
				m.slowReqs.Inc(1)
				defer m.slowReqs.Dec(1)
			}
		case <-ctx.Done():
			log.VEventf(ctx, 2, "%s while acquiring latch %s, held by %s", ctx.Err(), wait, held)
			return ctx.Err()
		case <-m.stopper.ShouldQuiesce():
			// While shutting down, requests may acquire
			// latches and never release them.
			return &roachpb.NodeUnavailableError{}
		}
	}
}

// Release releases the latches held by the provided Guard. After being called,
// dependent latch acquisition attempts can complete if not blocked on any other
// owned latches.
func (m *Manager) Release(lg *Guard) {
	lg.done.signal()

	m.mu.Lock()
	m.removeLocked(lg)
	m.mu.Unlock()
}

// removeLocked removes the latches owned by the provided Guard from the
// Manager. Must be called with mu held.
func (m *Manager) removeLocked(lg *Guard) {
	for s := SpanScope(0); s < NumSpanScope; s++ {
		sm := &m.scopes[s]
		for a := SpanAccess(0); a < NumSpanAccess; a++ {
			latches := lg.latches[s][a]
			for i := range latches {
				latch := &latches[i]
				if latch.inReadSet() {
					sm.readSet.remove(latch)
				} else {
					sm.trees[a].Delete(latch)
				}
			}
		}
	}
}

// Info returns information about the state of the Manager.
func (m *Manager) Info() (global, local storagepb.LatchManagerInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	global = m.scopes[SpanGlobal].infoLocked()
	local = m.scopes[SpanLocal].infoLocked()
	return global, local
}

func (sm *scopedManager) infoLocked() storagepb.LatchManagerInfo {
	var info storagepb.LatchManagerInfo
	info.ReadCount = int64(sm.trees[SpanReadOnly].Len() + sm.readSet.len)
	info.WriteCount = int64(sm.trees[SpanReadWrite].Len())
	return info
}
