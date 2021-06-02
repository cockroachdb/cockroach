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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// A Manager maintains an interval tree of key and key range latches. Latch
// acquisitions affecting keys or key ranges must wait on already-acquired
// latches which overlap their key ranges to be released.
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
	scopes  [spanset.NumSpanScope]scopedManager

	stopper  *stop.Stopper
	slowReqs *metric.Gauge
}

// scopedManager is a latch manager scoped to either local or global keys.
// See spanset.SpanScope.
type scopedManager struct {
	readSet latchList
	trees   [spanset.NumSpanAccess]btree
}

// Make returns an initialized Manager. Using this constructor is optional as
// the type's zero value is valid to use directly.
func Make(stopper *stop.Stopper, slowReqs *metric.Gauge) Manager {
	return Manager{
		stopper:  stopper,
		slowReqs: slowReqs,
	}
}

// latches are stored in the Manager's btrees. They represent the latching
// of a single key span.
type latch struct {
	id         uint64
	span       roachpb.Span
	ts         hlc.Timestamp
	done       *signal
	next, prev *latch // readSet linked-list.
}

func (la *latch) inReadSet() bool {
	return la.next != nil
}

//go:generate ../../../util/interval/generic/gen.sh *latch spanlatch

// Methods required by util/interval/generic type contract.
func (la *latch) ID() uint64         { return la.id }
func (la *latch) Key() []byte        { return la.span.Key }
func (la *latch) EndKey() []byte     { return la.span.EndKey }
func (la *latch) String() string     { return fmt.Sprintf("%s@%s", la.span, la.ts) }
func (la *latch) New() *latch        { return new(latch) }
func (la *latch) SetID(v uint64)     { la.id = v }
func (la *latch) SetKey(v []byte)    { la.span.Key = v }
func (la *latch) SetEndKey(v []byte) { la.span.EndKey = v }

// Guard is a handle to a set of acquired latches. It is returned by
// Manager.Acquire and accepted by Manager.Release.
type Guard struct {
	done signal
	// latches [spanset.NumSpanScope][spanset.NumSpanAccess][]latch, but half the size.
	latchesPtrs [spanset.NumSpanScope][spanset.NumSpanAccess]unsafe.Pointer
	latchesLens [spanset.NumSpanScope][spanset.NumSpanAccess]int32
	// Non-nil only when AcquireOptimistic has retained the snapshot for later
	// checking of conflicts, and waiting.
	snap *snapshot
}

func (lg *Guard) latches(s spanset.SpanScope, a spanset.SpanAccess) []latch {
	len := lg.latchesLens[s][a]
	if len == 0 {
		return nil
	}
	const maxArrayLen = 1 << 31
	return (*[maxArrayLen]latch)(lg.latchesPtrs[s][a])[:len:len]
}

func (lg *Guard) setLatches(s spanset.SpanScope, a spanset.SpanAccess, latches []latch) {
	lg.latchesPtrs[s][a] = unsafe.Pointer(&latches[0])
	lg.latchesLens[s][a] = int32(len(latches))
}

func allocGuardAndLatches(nLatches int) (*Guard, []latch) {
	// Guard would be an ideal candidate for object pooling, but without
	// reference counting its latches we can't know whether they're still
	// referenced by other tree snapshots. The latches hold a reference to
	// the signal living on the Guard, so the guard can't be recycled while
	// latches still point to it.
	if nLatches <= 1 {
		alloc := new(struct {
			g       Guard
			latches [1]latch
		})
		return &alloc.g, alloc.latches[:nLatches]
	} else if nLatches <= 2 {
		alloc := new(struct {
			g       Guard
			latches [2]latch
		})
		return &alloc.g, alloc.latches[:nLatches]
	} else if nLatches <= 4 {
		alloc := new(struct {
			g       Guard
			latches [4]latch
		})
		return &alloc.g, alloc.latches[:nLatches]
	} else if nLatches <= 8 {
		alloc := new(struct {
			g       Guard
			latches [8]latch
		})
		return &alloc.g, alloc.latches[:nLatches]
	}
	return new(Guard), make([]latch, nLatches)
}

func newGuard(spans *spanset.SpanSet) *Guard {
	nLatches := spans.Len()
	guard, latches := allocGuardAndLatches(nLatches)
	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		for a := spanset.SpanAccess(0); a < spanset.NumSpanAccess; a++ {
			ss := spans.GetSpans(a, s)
			n := len(ss)
			if n == 0 {
				continue
			}

			ssLatches := latches[:n]
			for i := range ssLatches {
				latch := &latches[i]
				latch.span = ss[i].Span
				latch.done = &guard.done
				latch.ts = ss[i].Timestamp
				// latch.setID() in Manager.insert, under lock.
			}
			guard.setLatches(s, a, ssLatches)
			latches = latches[n:]
		}
	}
	if len(latches) != 0 {
		panic("alloc too large")
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
func (m *Manager) Acquire(ctx context.Context, spans *spanset.SpanSet) (*Guard, error) {
	lg, snap := m.sequence(spans)
	defer snap.close()

	err := m.wait(ctx, lg, snap)
	if err != nil {
		m.Release(lg)
		return nil, err
	}
	return lg, nil
}

// AcquireOptimistic is like Acquire, except it does not wait for latches over
// overlapping spans to be released before returning. Instead, it
// optimistically assumes that there are no currently held latches that need
// to be waited on. This can be verified after the fact by passing the Guard
// and the spans actually read to CheckOptimisticNoConflicts.
//
// Despite existing latches being ignored by this method, future calls to
// Acquire will observe the latches inserted here and will wait for them to be
// Released, as usual.
//
// The method returns a Guard which must be provided to the
// CheckOptimisticNoConflicts, Release methods.
func (m *Manager) AcquireOptimistic(spans *spanset.SpanSet) *Guard {
	lg, snap := m.sequence(spans)
	lg.snap = &snap
	return lg
}

// CheckOptimisticNoConflicts returns true iff the spans in the provided
// spanset do not conflict with any existing latches (in the snapshot created
// in AcquireOptimistic). It must only be called after AcquireOptimistic, and
// if it returns true, the caller can skip calling WaitUntilAcquired and it is
// sufficient to only call Release. If it returns false, the caller will
// typically call WaitUntilAcquired to wait for latch acquisition. It is also
// acceptable for the caller to skip WaitUntilAcquired and directly call
// Release, in which case it never held the latches.
func (m *Manager) CheckOptimisticNoConflicts(lg *Guard, spans *spanset.SpanSet) bool {
	if lg.snap == nil {
		panic(errors.AssertionFailedf("snap must not be nil"))
	}
	snap := lg.snap
	var search latch
	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		tr := &snap.trees[s]
		for a := spanset.SpanAccess(0); a < spanset.NumSpanAccess; a++ {
			ss := spans.GetSpans(a, s)
			for _, sp := range ss {
				search.span = sp.Span
				search.ts = sp.Timestamp
				switch a {
				case spanset.SpanReadOnly:
					// Search for writes at equal or lower timestamps.
					it := tr[spanset.SpanReadWrite].MakeIter()
					if overlaps(&it, &search, ignoreLater) {
						return false
					}
				case spanset.SpanReadWrite:
					// Search for all other writes.
					it := tr[spanset.SpanReadWrite].MakeIter()
					if overlaps(&it, &search, ignoreNothing) {
						return false
					}
					// Search for reads at equal or higher timestamps.
					it = tr[spanset.SpanReadOnly].MakeIter()
					if overlaps(&it, &search, ignoreEarlier) {
						return false
					}
				default:
					panic("unknown access")
				}
			}
		}
	}
	// Note that we don't call lg.snap.close() since even when this returns
	// true, it is acceptable for the caller to call WaitUntilAcquired.
	return true
}

func overlaps(it *iterator, search *latch, ignore ignoreFn) bool {
	for it.FirstOverlap(search); it.Valid(); it.NextOverlap(search) {
		// The held latch may have already been signaled, but that doesn't allow
		// us to ignore it, since it could have been held while we were
		// concurrently evaluating, and we may not have observed the result of
		// evaluation of that conflicting latch holder.
		held := it.Cur()
		if !ignore(search.ts, held.ts) {
			return true
		}
	}
	return false
}

// WaitUntilAcquired is meant to be called when CheckOptimisticNoConflicts has
// returned false, and so the caller needs to do pessimistic latching.
func (m *Manager) WaitUntilAcquired(ctx context.Context, lg *Guard) (*Guard, error) {
	if lg.snap == nil {
		panic(errors.AssertionFailedf("snap must not be nil"))
	}
	defer func() {
		lg.snap.close()
		lg.snap = nil
	}()
	err := m.wait(ctx, lg, *lg.snap)
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
func (m *Manager) sequence(spans *spanset.SpanSet) (*Guard, snapshot) {
	lg := newGuard(spans)

	m.mu.Lock()
	snap := m.snapshotLocked(spans)
	m.insertLocked(lg)
	m.mu.Unlock()
	return lg, snap
}

// snapshot is an immutable view into the latch manager's state.
type snapshot struct {
	trees [spanset.NumSpanScope][spanset.NumSpanAccess]btree
}

// close closes the snapshot and releases any associated resources.
func (sn *snapshot) close() {
	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		for a := spanset.SpanAccess(0); a < spanset.NumSpanAccess; a++ {
			sn.trees[s][a].Reset()
		}
	}
}

// snapshotLocked captures an immutable snapshot of the latch manager. It takes
// a spanset to limit the amount of state captured.
func (m *Manager) snapshotLocked(spans *spanset.SpanSet) snapshot {
	var snap snapshot
	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		sm := &m.scopes[s]
		reading := len(spans.GetSpans(spanset.SpanReadOnly, s)) > 0
		writing := len(spans.GetSpans(spanset.SpanReadWrite, s)) > 0

		if writing {
			sm.flushReadSetLocked()
			snap.trees[s][spanset.SpanReadOnly] = sm.trees[spanset.SpanReadOnly].Clone()
		}
		if writing || reading {
			snap.trees[s][spanset.SpanReadWrite] = sm.trees[spanset.SpanReadWrite].Clone()
		}
	}
	return snap
}

// flushReadSetLocked flushes the read set into the read interval tree.
func (sm *scopedManager) flushReadSetLocked() {
	for sm.readSet.len > 0 {
		latch := sm.readSet.front()
		sm.readSet.remove(latch)
		sm.trees[spanset.SpanReadOnly].Set(latch)
	}
}

// insertLocked inserts the latches owned by the provided Guard into the
// Manager.
func (m *Manager) insertLocked(lg *Guard) {
	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		sm := &m.scopes[s]
		for a := spanset.SpanAccess(0); a < spanset.NumSpanAccess; a++ {
			latches := lg.latches(s, a)
			for i := range latches {
				latch := &latches[i]
				latch.id = m.nextIDLocked()
				switch a {
				case spanset.SpanReadOnly:
					// Add reads to the readSet. They only need to enter
					// the read tree if they're flushed by a write capturing
					// a snapshot.
					sm.readSet.pushBack(latch)
				case spanset.SpanReadWrite:
					// Add writes directly to the write tree.
					sm.trees[spanset.SpanReadWrite].Set(latch)
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

	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		tr := &snap.trees[s]
		for a := spanset.SpanAccess(0); a < spanset.NumSpanAccess; a++ {
			latches := lg.latches(s, a)
			for i := range latches {
				latch := &latches[i]
				switch a {
				case spanset.SpanReadOnly:
					// Wait for writes at equal or lower timestamps.
					a2 := spanset.SpanReadWrite
					it := tr[a2].MakeIter()
					if err := m.iterAndWait(ctx, timer, &it, a, a2, latch, ignoreLater); err != nil {
						return err
					}
				case spanset.SpanReadWrite:
					// Wait for all other writes.
					//
					// It is cheaper to wait on an already released latch than
					// it is an unreleased latch so we prefer waiting on longer
					// latches first. We expect writes to take longer than reads
					// to release their latches, so we wait on them first.
					a2 := spanset.SpanReadWrite
					it := tr[a2].MakeIter()
					if err := m.iterAndWait(ctx, timer, &it, a, a2, latch, ignoreNothing); err != nil {
						return err
					}
					// Wait for reads at equal or higher timestamps.
					a2 = spanset.SpanReadOnly
					it = tr[a2].MakeIter()
					if err := m.iterAndWait(ctx, timer, &it, a, a2, latch, ignoreEarlier); err != nil {
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
	ctx context.Context,
	t *timeutil.Timer,
	it *iterator,
	waitType, heldType spanset.SpanAccess,
	wait *latch,
	ignore ignoreFn,
) error {
	for it.FirstOverlap(wait); it.Valid(); it.NextOverlap(wait) {
		held := it.Cur()
		if held.done.signaled() {
			continue
		}
		if ignore(wait.ts, held.ts) {
			continue
		}
		if err := m.waitForSignal(ctx, t, waitType, heldType, wait, held); err != nil {
			return err
		}
	}
	return nil
}

// waitForSignal waits for the latch that is currently held to be signaled.
func (m *Manager) waitForSignal(
	ctx context.Context, t *timeutil.Timer, waitType, heldType spanset.SpanAccess, wait, held *latch,
) error {
	log.Eventf(ctx, "waiting to acquire %s latch %s, held by %s latch %s", waitType, wait, heldType, held)
	for {
		select {
		case <-held.done.signalChan():
			return nil
		case <-t.C:
			t.Read = true
			defer t.Reset(base.SlowRequestThreshold)

			log.Warningf(ctx, "have been waiting %s to acquire %s latch %s, held by %s latch %s",
				base.SlowRequestThreshold, waitType, wait, heldType, held)
			if m.slowReqs != nil {
				m.slowReqs.Inc(1)
				defer m.slowReqs.Dec(1)
			}
		case <-ctx.Done():
			log.VEventf(ctx, 2, "%s while acquiring %s latch %s, held by %s latch %s",
				ctx.Err(), waitType, wait, heldType, held)
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
	if lg.snap != nil {
		lg.snap.close()
	}

	m.mu.Lock()
	m.removeLocked(lg)
	m.mu.Unlock()
}

// removeLocked removes the latches owned by the provided Guard from the
// Manager. Must be called with mu held.
func (m *Manager) removeLocked(lg *Guard) {
	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		sm := &m.scopes[s]
		for a := spanset.SpanAccess(0); a < spanset.NumSpanAccess; a++ {
			latches := lg.latches(s, a)
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
func (m *Manager) Info() (global, local kvserverpb.LatchManagerInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	global = m.scopes[spanset.SpanGlobal].infoLocked()
	local = m.scopes[spanset.SpanLocal].infoLocked()
	return global, local
}

func (sm *scopedManager) infoLocked() kvserverpb.LatchManagerInfo {
	var info kvserverpb.LatchManagerInfo
	info.ReadCount = int64(sm.trees[spanset.SpanReadOnly].Len() + sm.readSet.len)
	info.WriteCount = int64(sm.trees[spanset.SpanReadWrite].Len())
	return info
}
