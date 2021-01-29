// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracker

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// lockfreeTracker is a performant implementation of Tracker, at the expense of
// precision. A precise implementation would hold all tracked timestamps in a
// min-heap, but that costs memory and can't be implemented in a lock-free
// manner (at least not by this author). This Tracker generally doesn't know
// exactly what the lowest tracked timestamp is; it just knows a lower-bound on
// it. Concretely, the tracker doesn't maintain information on all tracked
// timestamps; it only maintains a summary in the form of two buckets, each with
// one timestamp and a reference count. Every timestamp in a bucket counts as if
// it was equal to the bucket's timestamp - even though, in reality, they can be
// higher than the bucket's timestamp.
//
// Some combinations of operations are thread-safe, others need the caller to
// ensure mutual exclusion. In particular, insertions (Track()) are "lock free",
// but deletions (Untrack()) are not. Deletions need exclusive access, so the
// caller needs to use a lock; the intention is for that lock to be held in
// "read" mode for insertions and in write mode for deletions. This data
// structure is meant to be used in conjunction with a propBuf, which uses this
// locking model.
//
// Note that this implementation is only reasonable under some assumptions about
// the use: namely that the lifetimes of all the timestamps in the set are
// fairly similar, and that the timestamps tend to increase over time. This
// matches the expectations of requests (identified by their write timestamp),
// with the lifetime being their evaluation duration.
type lockfreeTracker struct {
	// tokens returned by Track() contain the pointer-identity of a bucket, so we
	// can't swap the buckets in this array in order to maintain b1 at the front.
	// Instead, we swap the b1 and b2 pointers to reorder them.
	buckets [2]bucket
	b1, b2  *bucket
}

// NewLockfreeTracker creates a tracker.
func NewLockfreeTracker() Tracker {
	t := lockfreeTracker{}
	t.b1 = &t.buckets[0]
	t.b2 = &t.buckets[1]
	return &t
}

// String cannot be called concurrently with Untrack.
func (t *lockfreeTracker) String() string {
	return fmt.Sprintf("b1: %s; b2: %s", t.b1, t.b2)
}

// Track is part of the Tracker interface.
func (t *lockfreeTracker) Track(ctx context.Context, ts hlc.Timestamp) RemovalToken {
	// The tracking scheme is based on maintaining (at most) two buckets of
	// timestamps, and continuously draining them and creating new buckets. Timestamps
	// come in (through this Track()) and enter a bucket. Later, they leave the
	// bucket through Untrack(). Each bucket has a bucket timestamp, which is the
	// lowest timestamp that ever entered it. A
	// bucket's timestamp can be lowered throughout its life, but never increased. A bucket doesn't
	// keep track of which timestamps are in it (it only maintains a count), so the
	// bucket is unaware of when the the lowest timestamp (i.e. the timestamp that
	// set the bucket's timestamp) leaves.
	//
	// When a bucket is emptied, it gets reset. Future Track() calls can
	// re-initialize it with a new timestamp (generally expected to be higher than
	// the timestamp it had before the reset).
	//
	// At any point, LowerBound() returns the first bucket's timestamp. That's a
	// lower bound on all the timestamps currently tracked, since b1's timestamp
	// is always lower than b2's.
	//
	// The diagram below tries to give intuition about how the two buckets work.
	// It shows two buckets with timestamps 10 and 20, and three timestamps
	// entering the set. It explains which bucket each timestamp joins.
	//
	//  ^ time grows upwards                     |    |
	//  |                                        |    |
	//  |                                        |    |
	//  |   ts 25 joins b2        ->  |    |     |    |
	//  |                             |    |     |    |
	//  |                             |    |     +----+
	//  |                             |    |     b2 ts: 20
	//  |   ts 15 joins b2,       ->  |    |
	//  |   extending it downwards    +----+
	//  |                             b1 ts: 10
	//  |   ts 5 joins b1,        ->
	//  |   extending it downwards
	//
	// Our goal is to maximize the Tracker's lower bound (i.e. its conservative
	// approximation about the lowest tracked timestamp), which is b1's timestamp
	// (see below).
	//
	// - 25 is above both buckets (meaning above the buckets' timestamp), so it
	// joins b2. It would be technically correct for it to join b1 too, but it'd
	// be a bad idea: if b1 would be slow enough to be on the critical path for b1
	// draining (which it likely is, if all the timestamp stay in the set for a
	// similar amount of time) then it'd be preventing bumping the lower bound
	// from 10 to 20 (which, in practice, would translate in the respective range not
	// closing the [10, 20) range of timestamps).
	// - 15 is below b2, but above b1. It's not quite as clear cut which
	// bucket is the best one to join; if its lifetime is short and
	// so it is *not* on the critical path for b1 draining, then it'd be better for
	// it to join b1. Once b1 drains, we'll be able to bump the tracker's lower
	// bound to 20. On the other hand, if it joins b2, then b2's timestamp comes
	// down to 15 and, once b1 drains and 15 is removed from the tracked set, the
	// tracker's lower bound would only become 15 (which is worse than 20). But,
	// on the third hand, if 15 stays tracked for a while and is on b1's critical
	// path, then putting it in b2 would at least allow us to bump the lower bound
	// to 15, which is better than nothing. We take this argument, and put it in
	// b2.
	// - 5 is below both buckets. The only sensible thing to do is putting it
	// in b1; otherwise we'd have to extend b2 downwards, inverting b1 and b2.
	//
	//
	// IMPLEMENTATION INVARIANTS:
	//
	// 1) After a bucket is initialized, its timestamp only gets lower until the
	// bucket is reset (i.e. it never increases). This serves to keep the relative
	// relation of buckets fixed.
	// 2) (a corollary) If both buckets are initialized, b1.timestamp < b2.timestamp.
	// 3) If only one bucket is initialized, it is b1. Note that both buckets
	// might be uninitialized.
	// 4) Initialized buckets are not empty.

	b1, b2 := t.b1, t.b2

	// The Tracker internally works with int64's, for atomic CAS purposes. So we
	// round down the hlc.Timestamp to just its WallTime.
	wts := ts.WallTime

	// Make sure that there's at least one bucket.
	t1, initialized := b1.timestamp()

	// Join b1 if wts is below it.
	//
	// It's possible that multiple requests coming at the same time pass the `wts
	// <= t1` check and enter b1, even through b2 is uninitialized. This is not
	// ideal; it'd be better if only the lowest request would end up in b1 and the
	// others would end up in b2 (or, more generally, if some "low" requests join
	// b1 and the rest (the "high" ones) go on to create and join b2). But that's
	// harder to implement.
	if !initialized || wts <= t1 {
		return b1.extendAndJoin(ctx, wts, ts.Synthetic)
	}

	// We know that b1 < wts. We can technically join either bucket, but we always
	// prefer b2 in order to let b1 drain as soon as possible (at which point
	// we'll be able to create a new bucket).
	return b2.extendAndJoin(ctx, wts, ts.Synthetic)
}

// Untrack is part of the Tracker interface.
func (t *lockfreeTracker) Untrack(ctx context.Context, tok RemovalToken) {
	b := tok.(lockfreeToken).b
	// Note that atomic ops are not required here, as we hold the exclusive lock.
	b.refcnt--
	if b.refcnt < 0 {
		log.Fatalf(ctx, "negative bucket refcount: %d", b.refcnt)
	}
	if b.refcnt == 0 {
		// Reset the bucket, so that future Track() calls can create a new one.
		b.ts = 0
		b.synthetic = 0
		// If we reset b1, swap the pointers, so that, if b2 is currently
		// initialized, it becomes b1. If a single bucket is initialized, we want it
		// to be b1.
		if b == t.b1 {
			t.b1 = t.b2
			t.b2 = b
		}
	}
}

// LowerBound is part of the Tracker interface.
func (t *lockfreeTracker) LowerBound(ctx context.Context) hlc.Timestamp {
	// Note that, if b1 is uninitialized, so is b2. If both are initialized,
	// b1 < b2. So, we only need to look at b1.
	ts, initialized := t.b1.timestamp()
	if !initialized {
		return hlc.Timestamp{}
	}
	return hlc.Timestamp{
		WallTime:  ts,
		Logical:   0,
		Synthetic: t.b1.isSynthetic(),
	}
}

// Count is part of the Tracker interface.
func (t *lockfreeTracker) Count() int {
	return int(t.b1.refcnt) + int(t.b2.refcnt)
}

// bucket represent a Tracker bucket: a data structure that coalesces a number
// of timestamps, keeping track only of their count and minimum.
//
// A bucket can be initialized or uninitialized. It's initialized when the ts is
// set.
type bucket struct {
	ts        int64 // atomic, nanos
	refcnt    int32 // atomic
	synthetic int32 // atomic
}

func (b *bucket) String() string {
	ts := atomic.LoadInt64(&b.ts)
	if ts == 0 {
		return "uninit"
	}
	refcnt := atomic.LoadInt32(&b.refcnt)
	return fmt.Sprintf("%d requests, lower bound: %s", refcnt, timeutil.Unix(0, ts))
}

// timestamp returns the bucket's timestamp. The bool retval is true if the
// bucket is initialized. If false, the timestamp is 0.
func (b *bucket) timestamp() (int64, bool) {
	ts := atomic.LoadInt64(&b.ts)
	return ts, ts != 0
}

// isSynthetic returns true if the bucket's timestamp (i.e. the bucket's lower
// bound) should be considered a synthetic timestamp.
func (b *bucket) isSynthetic() bool {
	return atomic.LoadInt32(&b.synthetic) != 0
}

// extendAndJoin extends the bucket downwards (if necessary) so that its
// timestamp is <= ts, and then adds a timestamp to the bucket. It returns a
// token to be used for removing the timestamp from the bucket.
//
// If the bucket it not initialized, it will be initialized to ts.
func (b *bucket) extendAndJoin(ctx context.Context, ts int64, synthetic bool) lockfreeToken {
	// Loop until either we set the bucket's timestamp, or someone else sets it to
	// an even lower value.
	var t int64
	for {
		t = atomic.LoadInt64(&b.ts)
		if t != 0 && t <= ts {
			break
		}
		if atomic.CompareAndSwapInt64(&b.ts, t, ts) {
			break
		}
	}
	// If we created the bucket, then we dictate if its lower bound will be
	// considered a synthetic timestamp or not. It's possible that we're now
	// inserting a synthetic timestamp into the bucket but, over time, a higher
	// non-synthetic timestamp joins. Or, that a lower non-synthetic timestamp
	// joins. In either case, the bucket will remain "synthetic" although it'd be
	// correct to make it non-synthetic. We don't make an effort to keep the
	// synthetic bit up to date within a bucket.
	if t == 0 && synthetic {
		atomic.StoreInt32(&b.synthetic, 1)
	}
	atomic.AddInt32(&b.refcnt, 1)
	return lockfreeToken{b: b}
}

// lockfreeToken implements RemovalToken.
type lockfreeToken struct {
	// The bucket that this timestamp is part of.
	b *bucket
}

var _ RemovalToken = lockfreeToken{}

// RemovalTokenMarker implements RemovalToken.
func (l lockfreeToken) RemovalTokenMarker() {}
