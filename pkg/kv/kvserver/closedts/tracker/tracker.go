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

// Tracker tracks a lower bound for the write timestamps of requests currently
// evaluating. It is used to track requests to a single range, and to figure out
// what timestamp can be closed when a request finishes evaluating (but the
// Tracker itself does not know anything about closed timestamps). The tracker
// implements a generic data structure that efficiently maintains an
// approximate, conservative lower-bound for a set of numbers. Numbers can be
// inserted and removed. However, the implementation is only reasonable under
// some assumptions about the use: namely that the lifetimes of all the numbers
// in the set are fairly similar, and that numbers tend to increase over time. This
// matches the expectations of requests (identified by their write timestamp), with the
// lifetime being their evaluation duration.
//
// The usage pattern is:
//
// Start of request evaluation:
//
// externalLock.RLock()
// tok := Tracker.Track(request.writeTimestamp)
// externalLock.RUnlock()
//
// Proposal buffer flush:
//
// externalLock.Lock()
// for each command being proposed:
// 		Tracker.Untrack(tok)
// newClosedTimestamp := min(now() - 5s, Tracker.LowerBound())
// externalLock.Unlock()
//
// The implementation is supposed to be efficient, at the expense of accuracy.
// An accurate implementation would hold all tracked requests in a heap ordered
// by their timestamp, but that costs memory and can't be implemented in a
// lock-free manner (at least not by this author). The tracker generally doesn't
// know exactly what the lowest timestamp of a currently evaluating request is;
// it just knows a lower-bound on it. Concretely, the tracker doesn't maintain
// information on all tracked requests; it only maintains a summary in the form
// of two buckets, with their timestamps and reference counts. Every request in
// a bucket counts as if its timestamp was the bucket's timestamp - even though,
// in reality, its timestamp is higher.
//
// Some combinations of operations are thread-safe, others need the caller to
// ensure mutual exclusion. In particular, insertions (Track()) are "lock free",
// but deletions (Untrack()) are not. Deletions need exclusive access, so the
// caller needs to use a lock; the intention is for that lock to be held in
// "read" mode for insertions and in write mode for deletions. This data
// structure is meant to be used in conjunction with a propBuf, which uses this
// locking model.
//
// Note that the Tracker deals with request timestamps as physical time only,
// although most of the system deals with logical timestamps. This is in order
// to make it easy for the lock-free implementation. The caller is expected to
// round logical timestamps up.
type Tracker struct {
	// tokens returned by Track() contain the pointer-identity of a bucket, so we
	// can't swap the buckets in this array in order to maintain b1 at the front.
	// Instead, we swap the b1 and b2 pointers to reorder them.
	buckets [2]bucket
	b1, b2  *bucket
}

// NewTracker creates a tracker.
func NewTracker() *Tracker {
	t := Tracker{}
	t.b1 = &t.buckets[0]
	t.b2 = &t.buckets[1]
	return &t
}

// String cannot be called concurrently with Untrack.
func (t *Tracker) String() string {
	return fmt.Sprintf("b1: %s; b2: %s", t.b1, t.b2)
}

// Track tracks a request trying to write at `wts` (write timestamp) until
// Untrack() is called. The returned token must be passed to Untrack().
//
// While the request is tracked, LowerBound() will return values less or equal
// to wts, thus ensuring that the timestamp at which this request is writing
// cannot be closed.
//
// Track can be called concurrently with other Track calls.
func (t *Tracker) Track(ctx context.Context, wts int64) Token {
	// The tracking scheme is based on maintaining (at most) two buckets of
	// request, and continuously draining them and creating new buckets. Requests
	// come in (through this Track()) and enter a bucket. Later, they leave the
	// bucket through Untrack(). Each bucket has a timestamp, which is the
	// timestamp of the request with the lowest timestamp that ever entered it. A
	// bucket's timestamp can be lowered, but never increased. A bucket doesn't
	// keep track of which requests are in it (it only maintains a count), so the
	// bucket is unaware of when the request that set its timestamp leaves.
	//
	// When a bucket is emptied, it gets reset. Future requests can re-initialize
	// it with a new timestamp (generally higher than the timestamp it had before
	// the reset).
	//
	// At any point, LowerBound() returns the first bucket's timestamp. That's a
	// lower bound on all the requests currently tracked, since b1's timestamp is
	// always lower than b2's.
	//
	// The diagram below tries to give intuition about how the two buckets work.
	// It shows two buckets with timestamps 10 and 20, and three requests with
	// different timestamps. It explains which bucket each request joins.
	//
	//  ^ time grows upwards                     |    |
	//  |                                        |    |
	//  |                                        |    |
	//  |  req1@25 joins b2       ->  |    |     |    |
	//  |                             |    |     |    |
	//  |                             |    |     +----+
	//  |                             |    |     b2 ts: 20
	//  |  req2@15 joins b2,      ->  |    |
	//  |   extending it downwards    +----+
	//  |                             b1 ts: 10
	//  |  req3@5 joins b1,       ->
	//  |   extending it downwards
	//
	// Our goal is to maximize the Tracker's lower bound (i.e. its conservative
	// approximation about the lowest timestamp at which a request currently
	// evaluates), which is b1's timestamp.
	//
	// - req1@25 is above both buckets, so it joins b2. It would be technically
	// correct for it to join b1 too, but it'd be a bad idea: if b1 would be slow
	// enough to be on the critical path for b1 draining (which it likely is, if
	// all the requests take the same amount of time to evaluate) then we'd be
	// preventing from closing the [10,20) range of timestamps.
	// - req2@15 is below b2, but above b1. It's not quite as clear cut which
	// bucket is the best one to join; if the request evaluates really quickly and
	// so is *not* on the critical path for b1 draining, then it'd be better for
	// it to join b1. Once b1 drains, we'll be able to bump the tracker's lower
	// bound to 20. On the other hand, if it joins b2, then b2's timestamp comes
	// down to 15 and, once b1 drains and req2 finishes, the tracker's lower bound
	// would only become 15 (which worse than 20). But, on the third hand, if the
	// request would be on b1's critical path, then putting it in b2 would at
	// least allow us to bump the lower bound to 15, which is better than nothing.
	// We take this argument, and put it in b2.
	// - req3@5 is below both buckets. The only sensible thing to do is putting it
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
	// 4) If a new bucket is initialized, it will not remain empty (i.e. some
	// request will join the bucket before the read lock is released). This
	// assures that we're not leaking buckets. The request creating the bucket
	// doesn't necessarily join the bucket; a request with a lower timestamp might
	// race to join the newly-created bucket and lower its timestamp in the
	// process, and the bucket-creating request might end up creating or otherwise
	// joining a second bucket.
	//

	b1, b2 := t.b1, t.b2

	// Make sure that there's at least one bucket.
	t1 := createIfNotInitialized(b1, wts)

	// Join b1 if wts is below it (or equal - e.g. if we just created it).
	//
	// It's possible that multiple requests coming at the same time pass the `wts
	// <= t1` check and enter b1, even through b2 is uninitialized. This is not
	// ideal; it'd be better if only the lowest request would end up in b1 and the
	// others would end up in b2 (or, more generally, if some "low" requests join
	// b1 and the rest (the "high" ones) go on to create and join b2). But that's
	// harder to implement.
	if wts <= t1 {
		return b1.extendAndJoin(ctx, wts)
	}

	// Create b2 if it doesn't exist.
	createIfNotInitialized(b2, wts)

	// Now both buckets are known to exist and we know that b1 < wts. We can
	// technically join either bucket, but we always prefer b2 in order to let b1
	// drain as soon as possible (at which point we'll be able to create a new
	// bucket).
	return b2.extendAndJoin(ctx, wts)
}

// Untrack stops tracking the request that generated `tok`. This might advance
// the timestamps that future LowerBound() calls return.
//
// Untrack cannot be called concurrently with other operations.
func (t *Tracker) Untrack(ctx context.Context, tok Token) {
	b := tok.b
	// Note that atomic ops are not required here, as we hold the exclusive lock.
	b.refcnt--
	if b.refcnt == 0 {
		// Reset the bucket, so that further request can create a new one.
		b.ts = 0
		// If we reset b1, swap the pointers, so that, if b2 is currently
		// initialized, it becomes b1. If a single bucket is initialized, we want it
		// to be b1.
		if b == t.b1 {
			t.b1 = t.b2
			t.b2 = b
		}
	}
}

// LowerBound returns the highest timestamp that's known to be below the
// timestamp at which currently-tracked requests are writing. If no requests are
// currently tracked, an empty timestamp is returned.
//
// LowerBound called concurrently with Track, but not with Untrack.
func (t *Tracker) LowerBound() int64 {
	// Note that, if b1 is uninitialized, so is b2. And if both are initialized,
	// b1 < b2. So, we only need to look at b1.
	return t.b1.initialized()
}

// LowerBoundHLC is like LowerBound but returns the output as an HLC.
func (t *Tracker) LowerBoundHLC() hlc.Timestamp {
	return hlc.Timestamp{WallTime: t.LowerBound()}
}

// bucket represent a Tracker bucket: a data structure that coallesces a number
// of requests, keeping track only of their count and the lowest ts among them.
//
// A bucket can be initialized or uninitialized. It's initialized when the
// ts is set.
type bucket struct {
	ts     int64 // atomic, nanos
	refcnt int32 // atomic
}

func (b *bucket) String() string {
	ts := atomic.LoadInt64(&b.ts)
	if ts == 0 {
		return "uninit"
	}
	refcnt := atomic.LoadInt32(&b.refcnt)
	return fmt.Sprintf("%d requests, lower bound: %s", refcnt, timeutil.Unix(0, ts))
}

// initialized returns the bucket's ts if the bucket is initialized, or 0
// otherwise.
func (b *bucket) initialized() int64 {
	return atomic.LoadInt64(&b.ts)
}

// join adds a request to the bucket and returns a token, to be used for
// removing the request from the bucket (through Tracker.Untrack()).
func (b *bucket) join() Token {
	atomic.AddInt32(&b.refcnt, 1)
	return Token{b: b}
}

// extendAndJoin extends the bucket downwards (if necessary) so that its
// ts is <= ts, and then adds a request to the bucket. Like join, it returns
// a token to be used for removing the request from the bucket.
func (b *bucket) extendAndJoin(ctx context.Context, ts int64) Token {
	// Loop until either we set the bucket's timestamp, or someone else sets it to
	// an even lower value.
	for {
		t := atomic.LoadInt64(&b.ts)
		if t == 0 {
			log.Fatalf(ctx, "attempting to extend uninitialized bucket")
		}
		if t <= ts {
			break
		}
		if atomic.CompareAndSwapInt64(&b.ts, t, ts) {
			break
		}
	}
	return b.join()
}

// Token represents the information identifying a tracked request, tying a
// Track() to an Untrack().
type Token token

type token struct {
	// The bucket that this request is part of.
	b *bucket
}

// createIfNotInitialized initializes b to ts if b is not already initialized.
//
// b's timestamp is returned in either case. If the bucket was found
// initialized, then its timestamp is not changed. As the bucket's timestamp can
// be lowered at any time by other requests, this returned timestamp can only be
// used as an upper bound.
//
// b.ts is assumed to not be set to 0 concurrently with calls to this function.
// In other words, others may race to initialize the bucket, but not to
// un-initialize it.
func createIfNotInitialized(b *bucket, ts int64) int64 {
	if t := b.initialized(); t != 0 {
		return t
	}
	// Initialize b.
	if atomic.CompareAndSwapInt64(&b.ts, 0, ts) {
		return ts
	}
	// If our CAS failed, we know that the bucket was initialized by a racer.
	return b.initialized()
}
