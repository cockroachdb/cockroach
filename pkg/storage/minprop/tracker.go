// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package minprop

import (
	"context"
	"sync/atomic"

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// A Tracker keeps tabs on ongoing operations (which it forces to successively
// higher timestamps) and provides a timestamp bound below which there's no
// current or future activity.
type Tracker struct {
	mu        syncutil.RWMutex
	cur, last hlc.Timestamp
	// Ref counts for cur, last. Atomically updated under read lock.
	curRef, lastRef int32
}

// NewTracker returns an initialized Tracker.
func NewTracker() *Tracker {
	return &Tracker{
		// Return a tracker that is initialized with a closed timestamp of zero.
		cur: hlc.Timestamp{Logical: 1},
	}
}

// String prints a string representation of the Tracker's state.
func (t *Tracker) String() string {
	t.mu.RLock()
	last, cur := t.last, t.cur
	lastRef, curRef := atomic.LoadInt32(&t.lastRef), atomic.LoadInt32(&t.curRef)
	t.mu.RUnlock()

	return fmt.Sprintf(
		"%s has %d (last)\n%s has %d (cur)",
		last, lastRef, cur, curRef,
	)
}

// Close attempts to establish a new min proposal timestamp. This is possible
// only if tracked commands that were in-flight when Close was last called have
// since completed. On success, the supplied timestamp will be applied to all
// future calls to Track and the returned closed timestamp will have increased
// relative to the last call to Close. On failure, the supplied timestamp is
// ignored and the returned closed timestamp is identical to that of the last
// call (in particular, the caller does not have to worry about whether the
// update was successful or not).
func (t *Tracker) Close(newMinProposal hlc.Timestamp) hlc.Timestamp {
	var closed hlc.Timestamp
	t.mu.Lock()
	if log.V(3) {
		log.Infof(context.TODO(), "close: lastRef=%d curRef=%d last=%s cur=%s new=%s", t.lastRef, t.curRef, t.last, t.cur, newMinProposal)
	}
	if t.lastRef > 0 {
		closed = t.last
	} else {
		// If `curRef == 0`, then all open proposals are ahead of `cur`, so we
		// can close out `cur`. Additionally, if nothing is in-flight at all at
		// the moment (`curRef == 0`), then we can close one tick before
		// `newMinProposal` (if that's better than using `t.cur`, which it
		// should be unless the caller is feeding us weird timestamps).
		//
		// NB: could try to decrement a logical tick here but it's more trouble
		// than it's worth).
		if t.curRef == 0 && t.cur.WallTime < newMinProposal.WallTime {
			closed = newMinProposal.Add(-1, 0)
		} else {
			closed = t.cur
		}

		// `last` moves forward to the closed timestamp, and picks up the
		// refcount (so that it is now responsible for tracking everything
		// that's in-flight).
		t.last = closed
		t.lastRef = t.curRef

		t.curRef = 0
		if !t.cur.Forward(newMinProposal) {
			// If, for some reason, the new proposal forwarding threshold is
			// not strictly ahead of the old one, move it a tick further.
			// Otherwise, we'd have closed out a timestamp that we still
			// allow proposals for.
			t.cur = t.cur.Next()
		}
	}
	t.mu.Unlock()
	return closed
}

// Track is called before evaluating a proposal. It returns the minimum
// timestamp at which the proposal can be evaluated (i.e. the request timestamp
// needs to be forwarded if necessary), and acquires a reference with the
// Tracker that is (and must eventually be) released by calling the returned
// closure.
func (t *Tracker) Track(ctx context.Context) (hlc.Timestamp, func()) {
	t.mu.RLock()
	minProp := t.cur
	atomic.AddInt32(&t.curRef, 1)
	t.mu.RUnlock()

	release := func() {
		t.mu.RLock()
		defer t.mu.RUnlock()
		if minProp == t.cur {
			if log.V(3) {
				log.Infof(ctx, "release: minprop == cur == %s", minProp)
			}
			val := atomic.AddInt32(&t.curRef, -1)
			if val < 0 {
				log.Fatalf(ctx, "min proposal current ref count < 0")
			}
		} else if minProp == t.last {
			if log.V(3) {
				log.Infof(ctx, "minprop == last == %s", minProp)
			}
			val := atomic.AddInt32(&t.lastRef, -1)
			if val < 0 {
				log.Fatalf(ctx, "min proposal last ref count < 0")
			}
		} else {
			log.Fatalf(ctx, "min proposal %s doesn't match current (%s) or last (%s)", minProp, t.cur, t.last)
		}
	}

	return minProp, release
}
