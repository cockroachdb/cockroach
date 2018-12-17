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
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Tracker implements TrackerI.
type Tracker struct {
	mu struct {
		syncutil.Mutex
		// closed is the most recently closed timestamp.
		closed hlc.Timestamp

		// The variables below track required information for the next closed
		// timestamp and beyond. First, `next` is the timestamp that will be
		// closed out next (i.e. will replace `closed`).
		//
		// "left" and "right" refers to how the timestamps at which the
		// associated command evaluations take place relate to `next`.
		// `left`-tracked proposals are taken into account for the next closed
		// timestamp, i.e. they could mutate at timestamps <= `next`. `right`
		// proposals affect only MVCC timestamps > `next` and thus will become
		// relevant only after `next` has been closed out, at which point the
		// "right" set will replace the "left".
		//
		//    closed           next
		//      |          left | right
		//      |               |
		//      |               |
		//      v               v
		//---------------------------------------------------------> time
		//
		// A replica wishing to serve a follower read will first have to catch
		// up to a lease applied index that is guaranteed to include all writes
		// affecting the closed timestamp or below. When `next` is closed out,
		// the set of relevant Lease Applied Indexes will be stored in `leftMLAI`.
		//
		// This is augmented by reference counts for the proposals currently in
		// the process of evaluating. `next` can only be closed out once
		// `leftRef` has been drained (i.e. has dropped to zero); new proposals
		// are always forced above `next` and consequently count towards
		// `rightRef`.

		next                hlc.Timestamp
		leftMLAI, rightMLAI map[roachpb.RangeID]ctpb.LAI
		leftRef, rightRef   int
	}
}

var _ closedts.TrackerI = (*Tracker)(nil)

// NewTracker returns a Tracker initialized to a closed timestamp of zero and
// a next closed timestamp of one logical tick past zero.
func NewTracker() *Tracker {
	t := &Tracker{}
	t.mu.next = hlc.Timestamp{Logical: 1}
	t.mu.leftMLAI = map[roachpb.RangeID]ctpb.LAI{}
	t.mu.rightMLAI = map[roachpb.RangeID]ctpb.LAI{}

	return t
}

// String prints a string representation of the Tracker's state.
func (t *Tracker) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	closed, next := t.mu.closed, t.mu.next
	leftRef, rightRef := t.mu.leftRef, t.mu.rightRef

	type item struct {
		rangeID roachpb.RangeID
		mlai    ctpb.LAI
		left    bool
	}

	var lais []item
	for rangeID, mlai := range t.mu.leftMLAI {
		lais = append(lais, item{rangeID, mlai, true})
	}
	for rangeID, mlai := range t.mu.rightMLAI {
		lais = append(lais, item{rangeID, mlai, false})
	}

	sort.Slice(lais, func(i, j int) bool {
		if lais[i].rangeID != lais[j].rangeID {
			return lais[i].rangeID < lais[j].rangeID
		}
		return lais[i].mlai < lais[j].mlai
	})

	var lines string
	for _, item := range lais {
		var format string
		if !item.left {
			format = `      |               @ %d     (r%d)
`
		} else {
			format = `      |   %11d @        (r%d)
`
		}
		lines += fmt.Sprintf(format, item.mlai, item.rangeID)
	}

	return fmt.Sprintf(`
  closed=%s
      |            next=%s
      |          left | right
      |           %3d # %d
`+lines+
		`      v               v
---------------------------------------------------------> time
`,
		closed, next, leftRef, rightRef,
	)
}

// Close attempts to close out the current candidate timestamp (replacing it
// with the provided one). This is possible only if tracked proposals that were
// evaluating when Close was previously called have since completed. On success,
// all subsequent proposals will be forced to evaluate strictly above the
// provided timestamp, and the timestamp previously passed to Close is returned
// as a closed timestamp along with a map of minimum Lease Applied Indexes
// reflecting the updates for the past period. On failure, the previous closed
// timestamp is returned along with a nil map (which can be treated by callers
// like a successful call that happens to not return any new information).
// Similarly, failure to provide a timestamp strictly larger than that to be
// closed out next results in the same "idempotent" return values.
func (t *Tracker) Close(next hlc.Timestamp) (hlc.Timestamp, map[roachpb.RangeID]ctpb.LAI) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var closed hlc.Timestamp
	var mlai map[roachpb.RangeID]ctpb.LAI
	if log.V(3) {
		log.Infof(context.TODO(), "close: leftRef=%d rightRef=%d next=%s closed=%s new=%s", t.mu.leftRef, t.mu.rightRef, t.mu.next, t.mu.closed, next)
	}

	// Make sure to not let `t.mu.next` regress, or we'll accept proposals
	// that violate earlier closed timestamps. (And if it stayed the same
	// the logic in the closure returned from Track would fall apart).
	if t.mu.leftRef == 0 && t.mu.next.Less(next) {
		// NB: if rightRef is also zero, then nothing is in flight right now and
		// we could theoretically close out `next`. However, we'd also have to
		// merge the left and right MLAI maps, and would force followers to
		// catch up to more commands much more rapidly than can be expected of
		// them. If we want to make use of this optimization, we should emit
		// two closed timestamp updates for this case.
		t.mu.closed = t.mu.next
		mlai = t.mu.leftMLAI // hold on to left MLAIs as return value

		// `next` moves forward to the provided timestamp, and picks up the
		// right refcount and MLAIs (so that it is now responsible for tracking
		// everything that's in-flight).
		t.mu.leftMLAI = t.mu.rightMLAI
		t.mu.leftRef = t.mu.rightRef
		t.mu.rightMLAI = map[roachpb.RangeID]ctpb.LAI{}
		t.mu.rightRef = 0

		t.mu.next = next
	}
	closed = t.mu.closed

	return closed, mlai
}

// Track is called before evaluating a proposal. It returns the minimum
// timestamp at which the proposal can be evaluated (i.e. the request timestamp
// needs to be forwarded if necessary), and acquires a reference with the
// Tracker. This reference is released by calling the returned closure either
// a) before proposing the command, supplying the Lease Applied Index at which
//    the proposal will be carried out, or
// b) with zero arguments if the command won't end up being proposed (i.e. hit
//    an error during evaluation).
//
// The closure is not thread safe. For convenience, it may be called with zero
// arguments once after a regular call.
func (t *Tracker) Track(
	ctx context.Context,
) (hlc.Timestamp, func(context.Context, roachpb.RangeID, ctpb.LAI)) {
	shouldLog := log.V(3)

	t.mu.Lock()
	minProp := t.mu.next.Next()
	t.mu.rightRef++
	t.mu.Unlock()

	if shouldLog {
		log.Infof(ctx, "track: proposal on the right at minProp %s", minProp)
	}

	var calls int
	release := func(ctx context.Context, rangeID roachpb.RangeID, lai ctpb.LAI) {
		calls++
		if calls != 1 {
			if lai != 0 || rangeID != 0 || calls > 2 {
				log.Fatal(ctx, log.Safe(fmt.Sprintf("command released %d times, this time with arguments (%d, %d)", calls, rangeID, lai)))
			}
			return
		}

		t.mu.Lock()
		defer t.mu.Unlock()
		if minProp == t.mu.closed.Next() {
			if shouldLog {
				log.Infof(ctx, "release: minprop %s on r%d@%d tracked on the left", minProp, rangeID, lai)
			}
			t.mu.leftRef--
			if t.mu.leftRef < 0 {
				log.Fatalf(ctx, "min proposal left ref count < 0")
			}
			if rangeID == 0 {
				return
			}

			if curLAI, found := t.mu.leftMLAI[rangeID]; !found || curLAI < lai {
				t.mu.leftMLAI[rangeID] = lai
			}
		} else if minProp == t.mu.next.Next() {
			if shouldLog {
				log.Infof(ctx, "release: minprop %s on r%d@%d tracked on the right", minProp, rangeID, lai)
			}
			t.mu.rightRef--
			if t.mu.rightRef < 0 {
				log.Fatalf(ctx, "min proposal right ref count < 0")
			}
			if rangeID == 0 {
				return
			}
			if curLAI, found := t.mu.rightMLAI[rangeID]; !found || curLAI < lai {
				t.mu.rightMLAI[rangeID] = lai
			}
		} else {
			log.Fatalf(ctx, "min proposal %s not tracked under closed (%s) or next (%s) timestamp", minProp, t.mu.closed, t.mu.next)
		}
	}

	return minProp, release
}
