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
		closed      hlc.Timestamp
		closedEpoch ctpb.Epoch

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

		// maxEpoch tracks the highest liveness epoch observed for any untracked
		// proposals. Tracking a maxEpoch allows the MPT to provide some MLAI
		// information about the current epoch when calls to Close straddle multiple
		// different epochs. Before epoch tracking was added the client of the MPT
		// was forced to assume that the MLAI information from the current call to
		// Close corresponded to the highest known epoch as of the previous call to
		// Close. This is problematic in cases where an epoch change leads to a
		// lease change for an otherwise quiescent range. If this mechanism were
		// not in place then the client would never learn about an MLAI for the
		// current epoch.
		leftEpoch  ctpb.Epoch
		rightEpoch ctpb.Epoch
	}
}

var _ closedts.TrackerI = (*Tracker)(nil)

// NewTracker returns a Tracker initialized to a closed timestamp of zero and
// a next closed timestamp of one logical tick past zero.
func NewTracker() *Tracker {
	t := &Tracker{}
	const initialEpoch = 1
	t.mu.closedEpoch = initialEpoch
	t.mu.leftEpoch = initialEpoch
	t.mu.rightEpoch = initialEpoch
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
//
// Callers additionally provide the current expected epoch value. If that value
// is older than the currently tracked data then the timestamp will fail to be
// closed. If the expected epoch value is older than the epoch tracked on the
// left but corresponds to the epoch of the previous successful close then the
// previous closed timestamp is returned along with a nil map like the
// unsuccessful close scenario due to unreleased proposals. This behavior
// enables the client to successfully obtain the tracked data at the newer epoch
// in a later query after its epoch has updated. If the caller's expected epoch
// is even older than the previously returned epoch then zero values are
// returned. If the caller's expected epoch is newer than that of tracked data
// the state of the tracker is progressed but zero values are returned.
func (t *Tracker) Close(
	next hlc.Timestamp, expCurEpoch ctpb.Epoch,
) (ts hlc.Timestamp, mlai map[roachpb.RangeID]ctpb.LAI, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if log.V(3) {
		log.Infof(context.TODO(),
			"close: leftRef=%d (ep: %d) rightRef=%d (ep: %d) next=%s closed=%s@ (ep: %d) new=%s (ep: %d)",
			t.mu.leftRef, t.mu.leftEpoch, t.mu.rightRef, t.mu.rightEpoch, t.mu.next,
			t.mu.closed, t.mu.closedEpoch, next, expCurEpoch)
	}

	// Make sure to not let `t.mu.next` regress, or we'll accept proposals
	// that violate earlier closed timestamps. (And if it stayed the same
	// the logic in the closure returned from Track would fall apart).
	canClose := t.mu.leftRef == 0 && t.mu.next.Less(next)

	// NB: the expected closed epoch may not match the epoch for the timestamp
	// we are currently closing. If the expected closed epoch is earlier than the
	// client likely read its liveness just before an epoch change and we should
	// not move the tracker state as the client will likely visit again with the
	// new epoch and would like the tracked information. If the expCurEpoch is
	// greater than or equal to the current epoch close out the current	timestamp,
	// deferring the decision about whether to return the updated state based on
	// epoch until afterwards.
	if canClose && t.mu.leftEpoch <= expCurEpoch {
		// NB: if rightRef is also zero, then nothing is in flight right now and
		// we could theoretically close out `next`. However, we'd also have to
		// merge the left and right MLAI maps, and would force followers to
		// catch up to more commands much more rapidly than can be expected of
		// them. If we want to make use of this optimization, we should emit
		// two closed timestamp updates for this case.
		t.mu.closed = t.mu.next
		t.mu.closedEpoch = t.mu.leftEpoch
		mlai = t.mu.leftMLAI

		// If the expCurEpoch is after the epoch tracked on the right, we'll never
		// be able to use that information so clear it.
		if t.mu.rightEpoch < expCurEpoch {
			t.mu.rightEpoch = expCurEpoch
			clearMLAIMap(t.mu.rightMLAI)
		}

		// `next` moves forward to the provided timestamp, and picks up the
		// right refcount and MLAIs (so that it is now responsible for tracking
		// everything that's in-flight).
		t.mu.leftMLAI = t.mu.rightMLAI
		t.mu.leftRef = t.mu.rightRef
		t.mu.leftEpoch = t.mu.rightEpoch
		t.mu.rightMLAI = map[roachpb.RangeID]ctpb.LAI{}
		t.mu.rightRef = 0

		t.mu.next = next
	}

	if t.mu.closedEpoch != expCurEpoch {
		return hlc.Timestamp{}, nil, false
	}
	return t.mu.closed, mlai, true
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
// The ReleaseFunc is not thread safe. For convenience, it may be called with
// zero arguments once after a regular call.
func (t *Tracker) Track(ctx context.Context) (hlc.Timestamp, closedts.ReleaseFunc) {
	shouldLog := log.V(3)

	t.mu.Lock()
	minProp := t.mu.next.Next()
	t.mu.rightRef++
	t.mu.Unlock()

	if shouldLog {
		log.Infof(ctx, "track: proposal on the right at minProp %s", minProp)
	}

	var calls int
	release := func(ctx context.Context, epoch ctpb.Epoch, rangeID roachpb.RangeID, lai ctpb.LAI) {
		calls++
		if calls != 1 {
			if lai != 0 || rangeID != 0 || calls > 2 {
				log.Fatal(ctx, log.Safe(fmt.Sprintf("command released %d times, this time with arguments (%d, %d)", calls, rangeID, lai)))
			}
			return
		}
		t.release(ctx, minProp, epoch, rangeID, lai, shouldLog)
	}

	return minProp, release
}

// release is the business logic to release properly account for the release of
// a tracked proposal. It is called from the ReleaseFunc closure returned from
// Track.
func (t *Tracker) release(
	ctx context.Context,
	minProp hlc.Timestamp,
	epoch ctpb.Epoch,
	rangeID roachpb.RangeID,
	lai ctpb.LAI,
	shouldLog bool,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var left bool
	if minProp == t.mu.closed.Next() {
		left = true
	} else if minProp == t.mu.next.Next() {
		left = false
	} else {
		log.Fatalf(ctx, "min proposal %s not tracked under closed (%s) or next (%s) timestamp", minProp, t.mu.closed, t.mu.next)
	}
	// If the epoch has increased we need to clear the current MLAI maps as
	// they pertain to a previous epoch. Tracking LAI relative to the
	// latest epoch allows the Tracker to provide a client with information.
	// The below code clears the left map if this update corresponds to data on
	// the left and clears the right map if this update is at a higher epoch
	// than the right regardless of whether it's on the right or left.
	if left && epoch > t.mu.leftEpoch {
		t.mu.leftEpoch = epoch
		clearMLAIMap(t.mu.leftMLAI)
	}
	if epoch > t.mu.rightEpoch {
		t.mu.rightEpoch = epoch
		clearMLAIMap(t.mu.rightMLAI)
	}
	if left {
		releaseProposal(ctx, "left", shouldLog, minProp, rangeID, lai,
			&t.mu.leftRef, t.mu.leftMLAI, t.mu.leftEpoch != epoch)
	} else {
		releaseProposal(ctx, "right", shouldLog, minProp, rangeID, lai,
			&t.mu.rightRef, t.mu.rightMLAI, t.mu.rightEpoch != epoch)
	}
}

func clearMLAIMap(m map[roachpb.RangeID]ctpb.LAI) {
	for rangeID := range m {
		delete(m, rangeID)
	}
}

func releaseProposal(
	ctx context.Context,
	side string,
	shouldLog bool,
	minProp hlc.Timestamp,
	rangeID roachpb.RangeID,
	lai ctpb.LAI,
	refs *int,
	mlaiMap map[roachpb.RangeID]ctpb.LAI,
	fromPreviousEpoch bool,
) {
	if shouldLog {
		log.Infof(ctx, "release: minprop %s on r%d@%d tracked on the %s", minProp, rangeID, lai, side)
	}
	*refs--
	if *refs < 0 {
		log.Fatalf(ctx, "min proposal %s ref count < 0", side)
	}
	if rangeID == 0 {
		return
	}
	if !fromPreviousEpoch {
		if curLAI, found := mlaiMap[rangeID]; !found || curLAI < lai {
			mlaiMap[rangeID] = lai
		}
	}
}
