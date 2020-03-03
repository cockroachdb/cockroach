// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// TODO(tbg): rename this package. `lbsplit`?

package split

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const minSplitSuggestionInterval = time.Minute

// A Decider collects measurements about the activity (measured in qps) on a
// Replica and, assuming that qps thresholds are exceeded, tries to determine
// a split key that would approximately result in halving the load on each of
// the resultant ranges.
//
// Operations should call `Record` with a current timestamp. Operation counts
// are aggregated over a second and a qps computed. If the QPS is above threshold,
// a split finder is instantiated and the spans supplied to Record are sampled
// for a duration (on the order of ten seconds). Assuming that load consistently
// remains over threshold, and the workload touches a diverse enough set of keys
// to benefit from a split, sampling will eventually instruct a caller of Record
// to carry out a split. When the split is initiated, it can obtain the suggested
// split point from MaybeSplitKey (which may have disappeared either due to a drop
// in qps or a change in the workload).
type Decider struct {
	intn         func(n int) int // supplied to Init
	qpsThreshold func() float64  // supplied to Init

	mu struct {
		syncutil.Mutex
		lastQPSRollover time.Time // most recent time recorded by requests.
		qps             float64   // last reqs/s rate as of lastQPSRollover

		count               int64     // number of requests recorded since last rollover
		splitFinder         *Finder   // populated when engaged or decided
		lastSplitSuggestion time.Time // last stipulation to client to carry out split
	}
}

// Init initializes a Decider (which is assumed to be zero). The signature allows
// embedding the Decider into a larger struct outside of the scope of this package
// without incurring a pointer reference. This is relevant since many Deciders
// may exist in the system at any given point in time.
func Init(lbs *Decider, intn func(n int) int, qpsThreshold func() float64) {
	lbs.intn = intn
	lbs.qpsThreshold = qpsThreshold
}

// Record notifies the Decider that 'n' operations are being carried out which
// operate on the span returned by the supplied method. The closure will only
// be called when necessary, that is, when the Decider is considering a split
// and is sampling key spans to determine a suitable split point.
//
// If the returned boolean is true, a split key is available (though it may
// disappear as more keys are sampled) and should be initiated by the caller,
// which can call MaybeSplitKey to retrieve the suggested key.
func (d *Decider) Record(now time.Time, n int, span func() roachpb.Span) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.recordLocked(now, n, span)
}

func (d *Decider) recordLocked(now time.Time, n int, span func() roachpb.Span) bool {
	d.mu.count += int64(n)

	// First compute requests per second since the last check.
	elapsedSinceLastQPS := now.Sub(d.mu.lastQPSRollover)
	if elapsedSinceLastQPS >= time.Second {
		if elapsedSinceLastQPS > 2*time.Second {
			// Force a QPS of zero; there wasn't any activity within the last
			// second at all.
			d.mu.count = 0
		}
		// Update the QPS and reset the time and request counter.
		d.mu.qps = (float64(d.mu.count) / float64(elapsedSinceLastQPS)) * 1e9
		d.mu.lastQPSRollover = now
		d.mu.count = 0

		// If the QPS for the range exceeds the threshold, start actively
		// tracking potential for splitting this range based on load.
		// This tracking will begin by initiating a splitFinder so it can
		// begin to Record requests so it can find a split point. If a
		// splitFinder already exists, we check if a split point is ready
		// to be used.
		if d.mu.qps >= d.qpsThreshold() {
			if d.mu.splitFinder == nil {
				d.mu.splitFinder = NewFinder(now)
			}
		} else {
			d.mu.splitFinder = nil
		}
	}

	if d.mu.splitFinder != nil && n != 0 {
		s := span()
		if s.Key != nil {
			d.mu.splitFinder.Record(span(), d.intn)
		}
		if now.Sub(d.mu.lastSplitSuggestion) > minSplitSuggestionInterval && d.mu.splitFinder.Ready(now) && d.mu.splitFinder.Key() != nil {
			d.mu.lastSplitSuggestion = now
			return true
		}
	}
	return false
}

// LastQPS returns the most recent QPS measurement.
func (d *Decider) LastQPS(now time.Time) float64 {
	d.mu.Lock()
	d.recordLocked(now, 0, nil)
	qps := d.mu.qps
	d.mu.Unlock()

	return qps
}

// MaybeSplitKey returns a key to perform a split at. The return value will be
// nil if either the Decider hasn't decided that a split should be carried out
// or if it wasn't able to determine a suitable split key.
//
// It is legal to call MaybeSplitKey at any time.
func (d *Decider) MaybeSplitKey(now time.Time) roachpb.Key {
	var key roachpb.Key

	d.mu.Lock()
	d.recordLocked(now, 0, nil)
	if d.mu.splitFinder != nil && d.mu.splitFinder.Ready(now) {
		// We've found a key to split at. This key might be in the middle of a
		// SQL row. If we fail to rectify that, we'll cause SQL crashes:
		//
		// https://github.com/cockroachdb/cockroach/pull/42056
		//
		// While the behavior at the SQL level is arguably bad and should be
		// fixed, splitting between column families is also never a good idea
		// for performance in general. So, if the split key is, say
		//
		//   /Table/51/52/53/54/55/9/1
		//
		// then we want to split instead at
		//
		//   /Table/51/52/53/54/55
		//
		// (see TestDeciderCallsEnsureSafeSplitKey).
		//
		// The key found here isn't guaranteed to be a valid SQL column family
		// key. This is because the keys are sampled from StartKey of requests
		// hitting this replica. Ranged operations may well wish to exclude the
		// start point by calling .Next() or may span multiple ranges, and so
		// such a key may end up being passed to EnsureSafeSplitKey here.
		//
		// We take the risk that the result may sometimes not be a good split
		// point (or even in this range).
		//
		// Note that we ignore EnsureSafeSplitKey when it returns an error since
		// that error only tells us that this key couldn't possibly be a SQL
		// key. This is more common than one might think since SQL issues plenty
		// of scans over all column families, meaning that we'll frequently find
		// a key that has no column family suffix and thus errors out in
		// EnsureSafeSplitKey.
		key = d.mu.splitFinder.Key()
		if safeKey, err := keys.EnsureSafeSplitKey(key); err == nil {
			key = safeKey
		}
	}
	d.mu.Unlock()

	return key
}

// Reset deactivates any current attempt at determining a split key.
func (d *Decider) Reset() {
	d.mu.Lock()
	d.mu.splitFinder = nil
	d.mu.count = 0
	d.mu.Unlock()
}
