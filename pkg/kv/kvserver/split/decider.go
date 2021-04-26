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
const minQueriesPerSecondSampleDuration = time.Second

// A Decider collects measurements about the activity (measured in qps) on a
// Replica and, assuming that qps thresholds are exceeded, tries to determine a
// split key that would approximately result in halving the load on each of the
// resultant ranges. Similarly, these measurements are used to determine when a
// range is serving sufficiently little load, such that it should be allowed to
// merge with its left or right hand neighbor.
//
// Operations should call `Record` with a current timestamp. Operation counts
// are aggregated over a second and a QPS is computed.
//
// If the QPS is above a threshold, a split finder is instantiated and the spans
// supplied to Record are sampled for a duration (on the order of ten seconds).
// Assuming that load consistently remains over threshold, and the workload
// touches a diverse enough set of keys to benefit from a split, sampling will
// eventually instruct a caller of Record to carry out a split. When the split
// is initiated, it can obtain the suggested split point from MaybeSplitKey
// (which may have disappeared either due to a drop in qps or a change in the
// workload).
//
// These second-long QPS samples are also aggregated together to track the
// maximum historical QPS over a configurable retention period. This maximum QPS
// measurement, which is accessible through the MaxQPS method, can be used to
// prevent load-based splits from being merged away until the resulting ranges
// have consistently remained below a certain QPS threshold for a sufficiently
// long period of time.
type Decider struct {
	intn         func(n int) int      // supplied to Init
	qpsThreshold func() float64       // supplied to Init
	qpsRetention func() time.Duration // supplied to Init

	mu struct {
		syncutil.Mutex

		// Fields tracking the current qps sample.
		lastQPSRollover time.Time // most recent time recorded by requests.
		lastQPS         float64   // last reqs/s rate as of lastQPSRollover
		count           int64     // number of requests recorded since last rollover

		// Fields tracking historical qps samples.
		maxQPS maxQPSTracker

		// Fields tracking split key suggestions.
		splitFinder         *Finder   // populated when engaged or decided
		lastSplitSuggestion time.Time // last stipulation to client to carry out split
	}
}

// Init initializes a Decider (which is assumed to be zero). The signature allows
// embedding the Decider into a larger struct outside of the scope of this package
// without incurring a pointer reference. This is relevant since many Deciders
// may exist in the system at any given point in time.
func Init(
	lbs *Decider,
	intn func(n int) int,
	qpsThreshold func() float64,
	qpsRetention func() time.Duration,
) {
	lbs.intn = intn
	lbs.qpsThreshold = qpsThreshold
	lbs.qpsRetention = qpsRetention
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
	if d.mu.lastQPSRollover.IsZero() {
		d.mu.lastQPSRollover = now
	}
	elapsedSinceLastQPS := now.Sub(d.mu.lastQPSRollover)
	if elapsedSinceLastQPS >= minQueriesPerSecondSampleDuration {
		// Update the latest QPS and reset the time and request counter.
		d.mu.lastQPS = (float64(d.mu.count) / float64(elapsedSinceLastQPS)) * 1e9
		d.mu.lastQPSRollover = now
		d.mu.count = 0

		// Record the latest QPS sample in the historical tracker.
		d.mu.maxQPS.record(now, d.qpsRetention(), d.mu.lastQPS)

		// If the QPS for the range exceeds the threshold, start actively
		// tracking potential for splitting this range based on load.
		// This tracking will begin by initiating a splitFinder so it can
		// begin to Record requests so it can find a split point. If a
		// splitFinder already exists, we check if a split point is ready
		// to be used.
		if d.mu.lastQPS >= d.qpsThreshold() {
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

// RecordMax adds a QPS measurement directly into the Decider's historical QPS
// tracker. The QPS sample is considered to have been captured at the provided
// time.
func (d *Decider) RecordMax(now time.Time, qps float64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mu.maxQPS.record(now, d.qpsRetention(), qps)
}

// LastQPS returns the most recent QPS measurement.
func (d *Decider) LastQPS(now time.Time) float64 {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.recordLocked(now, 0, nil) // force QPS computation
	return d.mu.lastQPS
}

// MaxQPS returns the maximum QPS measurement recorded over the retention
// period. If the Decider has not been recording for a full retention period,
// the method returns false.
func (d *Decider) MaxQPS(now time.Time) (float64, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.recordLocked(now, 0, nil) // force QPS computation
	return d.mu.maxQPS.maxQPS(now, d.qpsRetention())
}

// MaybeSplitKey returns a key to perform a split at. The return value will be
// nil if either the Decider hasn't decided that a split should be carried out
// or if it wasn't able to determine a suitable split key.
//
// It is legal to call MaybeSplitKey at any time.
func (d *Decider) MaybeSplitKey(now time.Time) roachpb.Key {
	var key roachpb.Key

	d.mu.Lock()
	defer d.mu.Unlock()

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
	return key
}

// Reset deactivates any current attempt at determining a split key. The method
// also discards any historical QPS tracking information.
func (d *Decider) Reset(now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mu.lastQPSRollover = time.Time{}
	d.mu.lastQPS = 0
	d.mu.count = 0
	d.mu.maxQPS.reset(now, d.qpsRetention())
	d.mu.splitFinder = nil
	d.mu.lastSplitSuggestion = time.Time{}
}

// maxQPSTracker collects a series of queries-per-second measurement samples and
// tracks the maximum observed over a period of time.
//
// The tracker internally uses a set of time windows in order to age out old
// partial aggregations without having to do hard resets. The `windows` array is
// a circular buffer of the last N windows of stats. We rotate through the
// circular buffer every so often as determined by `minRetention`.
//
// The tracker can be queried through its `maxQPS` method, which returns the
// maximum of all queries-per-second samples recorded over the retention period.
// If the tracker has not been recording for a full retention period, then the
// method returns false.
//
// The zero-value of a maxQPSTracker can be used immediately.
type maxQPSTracker struct {
	windows      [6]float64
	curIdx       int
	curStart     time.Time
	lastReset    time.Time
	minRetention time.Duration
}

// record adds the qps sample to the tracker.
func (t *maxQPSTracker) record(now time.Time, minRetention time.Duration, qps float64) {
	t.maybeReset(now, minRetention)
	t.maybeRotate(now)
	t.windows[t.curIdx] = max(t.windows[t.curIdx], qps)
}

// reset clears the tracker. maxQPS will begin returning false until a full
// minRetention period has elapsed.
func (t *maxQPSTracker) reset(now time.Time, minRetention time.Duration) {
	if minRetention <= 0 {
		panic("minRetention must be positive")
	}
	t.windows = [6]float64{}
	t.curIdx = 0
	t.curStart = now
	t.lastReset = now
	t.minRetention = minRetention
}

func (t *maxQPSTracker) maybeReset(now time.Time, minRetention time.Duration) {
	// If the retention period changes, simply reset the entire tracker. Merging
	// or splitting windows would be a difficult task and could lead to samples
	// either not being retained for long-enough, or being retained for too long.
	// Resetting indicates to maxQPS that a new retention period needs to be
	// measured before accurate results can be returned.
	if minRetention != t.minRetention {
		t.reset(now, minRetention)
	}
}

func (t *maxQPSTracker) maybeRotate(now time.Time) {
	sinceLastRotate := now.Sub(t.curStart)
	windowWidth := t.windowWidth()
	if sinceLastRotate < windowWidth {
		// Use the existing window.
		return
	}

	shift := int(sinceLastRotate / windowWidth)
	if shift >= len(t.windows) {
		// Clear all windows. We had a long period of inactivity.
		t.windows = [6]float64{}
		t.curIdx = 0
		t.curStart = now
		return
	}
	for i := 0; i < shift; i++ {
		t.curIdx = (t.curIdx + 1) % len(t.windows)
		t.curStart = t.curStart.Add(windowWidth)
		t.windows[t.curIdx] = 0
	}
}

// maxQPS returns the maximum queries-per-second samples recorded over the last
// retention period. If the tracker has not been recording for a full retention
// period, then the method returns false.
func (t *maxQPSTracker) maxQPS(now time.Time, minRetention time.Duration) (float64, bool) {
	t.record(now, minRetention, 0) // expire samples, if necessary

	if now.Sub(t.lastReset) < t.minRetention {
		// The tracker has not been recording for long enough.
		return 0, false
	}

	qps := 0.0
	for _, v := range t.windows {
		qps = max(qps, v)
	}
	return qps, true
}

func (t *maxQPSTracker) windowWidth() time.Duration {
	// NB: -1 because during a rotation, only len(t.windows)-1 windows survive.
	return t.minRetention / time.Duration(len(t.windows)-1)
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
