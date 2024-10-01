// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// TODO(tbg): rename this package. `lbsplit`?

package split

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/redact"
)

const minSplitSuggestionInterval = time.Minute
const minNoSplitKeyLoggingMetricsInterval = time.Minute
const minPerSecondSampleDuration = time.Second

type LoadBasedSplitter interface {
	redact.SafeFormatter
	// Record informs the LoadBasedSplitter about where the span lies with regard
	// to the keys in the samples.
	Record(span roachpb.Span, weight float64)

	// Key finds an appropriate split point from the sampled candidate split
	// keys. Returns a nil key if no appropriate key was found.
	Key() roachpb.Key

	// Ready checks if the LoadBasedSplitter has been initialized with a
	// sufficient sample duration.
	Ready(nowTime time.Time) bool

	// NoSplitKeyCauseLogMsg returns a log message containing information on the
	// number of samples that don't pass each split key requirement if not all
	// samples are invalid due to insufficient counters, otherwise returns an
	// empty string.
	NoSplitKeyCauseLogMsg() redact.RedactableString

	// PopularKeyFrequency returns the percentage that the most popular key
	// appears in the sampled candidate split keys.
	PopularKeyFrequency() float64

	// String formats the state of the load based splitter.
	String() string
}

type LoadSplitConfig interface {
	// NewLoadBasedSplitter returns a new LoadBasedSplitter that may be used to
	// find the midpoint based on recorded load.
	NewLoadBasedSplitter(time.Time, SplitObjective) LoadBasedSplitter
	// StatRetention returns the duration that recorded load is to be retained.
	StatRetention() time.Duration
	// StatThreshold returns the threshold for load above which the range
	// should be considered split.
	StatThreshold(SplitObjective) float64
}

type RandSource interface {
	// Float64 returns, as a float64, a pseudo-random number in the half-open
	// interval [0.0,1.0) from the RandSource.
	Float64() float64

	// Intn returns, as an int, a non-negative pseudo-random number in the
	// half-open interval [0,n).
	Intn(n int) int
}

// globalRandSource implements the RandSource interface.
type globalRandSource struct{}

// Float64 returns, as a float64, a pseudo-random number in the half-open
// interval [0.0,1.0) from the RandSource.
func (g globalRandSource) Float64() float64 {
	return rand.Float64()
}

// Intn returns, as an int, a non-negative pseudo-random number in the
// half-open interval [0,n).
func (g globalRandSource) Intn(n int) int {
	return rand.Intn(n)
}

// GlobalRandSource returns an implementation of the RandSource interface that
// redirects calls to the global rand.
func GlobalRandSource() RandSource {
	return globalRandSource{}
}

// A Decider collects measurements about the load activity on a
// Replica and, assuming that load thresholds are exceeded, tries to determine a
// split key that would approximately result in halving the load on each of the
// resultant ranges. Similarly, these measurements are used to determine when a
// range is serving sufficiently little load, such that it should be allowed to
// merge with its left or right hand neighbor.
//
// Operations should call `Record` with a current timestamp. Operation counts
// are aggregated over a second and a load-per-second is computed.
//
// If the load is above a threshold, a split finder is instantiated and the spans
// supplied to Record are sampled for a duration (on the order of ten seconds).
// Assuming that load consistently remains over threshold, and the workload
// touches a diverse enough set of keys to benefit from a split, sampling will
// eventually instruct a caller of Record to carry out a split. When the split
// is initiated, it can obtain the suggested split point from MaybeSplitKey
// (which may have disappeared either due to a drop in qps or a change in the
// workload).
//
// These second-long load samples are also aggregated together to track the
// maximum historical load over a configurable retention period. This maximum load
// measurement, which is accessible through the MaxStat method, can be used to
// prevent load-based splits from being merged away until the resulting ranges
// have consistently remained below a certain load threshold for a sufficiently
// long period of time.
//
// The Decider also maintains ownership of the SplitObjective. The
// SplitObjective controls which load stat threshold and split finder are used.
// We keep the SplitObjective under the finder mutex to prevent inconsistency
// that could result from separate calls to the decider, then split objective.

// LoadSplitterMetrics consists of metrics for load-based splitter split key.
type LoadSplitterMetrics struct {
	PopularKeyCount *metric.Counter
	NoSplitKeyCount *metric.Counter
}

// Decider tracks the latest load and if certain conditions are met, records
// incoming requests to find potential split keys and checks if sampled
// candidate split keys satisfy certain requirements.
type Decider struct {
	loadSplitterMetrics *LoadSplitterMetrics // supplied to Init
	config              LoadSplitConfig      // supplied to Init

	mu struct {
		syncutil.Mutex
		objective SplitObjective // supplied to Init

		// Fields tracking the current qps sample.
		lastStatRollover time.Time // most recent time recorded by requests.
		lastStatVal      float64   // last reqs/s rate as of lastStatRollover
		count            int64     // number of requests recorded since last rollover

		// Fields tracking historical qps samples.
		maxStat maxStatTracker

		// Fields tracking split key suggestions.
		splitFinder         LoadBasedSplitter // populated when engaged or decided
		lastSplitSuggestion time.Time         // last stipulation to client to carry out split
		suggestionsMade     int               // suggestions made since last reset

		// Fields tracking logging / metrics around load-based splitter split key.
		lastNoSplitKeyLoggingMetrics time.Time
	}
}

// Init initializes a Decider (which is assumed to be zero). The signature allows
// embedding the Decider into a larger struct outside of the scope of this package
// without incurring a pointer reference. This is relevant since many Deciders
// may exist in the system at any given point in time.
func Init(
	lbs *Decider,
	config LoadSplitConfig,
	loadSplitterMetrics *LoadSplitterMetrics,
	objective SplitObjective,
) {
	lbs.loadSplitterMetrics = loadSplitterMetrics
	lbs.config = config
	lbs.mu.objective = objective
}

type lockedDecider Decider

func (ld *lockedDecider) SafeFormat(w redact.SafePrinter, r rune) {
	w.Printf(
		"objective=%v count=%d suggestions=%d last=%.1f last-roll=%v last-suggest=%v",
		ld.mu.objective, ld.mu.count, ld.mu.suggestionsMade, ld.mu.lastStatVal,
		ld.mu.lastStatRollover, ld.mu.lastSplitSuggestion,
	)
	if ld.mu.splitFinder != nil {
		w.Printf(" %v", ld.mu.splitFinder)
	}
}

func (ld *lockedDecider) String() string {
	return redact.StringWithoutMarkers(ld)
}

// Record notifies the Decider that 'n' operations are being carried out which
// operate on the span returned by the supplied method. The closure will only
// be called when necessary, that is, when the Decider is considering a split
// and is sampling key spans to determine a suitable split point.
//
// If the returned boolean is true, a split key is available (though it may
// disappear as more keys are sampled) and should be initiated by the caller,
// which can call MaybeSplitKey to retrieve the suggested key.
func (d *Decider) Record(
	ctx context.Context, now time.Time, load func(SplitObjective) int, span func() roachpb.Span,
) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.recordLocked(ctx, now, load(d.mu.objective), span)
}

func (d *Decider) recordLocked(
	ctx context.Context, now time.Time, n int, span func() roachpb.Span,
) bool {
	d.mu.count += int64(n)

	// First compute requests per second since the last check.
	if d.mu.lastStatRollover.IsZero() {
		d.mu.lastStatRollover = now
	}
	elapsedSinceLastSample := now.Sub(d.mu.lastStatRollover)
	if elapsedSinceLastSample >= minPerSecondSampleDuration {
		// Update the latest stat value and reset the time and request counter.
		d.mu.lastStatVal = (float64(d.mu.count) / float64(elapsedSinceLastSample)) * 1e9
		d.mu.lastStatRollover = now
		d.mu.count = 0

		// Record the latest stat sample in the historical tracker.
		d.mu.maxStat.record(now, d.config.StatRetention(), d.mu.lastStatVal)

		// If the stat for the range exceeds the threshold, start actively
		// tracking potential for splitting this range based on load.
		// This tracking will begin by initiating a splitFinder so it can
		// begin to Record requests so it can find a split point. If a
		// splitFinder already exists, we check if a split point is ready
		// to be used.
		if d.mu.lastStatVal >= d.config.StatThreshold(d.mu.objective) {
			if d.mu.splitFinder == nil {
				d.mu.splitFinder = d.config.NewLoadBasedSplitter(now, d.mu.objective)
			}
		} else {
			d.mu.splitFinder = nil
		}
	}

	if d.mu.splitFinder != nil && n != 0 {
		s := span()
		if s.Key != nil {
			d.mu.splitFinder.Record(span(), float64(n))
		}
		// We don't want to check for a split key if we don't need to as it
		// requires some computation. When the splitFinder isn't ready or we
		// recently suggested a split, skip the key check.
		if d.mu.splitFinder.Ready(now) &&
			now.Sub(d.mu.lastSplitSuggestion) > minSplitSuggestionInterval {
			if splitKey := d.mu.splitFinder.Key(); splitKey != nil {
				log.KvDistribution.VEventf(ctx, 3, "suggesting split key %v splitter_state=%v",
					splitKey, (*lockedDecider)(d))
				d.mu.lastSplitSuggestion = now
				d.mu.suggestionsMade++
				return true
			} else {
				if now.Sub(d.mu.lastNoSplitKeyLoggingMetrics) > minNoSplitKeyLoggingMetricsInterval {
					d.mu.lastNoSplitKeyLoggingMetrics = now
					if causeMsg := d.mu.splitFinder.NoSplitKeyCauseLogMsg(); causeMsg != "" {
						popularKeyFrequency := d.mu.splitFinder.PopularKeyFrequency()
						log.KvDistribution.Infof(ctx, "%s, most popular key occurs in %d%% of samples",
							causeMsg, int(popularKeyFrequency*100))
						log.KvDistribution.VInfof(ctx, 3, "splitter_state=%v", (*lockedDecider)(d))
						if popularKeyFrequency >= splitKeyThreshold {
							d.loadSplitterMetrics.PopularKeyCount.Inc(1)
						}
						d.loadSplitterMetrics.NoSplitKeyCount.Inc(1)
					}
				}
			}
		}
	}
	return false
}

// RecordMax adds a stat measurement directly into the Decider's historical
// stat value tracker. The stat sample is considered to have been captured at
// the provided time.
func (d *Decider) RecordMax(now time.Time, qps float64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mu.maxStat.record(now, d.config.StatRetention(), qps)
}

// lastStatLocked returns the most recent stat measurement.
func (d *Decider) lastStatLocked(ctx context.Context, now time.Time) float64 {
	d.recordLocked(ctx, now, 0, nil) // force stat computation
	return d.mu.lastStatVal
}

// maxStatLocked returns the maximum stat measurement recorded over the retention
// period. If the Decider has not been recording for a full retention period,
// the method returns false.
func (d *Decider) maxStatLocked(ctx context.Context, now time.Time) (float64, bool) {
	d.recordLocked(ctx, now, 0, nil) // force stat computation
	return d.mu.maxStat.max(now, d.config.StatRetention())
}

// MaybeSplitKey returns a key to perform a split at. The return value will be
// nil if either the Decider hasn't decided that a split should be carried out
// or if it wasn't able to determine a suitable split key.
//
// It is legal to call MaybeSplitKey at any time.
// WARNING: The key returned from MaybeSplitKey has no guarantee of being a
// safe split key. The key is derived from sampled spans. See below.
func (d *Decider) MaybeSplitKey(ctx context.Context, now time.Time) roachpb.Key {
	var key roachpb.Key

	d.mu.Lock()
	defer d.mu.Unlock()

	d.recordLocked(ctx, now, 0, nil)
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
		// The key found here isn't guaranteed to be a valid SQL column family key.
		// This is because the keys are sampled from StartKey and EndKey of
		// requests hitting this replica. Ranged operations may well wish to
		// exclude the start point by calling .Next() or may span multiple ranges,
		// and so such a key may end up being returned. This is more common than
		// one might think since SQL issues plenty of scans over all column
		// families, meaning that we'll frequently find a key that has no column
		// family suffix and thus errors out in EnsureSafeSplitKey.
		//
		// We do not attempt to validate the key is safe here, simply return it to
		// the caller as the best possible split point found so far. See
		// replica.adminSplitWithDescriptor for how split keys are handled when we
		// aren't certain the provided key is safe.
		key = d.mu.splitFinder.Key()
	}
	return key
}

// Reset deactivates any current attempt at determining a split key. The method
// also discards any historical stat tracking information.
func (d *Decider) Reset(now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.resetLocked(now)
}

func (d *Decider) resetLocked(now time.Time) {
	d.mu.lastStatRollover = time.Time{}
	d.mu.lastStatVal = 0
	d.mu.count = 0
	d.mu.maxStat.reset(now, d.config.StatRetention())
	d.mu.splitFinder = nil
	d.mu.suggestionsMade = 0
	d.mu.lastSplitSuggestion = time.Time{}
	d.mu.lastNoSplitKeyLoggingMetrics = time.Time{}
}

// SetSplitObjective sets the decider split objective to the given value and
// discards any existing state.
func (d *Decider) SetSplitObjective(now time.Time, obj SplitObjective) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mu.objective = obj
	d.resetLocked(now)
}

// LoadSplitSnapshot contains a consistent snapshot of the decider state. It
// should be used when comparing the threshold, last value, max value or split
// objective; it is possible for inconsistent values otherwise e.g.
//
//	p1 max_stat = decider.MaxStat()
//	p2 decider.SetSplitObjective(...)
//	p1 threshold = decider.threshold(..) (doesn't exist for this reason)
//
//	p1 then asserts that max_stat < threshold, however the threhsold value
//	will be in terms of the split objective set by p2; which could be widly
//	wrong.
type LoadSplitSnapshot struct {
	SplitObjective       SplitObjective
	Max, Last, Threshold float64
	Ok                   bool
}

// Snapshot returns a consistent snapshot of the decider state.
func (d *Decider) Snapshot(ctx context.Context, now time.Time) LoadSplitSnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()

	maxStat, ok := d.maxStatLocked(ctx, now)
	lastStat := d.lastStatLocked(ctx, now)
	threshold := d.config.StatThreshold(d.mu.objective)

	return LoadSplitSnapshot{
		SplitObjective: d.mu.objective,
		Max:            maxStat,
		Last:           lastStat,
		Ok:             ok,
		Threshold:      threshold,
	}
}

// maxStatTracker collects a series of stat per-second measurement samples and
// tracks the maximum observed over a period of time.
//
// The tracker internally uses a set of time windows in order to age out old
// partial aggregations without having to do hard resets. The `windows` array is
// a circular buffer of the last N windows of stats. We rotate through the
// circular buffer every so often as determined by `minRetention`.
//
// The tracker can be queried through its `max` method, which returns the
// maximum of all queries-per-second samples recorded over the retention period.
// If the tracker has not been recording for a full retention period, then the
// method returns false.
//
// The zero-value of a maxStatTracker can be used immediately.
type maxStatTracker struct {
	windows      [6]float64
	curIdx       int
	curStart     time.Time
	lastReset    time.Time
	minRetention time.Duration
}

// record adds the qps sample to the tracker.
func (t *maxStatTracker) record(now time.Time, minRetention time.Duration, qps float64) {
	t.maybeReset(now, minRetention)
	t.maybeRotate(now)
	t.windows[t.curIdx] = max(t.windows[t.curIdx], qps)
}

// reset clears the tracker. maxStatTracker will begin returning false until a full
// minRetention period has elapsed.
func (t *maxStatTracker) reset(now time.Time, minRetention time.Duration) {
	if minRetention <= 0 {
		panic("minRetention must be positive")
	}
	t.windows = [6]float64{}
	t.curIdx = 0
	t.curStart = now
	t.lastReset = now
	t.minRetention = minRetention
}

func (t *maxStatTracker) maybeReset(now time.Time, minRetention time.Duration) {
	// If the retention period changes, simply reset the entire tracker. Merging
	// or splitting windows would be a difficult task and could lead to samples
	// either not being retained for long-enough, or being retained for too long.
	// Resetting indicates to max that a new retention period needs to be
	// measured before accurate results can be returned.
	if minRetention != t.minRetention {
		t.reset(now, minRetention)
	}
}

func (t *maxStatTracker) maybeRotate(now time.Time) {
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

// max returns the maximum queries-per-second samples recorded over the last
// retention period. If the tracker has not been recording for a full retention
// period, then the method returns false.
func (t *maxStatTracker) max(now time.Time, minRetention time.Duration) (float64, bool) {
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

func (t *maxStatTracker) windowWidth() time.Duration {
	// NB: -1 because during a rotation, only len(t.windows)-1 windows survive.
	return t.minRetention / time.Duration(len(t.windows)-1)
}
