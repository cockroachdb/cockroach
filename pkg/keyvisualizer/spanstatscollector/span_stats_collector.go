// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanstatscollector

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvissettings"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/container/ring"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// statsBucket implements the interval.Interface interface.
type statsBucket struct {
	sp      roachpb.Span
	id      uintptr
	counter atomic.Uint64
}

var _ interval.Interface = &statsBucket{}

// Range is part of the interval.Interface interface.
func (t *statsBucket) Range() interval.Range {
	return t.sp.AsRange()
}

// ID is part of the interval.Interface interface.
func (t *statsBucket) ID() uintptr {
	return t.id
}

type boundaryUpdate struct {
	boundaries    []roachpb.Span
	scheduledTime time.Time
}

// SpanStatsCollector generates keyvispb.Sample(s).
type SpanStatsCollector struct {
	// tree is an interval.Tree whose buckets are of type statsBucket.
	tree     atomic.Value
	settings *cluster.Settings
	mu       struct {
		syncutil.Mutex
		// A queue maintains boundary updates that should be applied in the
		// future.
		boundaryUpdateQueue []boundaryUpdate
		// A ring buffer is used to persist previous samples until they're
		// requested. After the last empty element is filled, r points to the
		// oldest sample, which is the next sample to be overwritten.
		r *ring.Ring[keyvispb.Sample]
	}
}

func (s *SpanStatsCollector) getTree() interval.Tree {
	return s.tree.Load().(interval.Tree)
}

// New returns a new instance of a SpanStatsCollector.
func New(settings *cluster.Settings) *SpanStatsCollector {
	collector := &SpanStatsCollector{}
	collector.tree.Store(newTreeWithBoundaries(nil))
	collector.settings = settings
	collector.mu.r = ring.New[keyvispb.Sample](5) // Keep the 5 most recent samples.
	return collector
}

// Start begins an async task to run the collector.
func (s *SpanStatsCollector) Start(ctx context.Context, stopper *stop.Stopper) {
	if err := stopper.RunAsyncTask(ctx, "span-stats-collector",
		func(ctx context.Context) {
			s.reset()
			var t timeutil.Timer
			defer t.Stop()
			for {
				samplePeriod := keyvissettings.SampleInterval.Get(&s.settings.SV)
				now := timeutil.Now()

				// The current time is truncated to synchronize the sample
				// period across different nodes.
				nextTick := now.Truncate(samplePeriod).Add(samplePeriod)
				t.Reset(nextTick.Sub(now))
				select {
				case <-t.C:
					t.Read = true
					s.rolloverSample(nextTick)
					s.reset()
				case <-stopper.ShouldQuiesce():
					return
				case <-ctx.Done():
					return
				}
			}
		}); err != nil {
		log.Infof(ctx, "error starting span stats collector: %v", err)
	}
}

// newTreeWithBoundaries will panic if an installed boundary is
// invalid, i.e. the start key is greater than or equal to the end key.
func newTreeWithBoundaries(spans []roachpb.Span) interval.Tree {
	t := interval.NewTree(interval.ExclusiveOverlapper)
	for i, sp := range spans {
		bucket := statsBucket{
			sp: sp,
			id: uintptr(i),
		}
		err := t.Insert(&bucket, false /* fast */)
		if err != nil {
			panic(errors.NewAssertionErrorWithWrappedErrf(
				err, "insert into interval tree failed"))
		}
	}

	return t
}

func resetTree(tree interval.Tree) {
	it := tree.Iterator()
	for {
		i, next := it.Next()
		if !next {
			break
		}
		bucket := i.(*statsBucket)
		bucket.counter.Store(0)
	}
}

func (s *SpanStatsCollector) getStats() []keyvispb.SpanStats {
	t := s.getTree()
	stats := make([]keyvispb.SpanStats, 0, t.Len())

	it := t.Iterator()
	for {
		i, next := it.Next()
		if !next {
			break
		}
		bucket := i.(*statsBucket)
		stats = append(stats, keyvispb.SpanStats{
			Span:     bucket.sp,
			Requests: bucket.counter.Load(),
		})
	}
	return stats
}

// Increment adds 1 to the counter that counts requests for this
// span. If the span does not fall within the previously saved boundaries,
// this is a no-op. If boundaries have not yet been installed,
// this function returns false.
func (s *SpanStatsCollector) Increment(sp roachpb.Span) {
	s.getTree().DoMatching(func(i interval.Interface) (done bool) {
		bucket := i.(*statsBucket)
		bucket.counter.Add(1)
		return false // want more
	}, sp.AsRange())
}

// discardStaleUpdates discards updates that will never be applied.
// An update should be discarded when there are other updates in the past
// that are more recent.
func discardStaleUpdates(currentTime time.Time, updates []boundaryUpdate) []boundaryUpdate {
	for {

		if len(updates) < 2 {
			break
		}

		t1 := updates[0].scheduledTime
		t2 := updates[1].scheduledTime

		if t1.Before(t2) && t2.Before(currentTime) {
			// The boundary update at index 0 will never be applied,
			// so it can be discarded.
			updates[0] = boundaryUpdate{}
			updates = updates[1:]
		} else {
			// The update at boundary index 0 should be applied,
			// so no more discarding is needed.
			break
		}
	}

	return updates
}

// SaveBoundaries stashes the desired collection boundaries. They are installed
// when a sample is rolled over, if the time at rollover is greater than
// scheduledTime.
func (s *SpanStatsCollector) SaveBoundaries(boundaries []roachpb.Span, scheduledTime time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	update := boundaryUpdate{
		boundaries:    boundaries,
		scheduledTime: scheduledTime,
	}

	l := len(s.mu.boundaryUpdateQueue)
	if l > 0 {
		last := s.mu.boundaryUpdateQueue[l-1].scheduledTime
		// The update should only be queued if it is scheduled after the
		// most recently queued update. The update queue should be sorted by
		// scheduled time.
		if scheduledTime.After(last) {
			s.mu.boundaryUpdateQueue = append(s.mu.boundaryUpdateQueue, update)
		}
	} else {
		s.mu.boundaryUpdateQueue = append(s.mu.boundaryUpdateQueue, update)
	}

	s.mu.boundaryUpdateQueue = discardStaleUpdates(
		timeutil.Now(), s.mu.boundaryUpdateQueue)
}

// GetSamples returns samples that are newer than t. Samples are returned in
// timestamp order. The oldest sample will be at the head.
func (s *SpanStatsCollector) GetSamples(t time.Time) []keyvispb.Sample {
	var res []keyvispb.Sample

	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.r.Do(func(s interface{}) {
		if s == nil {
			return
		}
		sample := s.(keyvispb.Sample)
		if sample.SampleTime.After(t) {
			res = append(res, sample)
		}
	})
	return res
}

// reset will clear the collector's tree. If there's a
// pending boundary update, a new tree will be installed with
// those boundaries. If there isn't a pending boundary update,
// the tree is cleared and will re-use its existing boundaries.
func (s *SpanStatsCollector) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If no boundary updates are queued, reset the tree with the current
	// boundaries.
	if len(s.mu.boundaryUpdateQueue) == 0 {
		resetTree(s.getTree())
		return
	}

	now := timeutil.Now()
	s.mu.boundaryUpdateQueue = discardStaleUpdates(
		now, s.mu.boundaryUpdateQueue)
	nextUpdate := s.mu.boundaryUpdateQueue[0]

	if now.After(nextUpdate.scheduledTime) {
		// Apply the next boundary update, and advance the queue.
		s.tree.Store(newTreeWithBoundaries(nextUpdate.boundaries))
		s.mu.boundaryUpdateQueue = s.mu.boundaryUpdateQueue[1:]
	} else {
		// Reset the tree using the currently installed boundaries.
		resetTree(s.getTree())
	}
}

func (s *SpanStatsCollector) rolloverSample(sampleTime time.Time) {
	stats := s.getStats()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Stash the sample at the ring buffer's current position.
	// The sample is timestamped at the end of the collection period.
	s.mu.r.Value = keyvispb.Sample{
		SampleTime: sampleTime,
		SpanStats:  stats,
	}

	// Advance the ring buffer.
	s.mu.r = s.mu.r.Next()
}
