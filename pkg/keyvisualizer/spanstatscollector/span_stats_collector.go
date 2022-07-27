// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanstatscollector

import (
	"container/ring"
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvissettings"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
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

// SpanStatsCollector does stuff
type SpanStatsCollector struct {
	tree     atomic.Value
	settings *cluster.Settings
	mu       struct {
		syncutil.Mutex
		// A queue maintains boundary updates that should be applied in the future.
		boundaryUpdateQueue []boundaryUpdate
		// A ring buffer is used to persist previous samples until they're requested.
		r *ring.Ring
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
	collector.mu.r = ring.New(5) // Keep the 5 most recent samples.
	return collector
}

// Start begins an async task to run the collector.
func (s *SpanStatsCollector) Start(ctx context.Context, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "span-stats-collector",
		func(ctx context.Context) {
			t := timeutil.NewTimer()
			samplePeriod := keyvissettings.SampleInterval.Get(&s.settings.SV)
			for {
				now := timeutil.Now()
				nextTick := now.Truncate(samplePeriod).Add(samplePeriod)
				t.Reset(nextTick.Sub(now))
				select {
				case <-t.C:
					t.Read = true
					s.rolloverSample()
				case <-stopper.ShouldQuiesce():
					return
				case <-ctx.Done():
					return
				}
			}
		})
}

// newTreeWithBoundaries returns an error if an installed boundary is
// invalid, i.e. the start key is greater than or equal to the end key,
// or the span is nil.
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
	stats := make([]keyvispb.SpanStats, 0, s.getTree().Len())

	it := s.getTree().Iterator()
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

// SaveBoundaries stashes the desired collection boundaries.
// They are installed when a sample is rolled over, if the time at rollover is greater than
// scheduledTime.
func (s *SpanStatsCollector) SaveBoundaries(boundaries []roachpb.Span, scheduledTime time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.boundaryUpdateQueue = append(s.mu.boundaryUpdateQueue, boundaryUpdate{
		boundaries:    boundaries,
		scheduledTime: scheduledTime,
	})
}

// GetSamples returns samples that are newer than t.
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

func (s *SpanStatsCollector) rolloverSample() {
	stats := s.getStats()

	s.mu.Lock()
	defer s.mu.Unlock()

	// The sample is timestamped at the end of the collection period.
	t := timeutil.Now()

	// Stash the sample at the ring buffer's current position.
	s.mu.r.Value = keyvispb.Sample{
		SampleTime: t,
		SpanStats:  stats,
	}

	// Advance the ring buffer.
	s.mu.r.Next()

	// If no boundary updates are queued, reset the tree with the current boundaries.
	if len(s.mu.boundaryUpdateQueue) == 0 {
		resetTree(s.getTree())
		return
	}

	nextUpdate := s.mu.boundaryUpdateQueue[0]

	// Apply the next boundary update, and advance the queue.
	if t.After(nextUpdate.scheduledTime) {
		s.tree.Store(newTreeWithBoundaries(nextUpdate.boundaries))
		s.mu.boundaryUpdateQueue = s.mu.boundaryUpdateQueue[1:]
	} else {
		resetTree(s.getTree())
	}
}
