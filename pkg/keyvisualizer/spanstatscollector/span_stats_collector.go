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

	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvissettings"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	counter uint64
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

type SpanStatsCollector struct {
	tree     interval.Tree
	settings *cluster.Settings
	mu       struct {
		syncutil.Mutex
		nextBoundaries         []*roachpb.Span
		nextBoundaryUpdateTime *hlc.Timestamp
		r                      *ring.Ring
	}
}

func New(settings *cluster.Settings) *SpanStatsCollector {
	collector := &SpanStatsCollector{}
	collector.tree = newTreeWithBoundaries(nil)
	collector.settings = settings
	collector.mu.r = ring.New(5) // Keep the 5 most recent samples.
	return collector
}

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
					s.rolloverSample(&hlc.Timestamp{WallTime: now.UnixNano()})
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
func newTreeWithBoundaries(spans []*roachpb.Span) interval.Tree {
	t := interval.NewTree(interval.ExclusiveOverlapper)
	for i, sp := range spans {
		bucket := statsBucket{
			sp:      *sp,
			id:      uintptr(i),
			counter: 0,
		}
		err := t.Insert(&bucket, false /* fast */)
		// panic if there's an error
		if err != nil {
			panic(errors.AssertionFailedf(
				"insert into interval tree failed with error %v", err))
		}
	}

	return t
}

func resetTree(tree interval.Tree) {
	it := tree.Iterator()
	for {
		i, next := it.Next()
		if next == false {
			break
		}
		bucket := i.(*statsBucket)
		bucket.counter = 0
	}
}

func (s *SpanStatsCollector) getStats() []*keyvispb.SpanStats {
	stats := make([]*keyvispb.SpanStats, 0, s.tree.Len())

	it := s.tree.Iterator()
	for {
		i, next := it.Next()
		if next == false {
			break
		}
		bucket := i.(*statsBucket)
		stats = append(stats, &keyvispb.SpanStats{
			Span:     &bucket.sp,
			Requests: atomic.LoadUint64(&bucket.counter),
		})
	}
	return stats
}

// Increment adds 1 to the counter that counts requests for this
// span. If the span does not fall within the previously saved boundaries,
// this is a no-op. If boundaries have not yet been installed,
// this function returns false.
func (s *SpanStatsCollector) Increment(sp roachpb.Span) {
	s.tree.DoMatching(func(i interval.Interface) (done bool) {
		bucket := i.(*statsBucket)
		atomic.AddUint64(&bucket.counter, 1)
		return false // want more
	}, sp.AsRange())
}

// SaveBoundaries persists the desired collection boundaries.
// They will installed after the next call to FlushSample.
func (s *SpanStatsCollector) SaveBoundaries(
	boundaries []*roachpb.Span, scheduledTime *hlc.Timestamp,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.nextBoundaries = boundaries
	s.mu.nextBoundaryUpdateTime = scheduledTime
}

func (s *SpanStatsCollector) GetSamples(t *hlc.Timestamp) []*keyvispb.Sample {
	var res []*keyvispb.Sample

	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.r.Do(func(s interface{}) {
		if s == nil {
			return
		}
		sample := s.(*keyvispb.Sample)
		if t.LessEq(*sample.SampleTime) {
			res = append(res, sample)
		}
	})

	return res
}

func (s *SpanStatsCollector) rolloverSample(timestamp *hlc.Timestamp) {
	stats := s.getStats()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Stash the sample at the ring buffer's current position.
	s.mu.r.Value = &keyvispb.Sample{
		SampleTime: timestamp,
		SpanStats:  stats,
	}

	// Advance the ring buffer.
	s.mu.r.Next()

	// Reset the tree.
	if s.mu.nextBoundaries != nil && s.mu.nextBoundaryUpdateTime.Less(*timestamp) {
		s.tree = newTreeWithBoundaries(s.mu.nextBoundaries)
		s.mu.nextBoundaries = nil
	} else {
		resetTree(s.tree)
	}
}
