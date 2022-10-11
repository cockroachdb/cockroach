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
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"sync/atomic"
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
	tree interval.Tree
	mu struct {
		syncutil.Mutex
		stashedBoundaries []*roachpb.Span
	}
}

func New() *SpanStatsCollector {
	collector := &SpanStatsCollector{}
	t, _ := newTreeWithBoundaries(nil)
	collector.tree = t
	return collector
}

// newTreeWithBoundaries returns an error if an installed boundary is
// invalid, i.e. the start key is greater than or equal to the end key,
// or the span is nil.
func newTreeWithBoundaries(spans []*roachpb.Span) (interval.Tree, error) {
	t := interval.NewTree(interval.ExclusiveOverlapper)
	for i, sp := range spans {
		bucket := statsBucket{
			sp:      *sp,
			id:      uintptr(i),
			counter: 0,
		}
		err := t.Insert(&bucket, false /* fast */)
		if err != nil {
			return nil, err
		}
	}

	return t, nil
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
			Requests: bucket.counter,
		})
	}

	return stats
}

// Increment adds 1 to the counter that counts requests for this
// span. If span does not fall within the previously saved boundaries,
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
func (s *SpanStatsCollector) SaveBoundaries(boundaries []*roachpb.Span) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.stashedBoundaries = boundaries
}

// FlushSample returns the collected statistics to the caller.
// It implicitly starts a new collection period by clearing the old
// statistics. A sample period is therefore defined by the interval between
// FlushSample calls.
// TODO(zachlite): this will change with improved fault tolerance mechanisms,
// because the lifecycle of a sample period should be decoupled from
// FlushSample calls.
func (s *SpanStatsCollector) FlushSample() ([]*keyvispb.SpanStats, error) {

	stats := s.getStats()

	// Reset the collector. This marks the beginning of a new sample period.
	s.mu.Lock()
	defer s.mu.Unlock()
	newTree, err := newTreeWithBoundaries(s.mu.stashedBoundaries)
	if err != nil {
		return nil, errors.Wrapf(err, "could not install boundaries")
	}
	s.tree = newTree

	// TODO(zachlite): until the collector can stash samples,
	// the collector will only return one sample at a time.
	return stats, nil
}
