// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func (s *Store) SetBucketBoundaries() error {

	hist := newSpanStatsHistogram()

	var err error
	s.VisitReplicas(func(replica *Replica) (wantMore bool) {
		desc := replica.Desc()

		err = hist.addBucket(desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey())

		if err != nil {
			return false
		}

		return true
	})

	// TODO: acquire lock
	if err != nil {
		return err
	}

	s.spanStatsHistogram = hist
	return nil
}


// TODO: use start and end times. Currently assuming the request only cares about the current active histogram.
func (s *Store) VisitSpanStatsBuckets(visitor func(span *roachpb.Span, batchRequests uint64)) {

	// TODO: acquire mutex lock?
	it := s.spanStatsHistogram.tree.Iterator()

	for {
		i, next := it.Next()
		if next == false {
			break
		}
		bucket := i.(*spanStatsHistogramBucket)
		visitor(&bucket.sp, bucket.counter)
	}
}

type tenantShardedSpanStats struct {
	// XXX: scope this to specific time windows, and roll things over etc.
	histogram map[roachpb.TenantID]spanStatsHistogram
}

type spanStatsHistogram struct {
	syncutil.Mutex // XXX: actually use this
	// XXX: span oriented data structure, queryable by a span, values are counters.
	// Scan[a,d) --> a list of counters of increment
	// GetAllCountersBetween(a, d) -> inc each one.
	tree         interval.Tree // tree of spans -> counters,
	lastBucketId uint64
}

func (s *spanStatsHistogram) addBucket(startKey, endKey roachpb.Key) error {
	s.lastBucketId++

	bucket := spanStatsHistogramBucket{
		sp:      roachpb.Span{Key: startKey, EndKey: endKey},
		id:      uintptr(s.lastBucketId),
		counter: 0,
	}

	return s.tree.Insert(&bucket, false)
}

func newSpanStatsHistogram() *spanStatsHistogram {
	return &spanStatsHistogram{
		tree: interval.NewTree(interval.ExclusiveOverlapper),
	}
}

type spanStatsHistogramBucket struct {
	sp      roachpb.Span
	id      uintptr
	counter uint64
}

func (s *spanStatsHistogramBucket) Range() interval.Range {
	return s.sp.AsRange()
}

func (s *spanStatsHistogramBucket) ID() uintptr {
	return s.id
}

var _ interval.Interface = &spanStatsHistogramBucket{}

func (s *spanStatsHistogram) increment(sp roachpb.Span) {
	s.tree.DoMatching(func(i interval.Interface) (done bool) {
		bucket := i.(*spanStatsHistogramBucket)
		bucket.counter += 1
		return false // want more
	}, sp.AsRange())
}
