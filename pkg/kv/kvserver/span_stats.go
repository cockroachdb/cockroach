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
	"context"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func (s *Store) GetSpanStats(
	ctx context.Context,
	start hlc.Timestamp,
	end hlc.Timestamp,
) error { // XXX: Fill me in, find the right return type
	tenantID, _ := roachpb.TenantFromContext(ctx) // XXX: unused return
	_ = tenantID

	copy := s.spanStatsHistogram.tree.Clone()
	_ = copy
	// XXX: construct the return type with the copy.
	return nil
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
	tree interval.Tree // tree of spans -> counters
}

func newSpanStatsHistogram() *spanStatsHistogram {
	return &spanStatsHistogram{
		tree: interval.NewTree(interval.ExclusiveOverlapper),
	}
	// XXX: needs to be initialized with a set of boundaries
	// XXX: want to be able to reset/reconfigure boundaries
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
	//TODO implement me
	panic("implement me")
}

var _ interval.Interface = &spanStatsHistogramBucket{}

func (s *spanStatsHistogram) increment(sp roachpb.Span) {
	s.tree.DoMatching(func(i interval.Interface) (done bool) {
		bucket := i.(*spanStatsHistogramBucket)
		bucket.counter += 1
		return false // want more
	}, sp.AsRange())
}
