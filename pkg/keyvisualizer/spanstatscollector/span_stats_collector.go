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
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanstats/spanstatspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/errors"
)

// tenantStatsBucket implements the interval.Interface interface.
type tenantStatsBucket struct {
	sp      roachpb.Span
	id      uintptr
	counter uint64
}

var _ interval.Interface = &tenantStatsBucket{}

// Range implements the interval.Interface interface.
func (t *tenantStatsBucket) Range() interval.Range {
	return t.sp.AsRange()
}

// ID implements the interval.Interface interface.
func (t *tenantStatsBucket) ID() uintptr {
	return t.id
}

type tenantStatsCollector struct {
	stashedBoundaries []*roachpb.Span
	tree              interval.Tree
}

func newTenantCollector() *tenantStatsCollector {
	return &tenantStatsCollector{
		stashedBoundaries: nil,
		tree:              interval.NewTree(interval.ExclusiveOverlapper),
	}
}

// newTreeWithBoundaries returns an error if an installed boundary is
// invalid, i.e. the start key is greater than or equal to the end key,
// or the span is nil.
func newTreeWithBoundaries(spans []*roachpb.Span) (interval.Tree, error) {
	t := interval.NewTree(interval.ExclusiveOverlapper)
	for i, sp := range spans {
		bucket := tenantStatsBucket{
			sp:      *sp,
			id:      uintptr(i),
			counter: 0,
		}
		err := t.Insert(&bucket, false) // fast = false
		if err != nil {
			return nil, err
		}
	}

	return t, nil
}

func (t *tenantStatsCollector) getStats() []*spanstatspb.SpanStats {

	stats := make([]*spanstatspb.SpanStats, 0, t.tree.Len())
	// TODO: acquire mutex lock?
	it := t.tree.Iterator()
	for {
		i, next := it.Next()
		if next == false {
			break
		}
		bucket := i.(*tenantStatsBucket)
		stats = append(stats, &spanstatspb.SpanStats{
			Span:     &bucket.sp,
			Requests: bucket.counter,
		})
	}

	return stats
}

func (t *tenantStatsCollector) increment(sp roachpb.Span) {
	t.tree.DoMatching(func(i interval.Interface) (done bool) {
		bucket := i.(*tenantStatsBucket)
		bucket.counter++
		return false // want more
	}, sp.AsRange())
}

// SpanStatsCollector maintains span statistics for each tenant.
type SpanStatsCollector struct {
	collectors map[roachpb.TenantID]*tenantStatsCollector
}

// New constructs a new SpanStatsCollector
func New() *SpanStatsCollector {
	return &SpanStatsCollector{
		collectors: map[roachpb.TenantID]*tenantStatsCollector{},
	}
}

// Increment adds 1 to the counter that counts requests for this tenant and
// span. If span does not fall within the previously saved boundaries,
// this is a no-op.
func (s *SpanStatsCollector) Increment(id roachpb.TenantID, span roachpb.Span) error {

	if collector, ok := s.collectors[id]; ok {
		collector.increment(span)
	} else {
		// A collector for this tenant has not been installed.
		// Under the current scheme, a collector for a tenant is installed
		// by the first call to SaveBoundaries.
		return errors.New("tenant not found")
	}

	return nil
}

// SaveBoundaries persists the desired collection boundaries for a given
// tenant. If the tenant doesn't exist,
// a collector is installed for the tenant.
func (s *SpanStatsCollector) SaveBoundaries(id roachpb.TenantID, boundaries []*roachpb.Span) {

	if collector, ok := s.collectors[id]; ok {
		collector.stashedBoundaries = boundaries
	} else {
		newCollector := newTenantCollector()
		newCollector.stashedBoundaries = boundaries
		s.collectors[id] = newCollector
	}
}

// GetSamples returns the tenant's statistics to the caller.
// It implicitly starts a new collection period by clearing the old
// statistics. A sample period is therefore defined by the interval between a
// tenant requesting samples.
// TODO(zachlite): this will change with improved fault tolerance mechanisms,
// because the lifecycle of a sample period should be decoupled from a tenant
// requesting samples.
func (s *SpanStatsCollector) GetSamples(id roachpb.TenantID) ([]spanstatspb.Sample, error) {

	collector, ok := s.collectors[id]
	if !ok {
		// A collector for this tenant has not been installed.
		// Under the current scheme, a collector for a tenant is installed
		// by the first call to SaveBoundaries.
		return nil, errors.New("tenant not found")
	}

	stats := collector.getStats()

	t, err := newTreeWithBoundaries(collector.stashedBoundaries)
	if err != nil {
		panic(fmt.Sprintf("could not build new interval tree: %v", err))
	}

	collector.tree = t

	// TODO(zachlite): until the collector can stash tenant samples,
	// the collector will only return one sample at a time.
	// While this is the case, the server sets the timestamp of the outgoing sample.
	return []spanstatspb.Sample{{
		SampleTime: nil,
		SpanStats:  stats,
	}}, nil
}
