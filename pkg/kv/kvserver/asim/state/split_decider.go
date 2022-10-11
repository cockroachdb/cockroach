// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package state

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/split"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// LoadSplitter provides an abstraction for load based splitting. It records
// load against ranges, suggested ranges to be split and possible split keys
// for ranges.
type LoadSplitter interface {
	// Record records a workload event at the time given, against the range
	// with ID RangeID.
	Record(time.Time, RangeID, workload.LoadEvent) bool
	// SplitKey returns whether split key and true if a valid split key exists
	// given the recorded load, otherwise returning false.
	SplitKey(time.Time, RangeID) (Key, bool)
	// ClearSplitKeys returns a suggested list of ranges that should be split
	// due to load. Calling this function resets the list of suggestions.
	ClearSplitKeys() []RangeID
	// ResetRange resets the collected statistics for a Range with ID RangeID.
	ResetRange(rangeID RangeID)
}

// SplitDecider implements the LoadSplitter interface.
type SplitDecider struct {
	deciders     map[RangeID]*split.Decider
	suggestions  []RangeID
	qpsThreshold func() float64
	qpsRetention func() time.Duration
	seed         int64
}

// NewSplitDecider returns a new SplitDecider.
func NewSplitDecider(
	seed int64, qpsThresholdFn func() float64, qpsRetentionFn func() time.Duration,
) *SplitDecider {
	return &SplitDecider{
		deciders:     make(map[RangeID]*split.Decider),
		suggestions:  []RangeID{},
		seed:         seed,
		qpsThreshold: qpsThresholdFn,
		qpsRetention: qpsRetentionFn,
	}
}

func (s *SplitDecider) newDecider() *split.Decider {
	rand := rand.New(rand.NewSource(s.seed))

	intN := func(n int) int {
		return rand.Intn(n)
	}

	decider := &split.Decider{}
	split.Init(decider, intN, s.qpsThreshold, s.qpsRetention, &split.LoadSplitterMetrics{
		PopularKeyCount: metric.NewCounter(metric.Metadata{}),
		NoSplitKeyCount: metric.NewCounter(metric.Metadata{}),
	})
	return decider
}

// Record records a workload event at the time given, against the range
// with ID RangeID.
func (s *SplitDecider) Record(tick time.Time, rangeID RangeID, le workload.LoadEvent) bool {
	decider := s.deciders[rangeID]

	if decider == nil {
		decider = s.newDecider()
		s.deciders[rangeID] = decider
	}

	qps := LoadEventQPS(le)
	shouldSplit := decider.Record(context.Background(), tick, int(qps), func() roachpb.Span {
		return roachpb.Span{Key: Key(le.Key).ToRKey().AsRawKey()}
	})

	if shouldSplit {
		s.suggestions = append(s.suggestions, rangeID)
	}

	return shouldSplit
}

// SplitKey returns whether split key and true if a valid split key exists
// given the recorded load, otherwise returning false.
func (s *SplitDecider) SplitKey(tick time.Time, rangeID RangeID) (Key, bool) {
	decider := s.deciders[rangeID]
	if decider == nil || !decider.IsFinderReady(tick) {
		return InvalidKey, false
	}

	key := decider.MaybeSplitKey(context.Background(), tick)
	if key == nil {
		return InvalidKey, false
	}

	return ToKey(key), true
}

// ClearSplitKeys returns a suggested list of ranges that should be split due
// to load. Calling this function resets the list of suggestions.
func (s *SplitDecider) ClearSplitKeys() []RangeID {
	suggestions := make([]RangeID, len(s.suggestions))
	copy(suggestions, s.suggestions)
	s.suggestions = []RangeID{}
	return suggestions
}

// ResetRange resets the collected statistics for a Range with ID RangeID.
func (s *SplitDecider) ResetRange(rangeID RangeID) {
	s.deciders[rangeID] = s.newDecider()
}
