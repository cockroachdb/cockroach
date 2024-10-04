// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
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

type loadSplitConfig struct {
	randSource split.RandSource
	settings   *config.SimulationSettings
}

// NewLoadBasedSplitter returns a new LoadBasedSplitter that may be used to
// find the midpoint based on recorded load.
func (lsc loadSplitConfig) NewLoadBasedSplitter(
	startTime time.Time, _ split.SplitObjective,
) split.LoadBasedSplitter {
	return split.NewUnweightedFinder(startTime, lsc.randSource)
}

// StatRetention returns the duration that recorded load is to be retained.
func (lsc loadSplitConfig) StatRetention() time.Duration {
	return lsc.settings.SplitStatRetention
}

// StatThreshold returns the threshold for load above which the range
// should be considered split.
func (lsc loadSplitConfig) StatThreshold(_ split.SplitObjective) float64 {
	return lsc.settings.SplitQPSThreshold
}

// SplitDecider implements the LoadSplitter interface.
type SplitDecider struct {
	deciders    map[RangeID]*split.Decider
	suggestions []RangeID
	splitConfig split.LoadSplitConfig
}

// NewSplitDecider returns a new SplitDecider.
func NewSplitDecider(settings *config.SimulationSettings) *SplitDecider {
	return &SplitDecider{
		deciders:    make(map[RangeID]*split.Decider),
		suggestions: []RangeID{},
		splitConfig: loadSplitConfig{
			randSource: rand.New(rand.NewSource(settings.Seed)),
			settings:   settings,
		},
	}
}

func (s *SplitDecider) newDecider() *split.Decider {
	decider := &split.Decider{}
	split.Init(decider, s.splitConfig, &split.LoadSplitterMetrics{
		PopularKeyCount: metric.NewCounter(metric.Metadata{}),
		NoSplitKeyCount: metric.NewCounter(metric.Metadata{}),
	}, split.SplitQPS)
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
	shouldSplit := decider.Record(
		context.Background(),
		tick,
		func(_ split.SplitObjective) int { return int(qps) },
		func() roachpb.Span {
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
	if decider == nil {
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
