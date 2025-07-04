// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import "time"

// TODO: This is an unusued implementation, it probably is only good as an
// idea, rather than a working implementation. See the objective and other
// commentary below.

// Objective: Know when a remote store is not likely to be able to shed any
// load, because its load delta haven't changed over the last t duration (in
// addition to tracking the last time the remote rebalancer ran). When
// this is the case, we can begin shedding load from the remote store to other
// stores. Presumably every other allocator would be doing the same.
//
// For a load update:
// (1) This allocator receives a store load msg for a store sX
// (2) This allocator then updates the store load tracker for sX using the load
// usage.
//
// For a rebalance:
// (1) When considering a rebalance for a remote store sX, this allocator
// checks whether the load delta of sX has decreased over the last t duration.
//
// (2) If the absolute load of sX has not decreased over the last t duration
// AND the remote rebalancer has run in the last t duration, then the
// remote store is not likely to be able to shed any load.
//
// Alternatively, we could also track the amount of load the remote store has
// shed locally. This would allow us to determine if the remote store is not likely
// to be able to shed any load as well.

// storeLoadTracker tracks the reported load of a store over time. It can be
// used to query the store load delta over time of a store in order to
// determine if the remote store is not likely to be able to shed any load.
type storeLoadTracker struct {
	// history stores load measurements with their timestamps
	history []loadRecord
}

// loadRecord represents a single load measurement at a specific time
type loadRecord struct {
	timestamp time.Time
	load      LoadVector
}

// addLoadRecord adds a new load record to the store load tracker
func (s *storeLoadTracker) addLoadRecord(now time.Time, load LoadVector) {
	s.history = append(s.history, loadRecord{
		timestamp: now,
		load:      load,
	})
}

func (s *storeLoadTracker) loadDeltaOverTime(t time.Duration) LoadVector {
	if len(s.history) == 0 {
		return LoadVector{}
	}

	// Find the most recent record and the record from t duration ago.
	now := s.history[len(s.history)-1].timestamp
	cutoff := now.Add(-t)

	// Find the first record that's after the cutoff.
	var startIdx int
	for i := len(s.history) - 1; i >= 0; i-- {
		if s.history[i].timestamp.Before(cutoff) {
			startIdx = i + 1
			break
		}
	}

	// If we don't have enough history, return an empty vector.
	if startIdx >= len(s.history) {
		return LoadVector{}
	}
	last := s.history[len(s.history)-1].load
	last.subtract(s.history[startIdx].load)
	// Calculate the delta between the most recent record and the record from t
	// duration ago.
	return last
}

var _ = &storeLoadTracker{}
var _ = &loadRecord{}
var _ = (&storeLoadTracker{}).addLoadRecord
var _ = (&storeLoadTracker{}).loadDeltaOverTime
