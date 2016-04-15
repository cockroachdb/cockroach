// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
)

// A rangeStats encapsulates access to a range's stats. Range
// statistics are maintained on every range operation using
// stat increments accumulated via MVCCStats structs. Stats are
// efficiently aggregated using the engine.Merge operator.
//
// MVCC stats values should be accessed directly only from the raft
// processing goroutine and should never be individually updated; use
// Update() instead. For access from other goroutines, use GetMVCC().
type rangeStats struct {
	rangeID          roachpb.RangeID
	sync.Mutex       // Protects MVCCStats
	engine.MVCCStats // embedded, cached version of stat values
}

// newRangeStats creates a new instance of rangeStats using the
// provided engine and range. In particular, the values of last update
// nanos and intent count are pulled from the engine and cached in the
// struct for efficient processing (i.e. each new merge does not
// require the values to be read from the engine).
func newRangeStats(rangeID roachpb.RangeID, e engine.Engine) (*rangeStats, error) {
	rs := &rangeStats{rangeID: rangeID}
	if err := engine.MVCCGetRangeStats(context.Background(), e, rangeID, &rs.MVCCStats); err != nil {
		return nil, err
	}
	return rs, nil
}

// Replace *rs with the contents of *other.
func (rs *rangeStats) Replace(other *rangeStats) {
	rs.Lock()
	defer rs.Unlock()

	// Note that in the one place where this method is used we do not
	// expect *other to be accessible by other threads, so the locking
	// here is just a precaution. If we were really concerned about both
	// objects being in use then we would also need to consider deadlocks.
	other.Lock()
	defer other.Unlock()

	rs.MVCCStats = other.MVCCStats
}

// GetMVCC returns a copy of the underlying MVCCStats. Use this for
// thread-safe access from goroutines other than the store raft
// processing goroutine.
func (rs *rangeStats) GetMVCC() engine.MVCCStats {
	rs.Lock()
	defer rs.Unlock()
	return rs.MVCCStats
}

// GetSize returns the range size as the sum of the key and value
// bytes. This includes all non-live keys and all versioned values.
func (rs *rangeStats) GetSize() int64 {
	rs.Lock()
	defer rs.Unlock()
	return rs.KeyBytes + rs.ValBytes
}

// MergeMVCCStats merges the results of an MVCC operation or series of MVCC
// operations into the range's stats. Stats are stored to the underlying engine
// and the rangeStats MVCCStats updated to reflect merged totals.
func (rs *rangeStats) MergeMVCCStats(e engine.Engine, ms engine.MVCCStats) error {
	rs.Lock()
	defer rs.Unlock()
	rs.MVCCStats.Add(ms)
	return engine.MVCCSetRangeStats(context.Background(), e, rs.rangeID, &rs.MVCCStats)
}

// SetStats sets stats wholesale.
func (rs *rangeStats) SetMVCCStats(e engine.Engine, ms engine.MVCCStats) error {
	rs.Lock()
	defer rs.Unlock()
	rs.MVCCStats = ms
	return engine.MVCCSetRangeStats(context.Background(), e, rs.rangeID, &ms)
}

// GetAvgIntentAge returns the average age of outstanding intents,
// based on current wall time specified via nowNanos.
func (rs *rangeStats) GetAvgIntentAge(nowNanos int64) float64 {
	rs.Lock()
	defer rs.Unlock()
	if rs.IntentCount == 0 {
		return 0
	}
	// Make a copy so that rs is not updated.
	cp := rs.MVCCStats
	// Advance age by any elapsed time since last computed.
	cp.AgeTo(nowNanos)
	return float64(cp.IntentAge) / float64(cp.IntentCount)
}

// GetGCBytesAge returns the total age of outstanding gc'able
// bytes, based on current wall time specified via nowNanos.
// nowNanos is ignored if it's a past timestamp as seen by
// rs.LastUpdateNanos.
func (rs *rangeStats) GetGCBytesAge(nowNanos int64) int64 {
	rs.Lock()
	defer rs.Unlock()
	// Make a copy so that rs is not updated.
	cp := rs.MVCCStats
	cp.AgeTo(nowNanos)
	return cp.GCBytesAge
}

// ComputeStatsForRange computes the stats for a given range by
// iterating over all key ranges for the given range that should
// be accounted for in its stats.
func ComputeStatsForRange(d *roachpb.RangeDescriptor, e engine.Engine, nowNanos int64) (engine.MVCCStats, error) {
	iter := e.NewIterator(nil)
	defer iter.Close()

	ms := engine.MVCCStats{}
	for _, r := range makeReplicatedKeyRanges(d) {
		msDelta, err := iter.ComputeStats(r.start, r.end, nowNanos)
		if err != nil {
			return engine.MVCCStats{}, err
		}
		ms.Add(msDelta)
	}
	return ms, nil
}
