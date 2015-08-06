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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"sync"

	"github.com/cockroachdb/cockroach/proto"
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
	rangeID          proto.RangeID
	sync.Mutex       // Protects MVCCStats
	engine.MVCCStats // embedded, cached version of stat values
}

// newRangeStats creates a new instance of rangeStats using the
// provided engine and range. In particular, the values of last update
// nanos and intent count are pulled from the engine and cached in the
// struct for efficient processing (i.e. each new merge does not
// require the values to be read from the engine).
func newRangeStats(rangeID proto.RangeID, e engine.Engine) (*rangeStats, error) {
	rs := &rangeStats{rangeID: rangeID}
	if err := engine.MVCCGetRangeStats(e, rangeID, &rs.MVCCStats); err != nil {
		return nil, err
	}
	return rs, nil
}

// GetMVCC returns a copy of the underlying MVCCStats. Use this for
// thread-safe access from goroutines other than the store multiraft
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

// MergeMVCCStats merges the results of an MVCC operation or series of
// MVCC operations into the range's stats. The intent age is augmented
// by multiplying the previous intent count by the elapsed nanos since
// the last update to range stats. Stats are stored to the underlying
// engine and the rangeStats MVCCStats updated to reflect merged totals.
func (rs *rangeStats) MergeMVCCStats(e engine.Engine, ms *engine.MVCCStats, nowNanos int64) error {
	rs.Lock()
	defer rs.Unlock()
	// Augment the current intent age.
	diffSeconds := nowNanos/1E9 - rs.LastUpdateNanos/1E9
	ms.LastUpdateNanos = nowNanos
	ms.IntentAge += rs.IntentCount * diffSeconds
	ms.GCBytesAge += engine.MVCCComputeGCBytesAge(rs.KeyBytes+rs.ValBytes-rs.LiveBytes, diffSeconds)
	rs.MVCCStats.Add(ms)
	return engine.MVCCSetRangeStats(e, rs.rangeID, &rs.MVCCStats)
}

// SetStats sets stats wholesale.
func (rs *rangeStats) SetMVCCStats(e engine.Engine, ms engine.MVCCStats) error {
	rs.Lock()
	defer rs.Unlock()
	rs.MVCCStats = ms
	return engine.MVCCSetRangeStats(e, rs.rangeID, &ms)
}

// GetAvgIntentAge returns the average age of outstanding intents,
// based on current wall time specified via nowNanos.
func (rs *rangeStats) GetAvgIntentAge(nowNanos int64) float64 {
	rs.Lock()
	defer rs.Unlock()
	if rs.IntentCount == 0 {
		return 0
	}
	// Advance age by any elapsed time since last computed.
	elapsedSeconds := nowNanos/1E9 - rs.LastUpdateNanos/1E9
	advancedIntentAge := rs.IntentAge + rs.IntentCount*elapsedSeconds
	return float64(advancedIntentAge) / float64(rs.IntentCount)
}

// GetGCBytesAge returns the total age of outstanding gc'able
// bytes, based on current wall time specified via nowNanos.
func (rs *rangeStats) GetGCBytesAge(nowNanos int64) int64 {
	rs.Lock()
	defer rs.Unlock()
	gcBytes := (rs.KeyBytes + rs.ValBytes - rs.LiveBytes)
	if gcBytes == 0 {
		return 0
	}
	// Advance gc bytes age by any elapsed time since last computed.
	elapsedSeconds := nowNanos/1E9 - rs.LastUpdateNanos/1E9
	return rs.GCBytesAge + engine.MVCCComputeGCBytesAge(gcBytes, elapsedSeconds)
}
