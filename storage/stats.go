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
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
)

// A rangeStats encapsulates access to a range's stats. Range
// statistics are maintained on every range operation using
// stat increments accumulated via MVCCStats structs. Stats are
// efficiently aggregated using the engine.Merge operator.
type rangeStats struct {
	raftID int64
	// Cached from the underlying stats.
	cached engine.MVCCStats
}

// newRangeStats creates a new instance of rangeStats using the
// provided engine and range. In particular, the values of last update
// nanos and intent count are pulled from the engine and cached in the
// struct for efficient processing (i.e. each new merge does not
// require the values to be read from the engine).
func newRangeStats(raftID int64, e engine.Engine) (*rangeStats, error) {
	rs := &rangeStats{raftID: raftID}
	if err := engine.MVCCGetRangeStats(e, raftID, &rs.cached); err != nil {
		return nil, err
	}
	return rs, nil
}

// Get returns the value of the named stat.
func (rs *rangeStats) Get(e engine.Engine, stat proto.Key) (int64, error) {
	return engine.MVCCGetRangeStat(e, rs.raftID, stat)
}

// GetSize returns the range size as the sum of the key and value
// bytes. This includes all non-live keys and all versioned values.
func (rs *rangeStats) GetSize(e engine.Engine) (int64, error) {
	return engine.MVCCGetRangeSize(e, rs.raftID)
}

// MergeMVCCStats merges the results of an MVCC operation or series of
// MVCC operations into the range's stats. The intent age is augmented
// by multiplying the previous intent count by the elapsed nanos since
// the last update to range stats.
func (rs *rangeStats) MergeMVCCStats(e engine.Engine, ms *engine.MVCCStats, nowNanos int64) {
	// Augment the current intent age.
	diffSeconds := nowNanos/1E9 - rs.cached.LastUpdateNanos/1E9
	ms.LastUpdateNanos = nowNanos - rs.cached.LastUpdateNanos
	ms.IntentAge += rs.cached.IntentCount * diffSeconds
	ms.GCBytesAge += (rs.cached.KeyBytes + rs.cached.ValBytes - rs.cached.LiveBytes) * diffSeconds
	ms.MergeStats(e, rs.raftID)
}

// SetStats sets stat counters.
func (rs *rangeStats) SetMVCCStats(e engine.Engine, ms engine.MVCCStats) {
	ms.SetStats(e, rs.raftID)
}

// Update updates the rangeStats' internal values for lastUpdateNanos and
// intentCount. This method should be invoked only after a successful
// commit of merged values to the underlying engine.
func (rs *rangeStats) Update(ms engine.MVCCStats) {
	rs.cached.LiveBytes += ms.LiveBytes
	rs.cached.KeyBytes += ms.KeyBytes
	rs.cached.ValBytes += ms.ValBytes
	rs.cached.IntentBytes += ms.IntentBytes
	rs.cached.LiveCount += ms.LiveCount
	rs.cached.KeyCount += ms.KeyCount
	rs.cached.ValCount += ms.ValCount
	rs.cached.IntentCount += ms.IntentCount
	rs.cached.IntentAge += ms.IntentAge
	rs.cached.GCBytesAge += ms.GCBytesAge
	rs.cached.LastUpdateNanos += ms.LastUpdateNanos
}

// GetAvgIntentAge returns the average age of outstanding intents,
// based on current wall time specified via nowNanos.
func (rs *rangeStats) GetAvgIntentAge(e engine.Engine, nowNanos int64) float64 {
	if rs.cached.IntentCount == 0 {
		return 0
	}
	// Advance age by any elapsed time since last computed.
	elapsedSeconds := nowNanos/1E9 - rs.cached.LastUpdateNanos/1E9
	advancedIntentAge := rs.cached.IntentAge + rs.cached.IntentCount*elapsedSeconds
	return float64(advancedIntentAge) / float64(rs.cached.IntentCount)
}

// GetAvgGCBytesAge returns the average age of outstanding gc'able
// bytes, based on current wall time specified via nowNanos.
func (rs *rangeStats) GetAvgGCBytesAge(e engine.Engine, nowNanos int64) float64 {
	gcBytes := (rs.cached.KeyBytes + rs.cached.ValBytes - rs.cached.LiveBytes)
	if gcBytes == 0 {
		return 0
	}
	// Advance gc bytes age by any elapsed time since last computed.
	elapsedSeconds := nowNanos/1E9 - rs.cached.LastUpdateNanos/1E9
	advancedGCBytesAge := rs.cached.GCBytesAge + gcBytes*elapsedSeconds
	return float64(advancedGCBytesAge) / float64(gcBytes)
}
