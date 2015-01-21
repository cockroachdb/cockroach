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
	// The following values are cached from the underlying stats.
	elapsedNanos int64 // nanos since the epoch
	intentCount  int64
}

// newRangeStats creates a new instance of rangeStats using the
// provided engine and range. In particular, the values of elapsed
// nanos and intent count are pulled from the engine and cached in
// the struct for efficient processing (i.e. each new merge does
// not require the values to be read from the engine).
func newRangeStats(raftID int64, e engine.Engine) (*rangeStats, error) {
	rs := &rangeStats{raftID: raftID}
	var err error
	if rs.elapsedNanos, err = engine.MVCCGetRangeStat(e, raftID, engine.StatElapsedNanos); err != nil {
		return nil, err
	}
	if rs.intentCount, err = engine.MVCCGetRangeStat(e, raftID, engine.StatIntentCount); err != nil {
		return nil, err
	}
	return rs, nil
}

// Get returns the value of the named stat.
func (rs *rangeStats) Get(e engine.Engine, stat proto.Key) (int64, error) {
	return engine.MVCCGetRangeStat(e, rs.raftID, stat)
}

// Clear clears stats for the specified range.
func (rs *rangeStats) Clear(e engine.Engine) error {
	statStartKey := engine.RangeStatKey(rs.raftID, proto.Key{})
	statEndKey := engine.RangeStatKey(rs.raftID+1, proto.Key{})
	_, err := engine.ClearRange(e, engine.MVCCEncodeKey(statStartKey), engine.MVCCEncodeKey(statEndKey))
	return err
}

// GetSize returns the range size as the sum of the key and value
// bytes. This includes all non-live keys and all versioned values.
func (rs *rangeStats) GetSize(e engine.Engine) (int64, error) {
	return engine.MVCCGetRangeSize(e, rs.raftID)
}

// MergeMVCCStats merges the results of an MVCC operation or series of
// MVCC operations into the range's stats. The intent age is augmented
// by multiplying the previous intent count by the elapsed nanos since
// the last merge.
func (rs *rangeStats) MergeMVCCStats(e engine.Engine, ms *engine.MVCCStats, nowNanos int64) {
	// Augment the current intent age.
	ms.ElapsedNanos = nowNanos - rs.elapsedNanos
	if ms.ElapsedNanos != 0 {
		ms.IntentAge += rs.intentCount * ms.ElapsedNanos
	}
	ms.MergeStats(e, rs.raftID)
}

// SetStats sets stat counters.
func (rs *rangeStats) SetMVCCStats(e engine.Engine, ms *engine.MVCCStats) {
	ms.SetStats(e, rs.raftID)
}

// Update updates the rangeStats' internal values for elapsedNanos and
// intentCount. This method should be invoked only after a successful
// commit of merged values to the underlying engine.
func (rs *rangeStats) Update(ms *engine.MVCCStats) {
	rs.elapsedNanos += ms.ElapsedNanos
	rs.intentCount += ms.IntentCount
}

// GetAvgIntentAge returns the average age of outstanding intents,
// based on current wall time specified via nowNanos.
func (rs *rangeStats) GetAvgIntentAge(e engine.Engine, nowNanos int64) (float64, error) {
	if rs.intentCount == 0 {
		return 0, nil
	}
	intentAge, err := rs.Get(e, engine.StatIntentAge)
	if err == nil {
		return 0, err
	}
	// Advance age by any elapsed time since last computed.
	elapsedNanos := nowNanos - rs.elapsedNanos
	intentAge += rs.intentCount * elapsedNanos
	return float64(intentAge) / float64(rs.intentCount), nil
}
