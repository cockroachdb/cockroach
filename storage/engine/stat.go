// Copyright 2014 The Cockroach Authors.
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

package engine

import (
	"github.com/cockroachdb/cockroach/proto"
	gogoproto "github.com/gogo/protobuf/proto"
)

// Constants for stat key construction.
var (
	// StatLiveBytes counts how many bytes are "live", including bytes
	// from both keys and values. Live rows include only non-deleted
	// keys and only the most recent value.
	StatLiveBytes = proto.Key("live-bytes")
	// StatKeyBytes counts how many bytes are used to store all keys,
	// including bytes from deleted keys. Key bytes are re-counted for
	// each versioned value.
	StatKeyBytes = proto.Key("key-bytes")
	// StatValBytes counts how many bytes are used to store all values,
	// including all historical versions and deleted tombstones.
	StatValBytes = proto.Key("val-bytes")
	// StatIntentBytes counts how many bytes are used to store values
	// which are unresolved intents. Includes bytes used for both intent
	// keys and values.
	StatIntentBytes = proto.Key("intent-bytes")
	// StatLiveCount counts how many keys are "live". This includes only
	// non-deleted keys.
	StatLiveCount = proto.Key("live-count")
	// StatKeyCount counts the total number of keys, including both live
	// and deleted keys.
	StatKeyCount = proto.Key("key-count")
	// StatValCount counts the total number of values, including all
	// historical versions and deleted tombstones.
	StatValCount = proto.Key("val-count")
	// StatIntentCount counts the number of unresolved intents.
	StatIntentCount = proto.Key("intent-count")
)

// RangeStatKey returns the key for accessing the named stat
// for the specified Raft ID.
func RangeStatKey(raftID int64, stat proto.Key) proto.Key {
	return MakeRangeIDKey(raftID, KeyLocalRangeStatSuffix, stat)
}

// StoreStatKey returns the key for accessing the named stat
// for the specified store ID.
func StoreStatKey(storeID int32, stat proto.Key) proto.Key {
	return MakeStoreKey(KeyLocalStoreStatSuffix, stat)
}

// GetRangeStat fetches the specified stat from the provided engine.
// If the stat could not be found, returns 0. An error is returned
// on stat decode error.
func GetRangeStat(engine Engine, raftID int64, stat proto.Key) (int64, error) {
	val, err := MVCCGet(engine, RangeStatKey(raftID, stat), proto.ZeroTimestamp, nil)
	if err != nil || val == nil {
		return 0, err
	}
	return val.GetInteger(), nil
}

// MergeStat flushes the specified stat to merge counters via the
// provided mvcc instance for both the affected range and store. Only
// updates range or store stats if the corresponding ID is non-zero.
func MergeStat(engine Engine, raftID int64, storeID int32, stat proto.Key, statVal int64) error {
	if statVal == 0 {
		return nil
	}
	value := proto.Value{Integer: gogoproto.Int64(statVal)}
	if raftID != 0 {
		if err := MVCCMerge(engine, nil, RangeStatKey(raftID, stat), value); err != nil {
			return err
		}
	}
	if storeID != 0 {
		if err := MVCCMerge(engine, nil, StoreStatKey(storeID, stat), value); err != nil {
			return err
		}
	}
	return nil
}

// SetStat writes the specified stat to counters via the provided mvcc
// instance for both the affected range and store. Only updates range
// or store stats if the corresponding ID is non-zero.
func SetStat(engine Engine, raftID int64, storeID int32, stat proto.Key, statVal int64) error {
	value := proto.Value{Integer: gogoproto.Int64(statVal)}
	if raftID != 0 {
		if err := MVCCPut(engine, nil, RangeStatKey(raftID, stat), proto.ZeroTimestamp, value, nil); err != nil {
			return err
		}
	}
	if storeID != 0 {
		if err := MVCCPut(engine, nil, StoreStatKey(storeID, stat), proto.ZeroTimestamp, value, nil); err != nil {
			return err
		}
	}
	return nil
}

// GetRangeSize returns the range size as the sum of the key and value
// bytes. This includes all non-live keys and all versioned values.
func GetRangeSize(engine Engine, raftID int64) (int64, error) {
	keyBytes, err := GetRangeStat(engine, raftID, StatKeyBytes)
	if err != nil {
		return 0, err
	}
	valBytes, err := GetRangeStat(engine, raftID, StatValBytes)
	if err != nil {
		return 0, err
	}
	return keyBytes + valBytes, nil
}

// ClearRangeStats clears stats for the specified range.
func ClearRangeStats(engine Engine, raftID int64) error {
	statStartKey := RangeStatKey(raftID, proto.Key{})
	statEndKey := RangeStatKey(raftID+1, proto.Key{})
	_, err := ClearRange(engine, MVCCEncodeKey(statStartKey), MVCCEncodeKey(statEndKey))
	return err
}
