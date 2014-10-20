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
	"fmt"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// Constants for stat key construction.
var (
	// StatLiveBytes counts how many bytes are "live", including bytes
	// from both keys and values. Live rows include only non-deleted
	// keys and only the most recent value.
	StatLiveBytes = Key("live-bytes")
	// StatKeyBytes counts how many bytes are used to store all keys,
	// including bytes from deleted keys. Key bytes are re-counted for
	// each versioned value.
	StatKeyBytes = Key("key-bytes")
	// StatValBytes counts how many bytes are used to store all values,
	// including all historical versions and deleted tombstones.
	StatValBytes = Key("val-bytes")
	// StatIntentBytes counts how many bytes are used to store values
	// which are unresolved intents. Includes bytes used for both intent
	// keys and values.
	StatIntentBytes = Key("intent-bytes")
	// StatLiveCount counts how many keys are "live". This includes only
	// non-deleted keys.
	StatLiveCount = Key("live-count")
	// StatKeyCount counts the total number of keys, including both live
	// and deleted keys.
	StatKeyCount = Key("key-count")
	// StatValCount counts the total number of values, including all
	// historical versions and deleted tombstones.
	StatValCount = Key("val-count")
	// StatIntentCount counts the number of unresolved intents.
	StatIntentCount = Key("intent-count")
)

// encodeStatValue constructs a proto.Value using the supplied stat
// increment and then encodes that into a byte slice. Encoding errors
// cause panics (as they should never happen). Returns false if stat
// is equal to 0 to avoid unnecessary merge.
func encodeStatValue(stat int64) (ok bool, enc []byte) {
	if stat == 0 {
		return false, nil
	}
	data, err := gogoproto.Marshal(&proto.Value{Integer: gogoproto.Int64(stat)})
	if err != nil {
		panic(fmt.Sprintf("could not marshal proto.Value: %s", err))
	}
	return true, data
}

// MakeRangeStatKey returns the key for accessing the named stat
// for the specified range ID.
func MakeRangeStatKey(rangeID int64, stat Key) Key {
	encRangeID := encoding.EncodeInt(nil, rangeID)
	return MakeKey(KeyLocalRangeStatPrefix, encRangeID, stat)
}

// MakeStoreStatKey returns the key for accessing the named stat
// for the specified store ID.
func MakeStoreStatKey(storeID int32, stat Key) Key {
	encStoreID := encoding.EncodeInt(nil, int64(storeID))
	return MakeKey(KeyLocalStoreStatPrefix, encStoreID, stat)
}

// GetRangeStat fetches the specified stat from the provided engine.
// If the stat could not be found, returns 0. An error is returned
// on stat decode error.
func GetRangeStat(engine Engine, rangeID int64, stat Key) (int64, error) {
	val := &proto.Value{}
	ok, err := GetProto(engine, MakeRangeStatKey(rangeID, stat).Encode(nil), val)
	if err != nil || !ok {
		return 0, err
	}
	return val.GetInteger(), nil
}

// MergeStat flushes the specified stat to merge counters via the
// provided batch for both the affected range and store. Only
// updates range or store stats if the corresponding ID is non-zero.
func MergeStat(batch *Batch, rangeID int64, storeID int32, stat Key, statVal int64) {
	if ok, encStat := encodeStatValue(statVal); ok {
		if rangeID != 0 {
			batch.Merge(MakeRangeStatKey(rangeID, stat).Encode(nil), encStat)
		}
		if storeID != 0 {
			batch.Merge(MakeStoreStatKey(storeID, stat).Encode(nil), encStat)
		}
	}
}

// SetStat writes the specified stat to counters via the provided
// batch for both the affected range and store. Only updates range or
// store stats if the corresponding ID is non-zero.
func SetStat(batch *Batch, rangeID int64, storeID int32, stat Key, statVal int64) {
	if ok, encStat := encodeStatValue(statVal); ok {
		if rangeID != 0 {
			batch.Put(MakeRangeStatKey(rangeID, stat).Encode(nil), encStat)
		}
		if storeID != 0 {
			batch.Put(MakeStoreStatKey(storeID, stat).Encode(nil), encStat)
		}
	}
}

// ClearRangeStats clears stats for the specified range.
func ClearRangeStats(engine Engine, rangeID int64) error {
	statStartKey := MakeKey(KeyLocalRangeStatPrefix, encoding.EncodeInt(nil, rangeID))
	_, err := ClearRange(engine, statStartKey.Encode(nil), statStartKey.PrefixEnd().Encode(nil))
	return err
}
