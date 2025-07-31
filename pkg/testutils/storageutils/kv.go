// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageutils

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// KVs is a slice of either MVCCKeyValue or MVCCRangeKeyValue.
type KVs []interface{}

// MVCCKeyValues converts the slice to []MVCCKeyValue.
func (kvs KVs) MVCCKeyValues() []storage.MVCCKeyValue {
	mvccKVs := []storage.MVCCKeyValue{}
	for _, kv := range kvs {
		mvccKVs = append(mvccKVs, kv.(storage.MVCCKeyValue))
	}
	return mvccKVs
}

// PointKey creates an MVCCKey for the given string key and timestamp (walltime
// seconds).
func PointKey(key string, ts int) storage.MVCCKey {
	return storage.MVCCKey{Key: roachpb.Key(key), Timestamp: WallTS(ts)}
}

// PointKV creates an MVCCKeyValue for the given string key/value and timestamp
// (walltime seconds). An empty string is a tombstone.
func PointKV(key string, ts int, value string) storage.MVCCKeyValue {
	return PointKVWithLocalTS(key, ts, 0, value)
}

// PointKVWithLocalTS creates an MVCCKeyValue for the given string key/value,
// timestamp, and local timestamp (walltime seconds). An empty string is a
// tombstone.
func PointKVWithLocalTS(key string, ts int, localTS int, value string) storage.MVCCKeyValue {
	var mvccValue storage.MVCCValue
	if value != "" {
		mvccValue = StringValue(value)
	}
	mvccValue = WithLocalTS(mvccValue, localTS)
	v, err := storage.EncodeMVCCValue(mvccValue)
	if err != nil {
		panic(err)
	}
	return storage.MVCCKeyValue{
		Key:   PointKey(key, ts),
		Value: v,
	}
}

// PointKVWithImportEpoch creates an MVCCKeyValue for the given string key/value,
// timestamp, and ImportEpoch.
func PointKVWithImportEpoch(
	key string, ts int, importEpoch uint32, value string,
) storage.MVCCKeyValue {
	var mvccValue storage.MVCCValue
	if value != "" {
		mvccValue = StringValue(value)
	}
	mvccValue.ImportEpoch = importEpoch
	v, err := storage.EncodeMVCCValue(mvccValue)
	if err != nil {
		panic(err)
	}
	return storage.MVCCKeyValue{
		Key:   PointKey(key, ts),
		Value: v,
	}
}

// RangeKey creates an MVCCRangeKey for the given string key and timestamp
// (in walltime seconds).
func RangeKey(start, end string, ts int) storage.MVCCRangeKey {
	return RangeKeyWithTS(start, end, WallTS(ts))
}

// RangeKeyWithTS creates an MVCCRangeKey for the given string key and timestamp.
func RangeKeyWithTS(start, end string, ts hlc.Timestamp) storage.MVCCRangeKey {
	return storage.MVCCRangeKey{
		StartKey:               roachpb.Key(start),
		EndKey:                 roachpb.Key(end),
		Timestamp:              ts,
		EncodedTimestampSuffix: storage.EncodeMVCCTimestampSuffix(ts),
	}
}

// RangeKV creates an MVCCRangeKeyValue for the given string keys, value, and
// timestamp (in walltime seconds).
func RangeKV(start, end string, ts int, value string) storage.MVCCRangeKeyValue {
	return RangeKVWithLocalTS(start, end, ts, 0, value)
}

// RangeKVWithLocalTS creates an MVCCRangeKeyValue for the given string keys,
// value, and timestamp (in walltime seconds).
func RangeKVWithLocalTS(
	start, end string, ts, localTS int, value string,
) storage.MVCCRangeKeyValue {
	var mvccValue storage.MVCCValue
	if value != "" {
		mvccValue = StringValue(value)
	}
	mvccValue = WithLocalTS(mvccValue, localTS)
	v, err := storage.EncodeMVCCValue(mvccValue)

	if err != nil {
		panic(err)
	}
	return storage.MVCCRangeKeyValue{
		RangeKey: RangeKey(start, end, ts),
		Value:    v,
	}
}

// RangeKVWithTS creates an MVCCRangeKeyValue for the given string keys, value, and
// timestamp.
func RangeKVWithTS(
	start, end string, ts hlc.Timestamp, value storage.MVCCValue,
) storage.MVCCRangeKeyValue {
	valueBytes, err := storage.EncodeMVCCValue(value)
	if err != nil {
		panic(err)
	}
	return storage.MVCCRangeKeyValue{
		RangeKey: RangeKeyWithTS(start, end, ts),
		Value:    valueBytes,
	}
}

// WallTS creates a timestamp for the given wall time (in seconds).
func WallTS(ts int) hlc.Timestamp {
	return hlc.Timestamp{WallTime: int64(ts)}
}

// StringValue creates an MVCCValue for a string.
func StringValue(s string) storage.MVCCValue {
	return storage.MVCCValue{Value: roachpb.MakeValueFromString(s)}
}

// StringValueRaw creates an encoded MVCCValue for a string.
func StringValueRaw(s string) []byte {
	b, err := storage.EncodeMVCCValue(StringValue(s))
	if err != nil {
		panic(err)
	}
	return b
}

// WithLocalTS attaches a local timestamp (in walltime seconds) to an MVCCValue.
func WithLocalTS(v storage.MVCCValue, ts int) storage.MVCCValue {
	v.MVCCValueHeader.LocalTimestamp = hlc.ClockTimestamp{WallTime: int64(ts)}
	return v
}
