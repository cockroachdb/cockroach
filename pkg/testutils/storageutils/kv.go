// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	var mvccValue storage.MVCCValue
	if value != "" {
		mvccValue = StringValue(value)
	}
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
	return storage.MVCCRangeKey{
		StartKey:  roachpb.Key(start),
		EndKey:    roachpb.Key(end),
		Timestamp: WallTS(ts),
	}
}

// RangeKV creates an MVCCRangeKeyValue for the given string keys, value, and
// timestamp (in walltime seconds).
func RangeKV(start, end string, ts int, value storage.MVCCValue) storage.MVCCRangeKeyValue {
	valueBytes, err := storage.EncodeMVCCValue(value)
	if err != nil {
		panic(err)
	}
	if valueBytes == nil {
		valueBytes = []byte{}
	}
	return storage.MVCCRangeKeyValue{
		RangeKey: RangeKey(start, end, ts),
		Value:    valueBytes,
	}
}

// WallTS creates a timestamp for the given wall time (in seconds).
func WallTS(ts int) hlc.Timestamp {
	return hlc.Timestamp{WallTime: int64(ts)}
}

// StringValue creates an MVCCValue for a string
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
