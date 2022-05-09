// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sstutil

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// KV is a simplified representation of an MVCC key/value pair.
type KV struct {
	KeyString     string
	WallTimestamp int64  // 0 for inline
	ValueString   string // "" for nil (tombstone)
}

// Key returns the roachpb.Key representation of the key.
func (kv KV) Key() roachpb.Key {
	return roachpb.Key(kv.KeyString)
}

// Timestamp returns the hlc.Timestamp representation of the timestamp.
func (kv KV) Timestamp() hlc.Timestamp {
	return hlc.Timestamp{WallTime: kv.WallTimestamp}
}

// MVCCKey returns the storage.MVCCKey representation of the key and timestamp.
func (kv KV) MVCCKey() storage.MVCCKey {
	return storage.MVCCKey{
		Key:       kv.Key(),
		Timestamp: kv.Timestamp(),
	}
}

// Value returns the roachpb.Value representation of the value.
func (kv KV) Value() roachpb.Value {
	value := roachpb.MakeValueFromString(kv.ValueString)
	if kv.ValueString == "" {
		value = roachpb.Value{}
	}
	value.InitChecksum(kv.Key())
	return value
}

// MVCCValue returns the storage.MVCCValue representation of the value.
func (kv KV) MVCCValue() storage.MVCCValue {
	return storage.MVCCValue{Value: kv.Value()}
}
