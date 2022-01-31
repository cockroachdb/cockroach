// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeedbuffer

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// RangeFeedValueEventToKV is a function to type assert an Event into a
// *roachpb.RangeFeedValue and then convert it to a roachpb.KeyValue.
func RangeFeedValueEventToKV(event Event) roachpb.KeyValue {
	rfv := event.(*roachpb.RangeFeedValue)
	return roachpb.KeyValue{Key: rfv.Key, Value: rfv.Value}
}

// EventsToKVs converts a slice of Events to a slice of KeyValue pairs.
func EventsToKVs(events []Event, f func(ev Event) roachpb.KeyValue) []roachpb.KeyValue {
	kvs := make([]roachpb.KeyValue, 0, len(events))
	for _, ev := range events {
		kvs = append(kvs, f(ev))
	}
	return kvs
}

// MergeKVs merges two sets of KVs into a single set of KVs with at most one
// KV for any key. The latest value in the merged set wins. If the latest
// value in the set corresponds to a deletion (i.e. its IsPresent() method
// returns false), the value will be omitted from the final set.
//
// Note that the assumption is that base has no duplicated keys. If the set
// of updates is empty, base is returned directly.
func MergeKVs(base, updates []roachpb.KeyValue) []roachpb.KeyValue {
	if len(updates) == 0 {
		return base
	}
	combined := make([]roachpb.KeyValue, 0, len(base)+len(updates))
	combined = append(append(combined, base...), updates...)
	sort.Slice(combined, func(i, j int) bool {
		cmp := combined[i].Key.Compare(combined[j].Key)
		if cmp == 0 {
			return combined[i].Value.Timestamp.Less(combined[j].Value.Timestamp)
		}
		return cmp < 0
	})
	r := combined[:0]
	for _, kv := range combined {
		prevIsSameKey := len(r) > 0 && r[len(r)-1].Key.Equal(kv.Key)
		if kv.Value.IsPresent() {
			if prevIsSameKey {
				r[len(r)-1] = kv
			} else {
				r = append(r, kv)
			}
		} else {
			if prevIsSameKey {
				r = r[:len(r)-1]
			}
		}
	}
	return r
}
