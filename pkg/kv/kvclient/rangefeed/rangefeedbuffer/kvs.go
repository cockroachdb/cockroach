// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeedbuffer

import (
	"cmp"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// RangeFeedValueEventToKV is a function to type assert an Event into a
// *kvpb.RangeFeedValue and then convert it to a roachpb.KeyValue.
func RangeFeedValueEventToKV(rfv *kvpb.RangeFeedValue) roachpb.KeyValue {
	return roachpb.KeyValue{Key: rfv.Key, Value: rfv.Value}
}

// EventsToKVs converts a slice of Events to a slice of KeyValue pairs.
func EventsToKVs[E Event](events []E, f func(ev E) roachpb.KeyValue) []roachpb.KeyValue {
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
	slices.SortFunc(combined, func(a, b roachpb.KeyValue) int {
		return cmp.Or(
			a.Key.Compare(b.Key),
			a.Value.Timestamp.Compare(b.Value.Timestamp),
		)
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
