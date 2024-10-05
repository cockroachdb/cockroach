// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeedbuffer

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

// TestMergeKVs tests the logic of MergeKVs and the logic of to extract
// KVs from a slice of events.
func TestMergeKVs(t *testing.T) {
	type row struct {
		key   string
		ts    int64
		value string
	}
	prefix := keys.SystemSQLCodec.TablePrefix(1)
	prefix = prefix[:len(prefix):len(prefix)]
	mkKey := func(r row) roachpb.Key {
		return encoding.EncodeStringAscending(prefix, r.key)
	}
	toKeyValue := func(r row) (kv roachpb.KeyValue) {
		kv.Key = mkKey(r)
		kv.Value.Timestamp = hlc.Timestamp{WallTime: r.ts}
		if r.value != "" {
			kv.Value.SetString(r.value)
		}
		kv.Value.InitChecksum(kv.Key)
		return kv
	}
	toRangeFeedEvent := func(r row) *kvpb.RangeFeedValue {
		kv := toKeyValue(r)
		return &kvpb.RangeFeedValue{
			Key:   kv.Key,
			Value: kv.Value,
		}
	}
	toKVs := func(rows []row) (kvs []roachpb.KeyValue) {
		for _, r := range rows {
			kvs = append(kvs, toKeyValue(r))
		}
		return kvs
	}
	toBuffer := func(t *testing.T, rows []row) *Buffer[*kvpb.RangeFeedValue] {
		buf := New[*kvpb.RangeFeedValue](len(rows))
		for _, r := range rows {
			require.NoError(t, buf.Add(toRangeFeedEvent(r)))
		}
		return buf
	}
	toKVsThroughBuffer := func(t *testing.T, rows []row) []roachpb.KeyValue {
		return EventsToKVs(toBuffer(t, rows).Flush(
			context.Background(),
			hlc.Timestamp{WallTime: math.MaxInt64},
		), RangeFeedValueEventToKV)
	}
	type testCase [3][]row // (a, b, merged)
	for _, tc := range []testCase{
		{
			{
				{"a", 1, "asdf"},
				{"a", 4, "af"},
				{"c", 3, "boo"},
			},
			{
				{"a", 5, "winner"},
				{"b", 5, "2as"},
				{"b", 1, "2"},
				{"c", 4, ""},
				{"d", 4, ""},
			},
			{
				{"a", 5, "winner"},
				{"b", 5, "2as"},
			},
		},
	} {
		a, b, merged := tc[0], tc[1], tc[2]

		require.Equal(t, toKVs(merged), MergeKVs(toKVs(a), toKVs(b)))

		// Exercise the conversions out of RangeFeedValue.
		require.Equal(t, toKVs(merged), MergeKVs(
			toKVsThroughBuffer(t, a), toKVsThroughBuffer(t, b),
		))
	}
}
