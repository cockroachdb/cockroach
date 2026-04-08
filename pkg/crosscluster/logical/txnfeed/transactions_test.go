// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func makeKV(key string, wallTime int64) streampb.StreamEvent_KV {
	return streampb.StreamEvent_KV{
		KeyValue: roachpb.KeyValue{
			Key: roachpb.Key(key),
			Value: roachpb.Value{
				Timestamp: hlc.Timestamp{WallTime: wallTime},
			},
		},
	}
}

func TestTransactions(t *testing.T) {
	// Build a batch with three transactions of different sizes: 1 KV at
	// ts=10, 3 KVs at ts=20, and 5 KVs at ts=30.
	batch := []streampb.StreamEvent_KV{
		makeKV("a", 10),
		makeKV("b", 20),
		makeKV("c", 20),
		makeKV("d", 20),
		makeKV("e", 30),
		makeKV("f", 30),
		makeKV("g", 30),
		makeKV("h", 30),
		makeKV("i", 30),
	}

	wantSizes := []int{1, 3, 5}
	wantTimestamps := []int64{10, 20, 30}

	var idx int
	for txn := range Transactions(batch) {
		require.Less(t, idx, len(wantSizes), "more transactions than expected")
		require.Len(t, txn, wantSizes[idx])
		for _, kv := range txn {
			require.Equal(t, wantTimestamps[idx], kv.KeyValue.Value.Timestamp.WallTime)
		}
		idx++
	}
	require.Equal(t, len(wantSizes), idx, "fewer transactions than expected")
}
