// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func staticSize(size int64) func() int64 {
	return func() int64 {
		return size
	}
}

func TestLeaseHolderCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.TODO()
	cacheSize := (1 << 4) * defaultShards
	lc := NewLeaseHolderCache(staticSize(int64(cacheSize)))
	if repStoreID, ok := lc.Lookup(ctx, 12); ok {
		t.Errorf("lookup of missing key returned: %d", repStoreID)
	}
	rangeID := roachpb.RangeID(5)
	replicaStoreID := roachpb.StoreID(1)
	lc.Update(ctx, rangeID, replicaStoreID)
	if repStoreID, ok := lc.Lookup(ctx, rangeID); !ok {
		t.Fatalf("expected StoreID %d", replicaStoreID)
	} else if repStoreID != replicaStoreID {
		t.Errorf("expected StoreID %d, got %d", replicaStoreID, repStoreID)
	}
	newReplicaStoreID := roachpb.StoreID(7)
	lc.Update(ctx, rangeID, newReplicaStoreID)
	if repStoreID, ok := lc.Lookup(ctx, rangeID); !ok {
		t.Fatalf("expected StoreID %d", replicaStoreID)
	} else if repStoreID != newReplicaStoreID {
		t.Errorf("expected StoreID %d, got %d", newReplicaStoreID, repStoreID)
	}

	lc.Update(ctx, rangeID, roachpb.StoreID(0))
	if repStoreID, ok := lc.Lookup(ctx, rangeID); ok {
		t.Errorf("lookup of evicted key returned: %d", repStoreID)
	}

	for i := 10; i < 10+cacheSize+2; i++ {
		lc.Update(ctx, roachpb.RangeID(i), replicaStoreID)
	}
	_, ok11 := lc.Lookup(ctx, 11)
	_, ok12 := lc.Lookup(ctx, 12)
	if ok11 || !ok12 {
		t.Fatalf("unexpected policy used in cache : %v, %v", ok11, ok12)
	}
}

func BenchmarkLeaseHolderCacheParallel(b *testing.B) {
	defer leaktest.AfterTest(b)()
	ctx := context.TODO()
	cacheSize := (1 << 4) * defaultShards
	lc := NewLeaseHolderCache(staticSize(int64(cacheSize)))
	numRanges := 2 * len(lc.shards)
	for i := 1; i <= numRanges; i++ {
		rangeID := roachpb.RangeID(i)
		lc.Update(ctx, rangeID, roachpb.StoreID(i))
	}
	b.RunParallel(func(pb *testing.PB) {
		var n int
		for pb.Next() {
			rangeID := roachpb.RangeID(n%numRanges + 1)
			n++
			if _, ok := lc.Lookup(ctx, rangeID); !ok {
				b.Fatalf("r%d: should be found in the cache", rangeID)
			}
		}
	})
}
