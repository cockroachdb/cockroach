// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License.

package kv

import (
	"testing"

	"golang.org/x/net/context"

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
	lc := NewLeaseHolderCache(staticSize(2))
	if repDesc, ok := lc.Lookup(ctx, 12); ok {
		t.Errorf("lookup of missing key returned: %+v", repDesc)
	}
	rangeID := roachpb.RangeID(5)
	replica := roachpb.ReplicaDescriptor{StoreID: 1}
	lc.Update(ctx, rangeID, replica)
	if repDesc, ok := lc.Lookup(ctx, rangeID); !ok {
		t.Fatalf("expected %+v", replica)
	} else if repDesc != replica {
		t.Errorf("expected %+v, got %+v", replica, repDesc)
	}
	newReplica := roachpb.ReplicaDescriptor{StoreID: 7}
	lc.Update(ctx, rangeID, newReplica)
	if repDesc, ok := lc.Lookup(ctx, rangeID); !ok {
		t.Fatalf("expected %+v", replica)
	} else if repDesc != newReplica {
		t.Errorf("expected %+v, got %+v", newReplica, repDesc)
	}
	lc.Update(ctx, rangeID, roachpb.ReplicaDescriptor{})
	if repDesc, ok := lc.Lookup(ctx, rangeID); ok {
		t.Errorf("lookup of evicted key returned: %+v", repDesc)
	}

	for i := 10; i < 45; i++ {
		lc.Update(ctx, roachpb.RangeID(i), replica)
	}
	_, ok12 := lc.Lookup(ctx, 12)
	_, ok13 := lc.Lookup(ctx, 13)
	if ok12 || !ok13 {
		t.Fatalf("unexpected policy used in cache : %v, %v", ok12, ok13)
	}
}

func BenchmarkLeaseHolderCacheParallel(b *testing.B) {
	defer leaktest.AfterTest(b)()
	ctx := context.TODO()
	lc := NewLeaseHolderCache(staticSize(4))
	for i := 1; i < 1<<6; i++ {
		rangeID := roachpb.RangeID(i)
		replica := roachpb.ReplicaDescriptor{StoreID: roachpb.StoreID(i)}
		lc.Update(ctx, rangeID, replica)
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 1; i < 1<<6; i++ {
				rangeID := roachpb.RangeID(i)
				replica := roachpb.ReplicaDescriptor{StoreID: roachpb.StoreID(i)}
				if _, ok := lc.Lookup(ctx, rangeID); !ok {
					b.Fatalf("expected %+v", replica)
				}
			}
		}
	})
}
