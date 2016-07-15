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
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package kv

import (
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestLeaseHolderCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	lc := newLeaseHolderCache(3)
	if repDesc, ok := lc.Lookup(12); ok {
		t.Errorf("lookup of missing key returned: %+v", repDesc)
	}
	rangeID := roachpb.RangeID(5)
	replica := roachpb.ReplicaDescriptor{StoreID: 1}
	lc.Update(rangeID, replica)
	if repDesc, ok := lc.Lookup(rangeID); !ok {
		t.Fatalf("expected %+v", replica)
	} else if repDesc != replica {
		t.Errorf("expected %+v, got %+v", replica, repDesc)
	}
	newReplica := roachpb.ReplicaDescriptor{StoreID: 7}
	lc.Update(rangeID, newReplica)
	if repDesc, ok := lc.Lookup(rangeID); !ok {
		t.Fatalf("expected %+v", replica)
	} else if repDesc != newReplica {
		t.Errorf("expected %+v, got %+v", newReplica, repDesc)
	}
	lc.Update(rangeID, roachpb.ReplicaDescriptor{})
	if repDesc, ok := lc.Lookup(rangeID); ok {
		t.Errorf("lookup of evicted key returned: %+v", repDesc)
	}

	for i := 10; i < 20; i++ {
		lc.Update(roachpb.RangeID(i), replica)
	}
	_, ok16 := lc.Lookup(16)
	_, ok17 := lc.Lookup(17)
	if ok16 || !ok17 {
		t.Fatalf("unexpected policy used in cache")
	}
}
