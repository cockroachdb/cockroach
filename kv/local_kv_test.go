// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matthew O'Connor (matthew.t.oconnor@gmail.com)
// Author: Zach Brock (zbrock@gmail.com)

package kv

import (
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
)

func TestReplicaLookup(t *testing.T) {
	kv := NewLocalKV()
	r1 := addTestRange(kv, engine.KeyMin, engine.Key("C"))
	r2 := addTestRange(kv, engine.Key("C"), engine.Key("X"))
	r3 := addTestRange(kv, engine.Key("X"), engine.KeyMax)
	if len(kv.ranges) != 3 {
		t.Errorf("Pre-condition failed! Expected ranges to be size 3, got %d", len(kv.ranges))
	}

	assertReplicaForRange(t, kv.lookupReplica(engine.KeyMin), r1)
	assertReplicaForRange(t, kv.lookupReplica(engine.Key("B")), r1)
	assertReplicaForRange(t, kv.lookupReplica(engine.Key("C")), r2)
	assertReplicaForRange(t, kv.lookupReplica(engine.Key("M")), r2)
	assertReplicaForRange(t, kv.lookupReplica(engine.Key("X")), r3)
	assertReplicaForRange(t, kv.lookupReplica(engine.Key("Z")), r3)
	if kv.lookupReplica(engine.KeyMax) != nil {
		t.Errorf("Expected engine.KeyMax to not have an associated Replica.")
	}
}

func assertReplicaForRange(t *testing.T, repl *proto.Replica, rng *storage.Range) {
	if repl == nil {
		t.Errorf("No replica returned!")
	} else if repl.RangeID != rng.Meta.RangeID {
		t.Errorf("Wrong replica returned! Expected %+v and %+v to have the same RangeID", rng.Meta, repl)
	}
}

func addTestRange(kv *LocalKV, start, end engine.Key) *storage.Range {
	r := storage.Range{}
	rep := proto.Replica{NodeID: 1, StoreID: 1, RangeID: int64(len(kv.ranges) + 1)}
	r.Meta = &proto.RangeMetadata{
		ClusterID:       "some-cluster",
		RangeID:         rep.RangeID,
		RangeDescriptor: proto.RangeDescriptor{StartKey: start, EndKey: end, Replicas: []proto.Replica{rep}},
	}
	kv.ranges = append(kv.ranges, &r)
	return &r
}
