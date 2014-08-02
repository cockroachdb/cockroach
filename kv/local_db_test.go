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

	"github.com/cockroachdb/cockroach/storage"
)

func TestReplicaLookup(t *testing.T) {

	db := NewLocalDB()
	r1 := db.addTestRange(storage.KeyMin, storage.Key("C"))
	r2 := db.addTestRange(storage.Key("C"), storage.Key("X"))
	r3 := db.addTestRange(storage.Key("X"), storage.KeyMax)
	if len(db.ranges) != 3 {
		t.Errorf("Pre-condition failed! Expected ranges to be size 3, got %d", len(db.ranges))
	}

	assertReplicaForRange(t, db.lookupReplica(storage.KeyMin), r1)
	assertReplicaForRange(t, db.lookupReplica(storage.Key("B")), r1)
	assertReplicaForRange(t, db.lookupReplica(storage.Key("C")), r2)
	assertReplicaForRange(t, db.lookupReplica(storage.Key("M")), r2)
	assertReplicaForRange(t, db.lookupReplica(storage.Key("X")), r3)
	assertReplicaForRange(t, db.lookupReplica(storage.Key("Z")), r3)
	if db.lookupReplica(storage.KeyMax) != nil {
		t.Errorf("Expected storage.KeyMax to not have an associated Replica.")
	}
}

func assertReplicaForRange(t *testing.T, repl *storage.Replica, rng *storage.Range) {
	if repl == nil {
		t.Errorf("No replica returned!")
	} else if repl.RangeID != rng.Meta.RangeID {
		t.Errorf("Wrong replica returned! Expected %+v and %+v to have the same RangeID", rng.Meta, repl)
	}
}

func (db *LocalDB) addTestRange(start, end storage.Key) *storage.Range {
	r := storage.Range{}
	rep := storage.Replica{NodeID: 1, StoreID: 1, RangeID: int64(len(db.ranges) + 1)}
	r.Meta = storage.RangeMetadata{
		ClusterID:       "some-cluster",
		RangeID:         rep.RangeID,
		RangeDescriptor: storage.RangeDescriptor{StartKey: start, EndKey: end, Replicas: []storage.Replica{rep}},
	}
	db.ranges = append(db.ranges, &r)
	return &r
}
