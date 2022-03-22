// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type rangeIDReplicaMap syncutil.IntMap

// Load loads the Replica for the RangeID. If not found, returns
// (nil, false), otherwise the Replica and true.
func (m *rangeIDReplicaMap) Load(rangeID roachpb.RangeID) (*Replica, bool) {
	val, ok := (*syncutil.IntMap)(m).Load(int64(rangeID))
	return (*Replica)(val), ok
}

// LoadOrStore loads the replica and returns it (and `true`). If it does not
// exist, atomically inserts the provided Replica and returns it along with
// `false`.
func (m *rangeIDReplicaMap) LoadOrStore(
	rangeID roachpb.RangeID, repl *Replica,
) (_ *Replica, loaded bool) {
	val, loaded := (*syncutil.IntMap)(m).LoadOrStore(int64(rangeID), unsafe.Pointer(repl))
	return (*Replica)(val), loaded
}

// Delete drops the Replica if it existed in the map.
func (m *rangeIDReplicaMap) Delete(rangeID roachpb.RangeID) {
	(*syncutil.IntMap)(m).Delete(int64(rangeID))
}

// Range invokes the provided function with each Replica in the map.
func (m *rangeIDReplicaMap) Range(f func(*Replica)) {
	v := func(k int64, v unsafe.Pointer) bool {
		f((*Replica)(v))
		return true // wantMore
	}
	(*syncutil.IntMap)(m).Range(v)
}
