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
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type rangeIDReplicaMap struct {
	m syncutil.IntMap
}

// Load loads the Replica for the RangeID. If not found, returns
// (nil, false), otherwise the Replica and true.
func (m *rangeIDReplicaMap) Load(rangeID roachpb.RangeID) (*Replica, bool) {
	val, ok := m.m.Load(int64(rangeID))
	return (*Replica)(val), ok
}

// LoadOrStore loads the replica and returns it (and `true`). If it does not
// exist, atomically inserts the provided Replica and returns it along with
// `false`.
func (m *rangeIDReplicaMap) LoadOrStore(
	rangeID roachpb.RangeID, repl *Replica,
) (_ *Replica, loaded bool) {
	val, loaded := m.m.LoadOrStore(int64(rangeID), unsafe.Pointer(repl))
	return (*Replica)(val), loaded
}

// Delete drops the Replica if it existed in the map.
func (m *rangeIDReplicaMap) Delete(rangeID roachpb.RangeID) {
	m.m.Delete(int64(rangeID))
}

// Range invokes the provided function with each Replica in the map.
// Iteration stops on any error. `iterutil.StopIteration()` can be
// returned from the closure to stop iteration without an error
// resulting from Range().
func (m *rangeIDReplicaMap) Range(f func(*Replica) error) error {
	var err error
	v := func(k int64, v unsafe.Pointer) (wantMore bool) {
		err = f((*Replica)(v))
		return err == nil
	}
	m.m.Range(v)
	if errors.Is(err, iterutil.StopIteration()) {
		return nil
	}
	return nil
}
