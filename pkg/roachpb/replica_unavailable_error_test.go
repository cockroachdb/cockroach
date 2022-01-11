// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import "testing"

func TestNewReplicaUnavailableError(t *testing.T) {
	var _ = (*ReplicaUnavailableError)(nil)
	rDesc := ReplicaDescriptor{NodeID: 1, StoreID: 2, ReplicaID: 3}
	var set ReplicaSet
	set.AddReplica(rDesc)
	desc := NewRangeDescriptor(123, RKeyMin, RKeyMax, set)
	err := NewReplicaUnavailableError(desc, rDesc)
	_ = err
	// t.Log(err) <-- infinite recursion
}
