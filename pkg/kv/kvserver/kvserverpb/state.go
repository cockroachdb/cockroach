// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserverpb

// MigratedToRaftClosedTimestamp returns whether this range is using Raft-driven
// closed timestamps. This feature was introduced in 21.1. Instead of doing a
// cluster-version check, which is not atomic across replicas, we look at the
// replica state itself. If any node decided to do the migration (by starting to
// use the RaftClosedTimestamp field), then the cluster version must be 21.1
// regardless of whether any individual node already knows that or not.
//
// TODO(andrei): Remove in 21.2.
func (s ReplicaState) MigratedToRaftClosedTimestamp() bool {
	return !s.RaftClosedTimestamp.IsEmpty()
}
