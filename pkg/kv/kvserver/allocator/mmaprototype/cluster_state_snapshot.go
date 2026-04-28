// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype/mmasnappb"

// Snapshot returns a structured proto representation of cs suitable for
// diagnostics and offline analysis. The returned proto can be marshaled to
// proto binary, jsonpb, or text.
//
// The snapshot is not a complete round-trip representation of clusterState:
// scratch/workspace fields, runtime injections (clocks, interface back-refs),
// and pure caches are intentionally omitted. The
// TestSnapshotCoversAllFields test enforces that every state-bearing field
// reachable from clusterState is either represented in the snapshot or has a
// recorded omission reason, so adding a new field to clusterState (or any
// owned struct it transitively references) forces a deliberate decision about
// snapshot inclusion.
func (cs *clusterState) Snapshot() *mmasnappb.ClusterStateSnapshot {
	return &mmasnappb.ClusterStateSnapshot{
		MMAID:                   int32(cs.mmaid),
		DiskUtilRefuseThreshold: cs.diskUtilRefuseThreshold,
		DiskUtilShedThreshold:   cs.diskUtilShedThreshold,
		ChangeSeqGen:            uint64(cs.changeSeqGen),
	}
}
