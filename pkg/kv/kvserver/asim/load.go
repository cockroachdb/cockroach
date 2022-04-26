// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package asim

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ReplicaLoad defines the methods a datastructure is required to perform in
// order to record and report load events.
type ReplicaLoad interface {
	// AppluLoad applies a load event to a replica.
	ApplyLoad(le LoadEvent)
	// Load translates the recorded load events into usage information of the
	// replica.
	Load() allocator.RangeUsageInfo
}

// ReplicaLoadCounter is the sum of all key accesses and size of bytes, both written
// and read.
// TODO(kvoli): In the non-simulated code, replica_stats currently maintains
// this structure, which is rated. This datastructure needs to be adapated by
// the user to be rated over time. In the future we should introduce a better
// general pupose stucture that enables rating.
type ReplicaLoadCounter struct {
	WriteKeys  int64
	WriteBytes int64
	ReadKeys   int64
	ReadBytes  int64
}

// ApplyLoad applies a load event onto a replica load counter.
func (rl *ReplicaLoadCounter) ApplyLoad(le LoadEvent) {
	if le.isWrite {
		rl.WriteBytes += le.size
		rl.WriteKeys++
	} else {
		rl.ReadBytes += le.size
		rl.ReadKeys++
	}
}

// Load translates the recorded key accesses and size into range usage
// information.
func (rl *ReplicaLoadCounter) Load() allocator.RangeUsageInfo {
	return allocator.RangeUsageInfo{
		LogicalBytes:     rl.WriteBytes,
		QueriesPerSecond: float64(rl.WriteKeys + rl.ReadKeys),
		WritesPerSecond:  float64(rl.WriteKeys),
	}
}

// Capacity returns the current store capacity. It aggregates the load from each
// replica within the store.
func (s *Store) Capacity() roachpb.StoreCapacity {
	capacity := roachpb.StoreCapacity{}
	for _, repl := range s.Replicas {
		if repl.leaseHolder {
			capacity.LeaseCount++
		}
		capacity.RangeCount++
		capacity.QueriesPerSecond += repl.ReplicaLoad.Load().QueriesPerSecond
		capacity.WritesPerSecond += repl.ReplicaLoad.Load().WritesPerSecond
		capacity.LogicalBytes += repl.ReplicaLoad.Load().LogicalBytes
	}
	return capacity
}
