// Copyright 2018 The Cockroach Authors.
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

package storage

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/google/btree"
)

type nonResidentReplica struct {
	desc               *roachpb.RangeDescriptor
	stats              enginepb.MVCCStats
	zone               *config.ZoneConfig
	leader             bool
	lease              roachpb.Lease
	quiescentButBehind bool
}

// ReplicaShim holds either a RangeDescriptor or an initialized
// Replica object, depending on whether the replica is non-resident or
// resident in memory, respectively.
type ReplicaShim struct {
	syncutil.Mutex
	replica *Replica // non-nil if resident

	// nonResidentReplica not valid if replica is non-nil.
	nonResidentReplica
}

var _ KeyRange = &ReplicaShim{}
var _ btree.Item = &ReplicaShim{}

// RangeID returns the shim range ID.
func (rs *ReplicaShim) RangeID() roachpb.RangeID {
	rs.Lock()
	defer rs.Unlock()
	if rs.replica != nil {
		return rs.replica.RangeID
	}
	return rs.desc.RangeID
}

// Desc returns the replica's descriptor. Note that this method
// will acquire the replica's mutex if resident.
func (rs *ReplicaShim) Desc() *roachpb.RangeDescriptor {
	rs.Lock()
	defer rs.Unlock()
	if rs.replica != nil {
		return rs.replica.Desc()
	}
	return rs.desc
}

// GetMVCCStats returns the replica's or shim's MVCC stats.
func (rs *ReplicaShim) GetMVCCStats() enginepb.MVCCStats {
	rs.Lock()
	defer rs.Unlock()
	if rs.replica != nil {
		return rs.replica.GetMVCCStats()
	}
	return rs.stats
}

// SetZoneConfig sets the replica's zone config.
func (rs *ReplicaShim) SetZoneConfig(zone *config.ZoneConfig) {
	rs.Lock()
	defer rs.Unlock()
	if rs.replica != nil {
		rs.replica.SetZoneConfig(zone)
	} else {
		rs.zone = zone
	}
}

// ContainsKey returns whether this range contains the specified key.
func (rs *ReplicaShim) ContainsKey(key roachpb.Key) bool {
	rs.Lock()
	defer rs.Unlock()
	if rs.replica != nil {
		return rs.replica.ContainsKey(key)
	}
	return storagebase.ContainsKey(*rs.desc, key)
}

// QuiescentButBehind returns whether the range was quiesced while one
// or more replicas on non-live nodes were behind.
func (rs *ReplicaShim) QuiescentButBehind() bool {
	rs.Lock()
	defer rs.Unlock()
	if repl := rs.replica; repl != nil {
		repl.mu.RLock()
		defer repl.mu.RUnlock()
		return repl.mu.quiescentButBehind
	}
	return rs.quiescentButBehind
}

// GetReplica returns the replica if resident, or else loads the
// replica to make it resident and potentially returns an error on
// failure to initialize.
func (rs *ReplicaShim) GetReplica(s *Store) (*Replica, error) {
	rs.Lock()
	defer rs.Unlock()
	if rs.replica != nil {
		return rs.replica, nil
	}
	replica, err := NewReplica(rs.desc, s, 0)
	if err != nil {
		return nil, err
	}
	replica.SetZoneConfig(rs.zone)
	rs.replica = replica
	rs.nonResidentReplica = nonResidentReplica{}
	return rs.replica, nil
}

/*
// MaybeMakeNonResident frees the replica and reduces the shim to
// holding just the range descriptor, if possible. Returns whether the
// shim was dehydrated.
func (rs *ReplicaShim) MaybeMakeNonResident() bool {
	rs.Lock()
	defer rs.Unlock()
	repl := rs.replica
	if repl == nil {
		return false
	}
	repl.mu.RLock()
	defer repl.mu.RLock()
	// Only make non-resident if the replica is in a proper state.
	if !repl.mu.quiescent ||
		repl.mu.draining ||
		repl.mu.destroyStatus.reason != destroyReasonAlive ||
		repl.mu.state.Lease.Type() != roachpb.LeaseEpoch ||
		repl.mu.mergeComplete != nil ||
		!repl.isInitializedRLocked() {
		return false
	}
	rs.replica = nil
	rs.desc = repl.mu.state.Desc
	rs.stats = *repl.mu.state.Stats
	rs.zone = repl.mu.zone
	rs.leader = isRaftLeader(repl.raftStatusRLocked())
	rs.lease = *repl.mu.state.Lease
	rs.quiescentButBehind = repl.mu.quiescentButBehind
	return true
}
*/

// Capacity returns the replica's latest capacity info if resident.
// Otherwise, a ReplicaCapacity struct is synthesized from information
// the shim has about the non-resident replica.
func (rs *ReplicaShim) Capacity(
	storeID roachpb.StoreID, now hlc.Timestamp, livenessMap IsLiveMap,
) ReplicaCapacity {
	rs.Lock()
	defer rs.Unlock()
	if rep := rs.replica; rep != nil {
		return rep.Capacity(now)
	}
	_, holder := rs.leaseInfoLocked(storeID, livenessMap)
	return ReplicaCapacity{
		Leaseholder: holder,
		Stats:       rs.stats,
	}
}

// Metrics returns the replica and its latest metrics if
// resident. Otherwise, returns nil and a ReplicaMetrics object
// synthesized from information the shim has about the non-resident
// replica.
func (rs *ReplicaShim) Metrics(
	ctx context.Context,
	storeID roachpb.StoreID,
	timestamp hlc.Timestamp,
	livenessMap IsLiveMap,
	availableNodes int,
) (*Replica, ReplicaMetrics) {
	rs.Lock()
	defer rs.Unlock()
	if rep := rs.replica; rep != nil {
		return rep, rep.Metrics(ctx, timestamp, livenessMap, availableNodes)
	}
	valid, holder := rs.leaseInfoLocked(storeID, livenessMap)
	rangeCounter, unavailable, underreplicated :=
		calcRangeCounter(storeID, rs.desc, livenessMap, *rs.zone.NumReplicas, availableNodes)
	return nil, ReplicaMetrics{
		Leader:          rs.leader,
		LeaseValid:      valid,
		Leaseholder:     holder,
		LeaseType:       roachpb.LeaseEpoch,
		Quiescent:       true, // non-resident replicas are always quiescent
		RangeCounter:    rangeCounter,
		Unavailable:     unavailable,
		Underreplicated: underreplicated,
	}
}

func (rs *ReplicaShim) leaseInfoLocked(
	storeID roachpb.StoreID, livenessMap IsLiveMap,
) (valid, holder bool) {
	if rs.lease.Type() == roachpb.LeaseEpoch && livenessMap != nil {
		entry := livenessMap[rs.lease.Replica.NodeID]
		valid = entry.IsLive && entry.Epoch == rs.lease.Epoch
		holder = rs.lease.Replica.StoreID == storeID
	}
	return
}

// startKey implements the rangeKeyItem interface.
func (rs *ReplicaShim) startKey() roachpb.RKey {
	return rs.Desc().StartKey
}

// Less implements the btree.Item interface.
func (rs *ReplicaShim) Less(i btree.Item) bool {
	return rs.startKey().Less(i.(rangeKeyItem).startKey())
}

func (rs *ReplicaShim) String() string {
	desc := rs.Desc()
	return fmt.Sprintf("range=%d [%s-%s) (shim)",
		desc.RangeID, desc.StartKey, desc.EndKey)
}
