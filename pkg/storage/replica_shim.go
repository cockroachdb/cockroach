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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/google/btree"
)

type nonResidentReplica struct {
	rangeDesc          *roachpb.RangeDescriptor
	stats              enginepb.MVCCStats
	leader             bool
	lease              roachpb.Lease
	zone               config.ZoneConfig
	quiescentButBehind bool
}

// ReplicaShim holds either a RangeDescriptor or an initialized
// Replica object, depending on whether the replica is non-resident or
// resident in memory, respectively.
type ReplicaShim struct {
	mu struct {
		syncutil.Mutex
		replica            *Replica // non-nil if resident
		nonResidentReplica          // these fields are only valid if not resident
	}
}

var _ KeyRange = &ReplicaShim{}
var _ btree.Item = &ReplicaShim{}

// RangeID returns the shim range ID.
func (rs *ReplicaShim) RangeID() roachpb.RangeID {
	rs.mu.Lock()
	rs.mu.Unlock()
	if rs.mu.replica != nil {
		return rs.mu.replica.RangeID
	}
	return rs.mu.rangeDesc.RangeID
}

// Desc returns the replica's descriptor. Note that this method
// will acquire the replica's mutex if resident.
func (rs *ReplicaShim) Desc() *roachpb.RangeDescriptor {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.mu.replica != nil {
		return rs.mu.replica.Desc()
	}
	return rs.mu.rangeDesc
}

// QuiescentButBehind returns whether the range was quiesced while one
// or more replicas on non-live nodes were behind.
func (rs *ReplicaShim) QuiescentButBehind() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if repl := rs.mu.replica; repl != nil {
		repl.mu.RLock()
		defer repl.mu.RUnlock()
		return repl.mu.quiescentButBehind
	}
	return rs.mu.quiescentButBehind
}

// MaybeMakeNonResident frees the replica and reduces the shim to
// holding just the minimum data to accurately compute metrics, as.
// long as all conditions enabling replica dehydration hold.
func (rs *ReplicaShim) MaybeMakeNonResident() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	repl := rs.mu.replica
	if repl == nil {
		return
	}
	repl.mu.RLock()
	defer repl.mu.RUnlock()

	// Only make non-resident if the replica is in a proper state.
	if repl.RefCount() > 0 ||
		!repl.mu.quiescent ||
		repl.mu.draining ||
		repl.mu.destroyStatus.reason != destroyReasonAlive ||
		repl.mu.state.Lease.Type() != roachpb.LeaseEpoch ||
		repl.mu.mergeComplete != nil {
		return
	}

	rs.mu.replica = nil
	rs.mu.rangeDesc = repl.mu.state.Desc
	rs.mu.stats = *repl.mu.state.Stats
	rs.mu.leader = isRaftLeader(repl.raftStatusRLocked())
	rs.mu.lease = *repl.mu.state.Lease
	rs.mu.zone = repl.mu.zone
	rs.mu.quiescentButBehind = repl.mu.quiescentButBehind
}

// Capacity returns the replica's latest capacity info if resident.
// Otherwise, a ReplicaCapacity struct is synthesized from information
// the shim has about the non-resident replica.
func (rs *ReplicaShim) Capacity(
	storeID roachpb.StoreID, now hlc.Timestamp, livenessMap IsLiveMap,
) ReplicaCapacity {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rep := rs.mu.replica; rep != nil {
		return rep.Capacity(now)
	}

	_, holder := rs.leaseInfoLocked(storeID, livenessMap)
	return ReplicaCapacity{
		Leaseholder: holder,
		Stats:       rs.mu.stats,
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
) (*Replica, ReplicaMetrics) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rep := rs.mu.replica; rep != nil {
		return rep, rep.Metrics(ctx, storeID, timestamp, livenessMap)
	}

	valid, holder := rs.leaseInfoLocked(storeID, livenessMap)
	rangeCounter, unavailable, underreplicated :=
		calcRangeCounter(storeID, rs.mu.rangeDesc, livenessMap, rs.mu.zone.NumReplicas)

	return nil, ReplicaMetrics{
		Leader:          rs.mu.leader,
		LeaseValid:      valid,
		Leaseholder:     holder,
		LeaseType:       roachpb.LeaseEpoch,
		Quiescent:       true,
		RangeCounter:    rangeCounter,
		Unavailable:     unavailable,
		Underreplicated: underreplicated,
	}
}

func (rs *ReplicaShim) leaseInfoLocked(
	storeID roachpb.StoreID, livenessMap IsLiveMap,
) (valid, holder bool) {
	if rs.mu.lease.Type() == roachpb.LeaseEpoch && livenessMap != nil {
		entry := livenessMap[rs.mu.lease.Replica.NodeID]
		valid = entry.IsLive && entry.Epoch == rs.mu.lease.Epoch
		holder = rs.mu.lease.Replica.StoreID == storeID
	}
	return
}

func (rs *ReplicaShim) endKey() roachpb.RKey {
	return rs.Desc().EndKey
}

// Less implements the btree.Item interface.
func (rs *ReplicaShim) Less(i btree.Item) bool {
	return rs.Desc().EndKey.Less(i.(rangeKeyItem).endKey())
}

func (rs *ReplicaShim) String() string {
	desc := rs.Desc()
	return fmt.Sprintf("range=%d [%s-%s) (shim)",
		desc.RangeID, desc.StartKey, desc.EndKey)
}
