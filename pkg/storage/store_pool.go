// Copyright 2015 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const (
	// TestTimeUntilStoreDead is the test value for TimeUntilStoreDead to
	// quickly mark stores as dead.
	TestTimeUntilStoreDead = 5 * time.Millisecond

	// TestTimeUntilStoreDeadOff is the test value for TimeUntilStoreDead that
	// prevents the store pool from marking stores as dead.
	TestTimeUntilStoreDeadOff = 24 * time.Hour
)

// declinedReservationsTimeout needs to be non-zero to prevent useless retries
// in the replicateQueue.process() retry loop.
var declinedReservationsTimeout = settings.RegisterNonNegativeDurationSetting(
	"server.declined_reservation_timeout",
	"the amount of time to consider the store throttled for up-replication after a reservation was declined",
	1*time.Second,
)

var failedReservationsTimeout = settings.RegisterNonNegativeDurationSetting(
	"server.failed_reservation_timeout",
	"the amount of time to consider the store throttled for up-replication after a failed reservation call",
	5*time.Second,
)

// TimeUntilStoreDead wraps "server.time_until_store_dead".
var TimeUntilStoreDead = settings.RegisterNonNegativeDurationSetting(
	"server.time_until_store_dead",
	"the time after which if there is no new gossiped information about a store, it is considered dead",
	5*time.Minute,
)

// A NodeLivenessFunc accepts a node ID, current time and threshold before
// a node is considered dead and returns whether or not the node is live.
type NodeLivenessFunc func(roachpb.NodeID, time.Time, time.Duration) NodeLivenessStatus

// MakeStorePoolNodeLivenessFunc returns a function which determines
// the status of a node based on information provided by the specified
// NodeLiveness.
func MakeStorePoolNodeLivenessFunc(nodeLiveness *NodeLiveness) NodeLivenessFunc {
	return func(nodeID roachpb.NodeID, now time.Time, threshold time.Duration) NodeLivenessStatus {
		liveness, err := nodeLiveness.GetLiveness(nodeID)
		if err != nil {
			return NodeLivenessStatus_UNAVAILABLE
		}
		return liveness.LivenessStatus(now, threshold, nodeLiveness.clock.MaxOffset())
	}
}

type storeDetail struct {
	desc *roachpb.StoreDescriptor
	// throttledUntil is when a throttled store can be considered available again
	// due to a failed or declined snapshot.
	throttledUntil time.Time
	// lastUpdatedTime is set when a store is first consulted and every time
	// gossip arrives for a store.
	lastUpdatedTime time.Time
	deadReplicas    map[roachpb.RangeID][]roachpb.ReplicaDescriptor
}

// isThrottled returns whether the store is currently throttled.
func (sd storeDetail) isThrottled(now time.Time) bool {
	return sd.throttledUntil.After(now)
}

// storeStatus is the current status of a store.
type storeStatus int

// These are the possible values for a storeStatus.
const (
	_ storeStatus = iota
	// The store's node is not live or no gossip has been received from
	// the store for more than the timeUntilStoreDead threshold.
	storeStatusDead
	// The store isn't available because it hasn't gossiped yet. This
	// status lasts until either gossip is received from the store or
	// the timeUntilStoreDead threshold has passed, at which point its
	// status will change to dead.
	storeStatusUnknown
	// The store is alive but it is throttled.
	storeStatusThrottled
	// The store is alive but a replica for the same rangeID was recently
	// discovered to be corrupt.
	storeStatusReplicaCorrupted
	// The store is alive and available.
	storeStatusAvailable
	// The store is decommissioning.
	storeStatusDecommissioning
)

// status returns the current status of the store, including whether
// any of the replicas for the specified rangeID are corrupted.
func (sd *storeDetail) status(
	now time.Time, threshold time.Duration, rangeID roachpb.RangeID, nl NodeLivenessFunc,
) storeStatus {
	// The store is considered dead if it hasn't been updated via gossip
	// within the liveness threshold. Note that lastUpdatedTime is set
	// when the store detail is created and will have a non-zero value
	// even before the first gossip arrives for a store.
	deadAsOf := sd.lastUpdatedTime.Add(threshold)
	if now.After(deadAsOf) {
		return storeStatusDead
	}
	// If there's no descriptor (meaning no gossip ever arrived for this
	// store), return unavailable.
	if sd.desc == nil {
		return storeStatusUnknown
	}

	// Even if the store has been updated via gossip, we still rely on
	// the node liveness to determine whether it is considered live.
	switch nl(sd.desc.Node.NodeID, now, threshold) {
	case NodeLivenessStatus_DEAD, NodeLivenessStatus_DECOMMISSIONED:
		return storeStatusDead
	case NodeLivenessStatus_DECOMMISSIONING:
		return storeStatusDecommissioning
	case NodeLivenessStatus_UNKNOWN, NodeLivenessStatus_UNAVAILABLE:
		return storeStatusUnknown
	}

	if sd.isThrottled(now) {
		return storeStatusThrottled
	}
	if len(sd.deadReplicas[rangeID]) > 0 {
		return storeStatusReplicaCorrupted
	}

	return storeStatusAvailable
}

// localityWithString maintains a string representation of each locality along
// with its protocol buffer implementation. This is for the sake of optimizing
// memory usage by allocating a single copy of each that can be returned to
// callers of getNodeLocalityString rather than each caller (which is currently
// each replica in the local store) making its own copy.
type localityWithString struct {
	locality roachpb.Locality
	str      string
}

// StorePool maintains a list of all known stores in the cluster and
// information on their health.
type StorePool struct {
	log.AmbientContext
	st *cluster.Settings

	clock          *hlc.Clock
	gossip         *gossip.Gossip
	nodeLivenessFn NodeLivenessFunc
	startTime      time.Time
	deterministic  bool
	// We use separate mutexes for storeDetails and nodeLocalities because the
	// nodeLocalities map is used in the critical code path of Replica.Send()
	// and we'd rather not block that on something less important accessing
	// storeDetails.
	detailsMu struct {
		syncutil.RWMutex
		storeDetails map[roachpb.StoreID]*storeDetail
	}
	localitiesMu struct {
		syncutil.RWMutex
		nodeLocalities map[roachpb.NodeID]localityWithString
	}
}

// NewStorePool creates a StorePool and registers the store updating callback
// with gossip.
func NewStorePool(
	ambient log.AmbientContext,
	st *cluster.Settings,
	g *gossip.Gossip,
	clock *hlc.Clock,
	nodeLivenessFn NodeLivenessFunc,
	deterministic bool,
) *StorePool {
	sp := &StorePool{
		AmbientContext: ambient,
		st:             st,
		clock:          clock,
		gossip:         g,
		nodeLivenessFn: nodeLivenessFn,
		startTime:      clock.PhysicalTime(),
		deterministic:  deterministic,
	}
	sp.detailsMu.storeDetails = make(map[roachpb.StoreID]*storeDetail)
	sp.localitiesMu.nodeLocalities = make(map[roachpb.NodeID]localityWithString)

	storeRegex := gossip.MakePrefixPattern(gossip.KeyStorePrefix)
	g.RegisterCallback(storeRegex, sp.storeGossipUpdate)
	deadReplicasRegex := gossip.MakePrefixPattern(gossip.KeyDeadReplicasPrefix)
	g.RegisterCallback(deadReplicasRegex, sp.deadReplicasGossipUpdate)

	return sp
}

func (sp *StorePool) String() string {
	sp.detailsMu.RLock()
	defer sp.detailsMu.RUnlock()

	ids := make(roachpb.StoreIDSlice, 0, len(sp.detailsMu.storeDetails))
	for id := range sp.detailsMu.storeDetails {
		ids = append(ids, id)
	}
	sort.Sort(ids)

	var buf bytes.Buffer
	now := sp.clock.PhysicalTime()

	for _, id := range ids {
		detail := sp.detailsMu.storeDetails[id]
		fmt.Fprintf(&buf, "%d", id)
		status := detail.status(now, TimeUntilStoreDead.Get(&sp.st.SV), 0, sp.nodeLivenessFn)
		if status != storeStatusAvailable {
			fmt.Fprintf(&buf, " (status=%d)", status)
		}
		fmt.Fprintf(&buf, ": range-count=%d fraction-used=%.2f",
			detail.desc.Capacity.RangeCount, detail.desc.Capacity.FractionUsed())
		throttled := detail.throttledUntil.Sub(now)
		if throttled > 0 {
			fmt.Fprintf(&buf, " [throttled=%.1fs]", throttled.Seconds())
		}
		_, _ = buf.WriteString("\n")
	}
	return buf.String()
}

// storeGossipUpdate is the gossip callback used to keep the StorePool up to date.
func (sp *StorePool) storeGossipUpdate(_ string, content roachpb.Value) {
	var storeDesc roachpb.StoreDescriptor
	if err := content.GetProto(&storeDesc); err != nil {
		ctx := sp.AnnotateCtx(context.TODO())
		log.Error(ctx, err)
		return
	}

	sp.detailsMu.Lock()
	detail := sp.getStoreDetailLocked(storeDesc.StoreID)
	detail.desc = &storeDesc
	detail.lastUpdatedTime = sp.clock.PhysicalTime()
	sp.detailsMu.Unlock()

	sp.localitiesMu.Lock()
	sp.localitiesMu.nodeLocalities[storeDesc.Node.NodeID] =
		localityWithString{storeDesc.Node.Locality, storeDesc.Node.Locality.String()}
	sp.localitiesMu.Unlock()
}

// deadReplicasGossipUpdate is the gossip callback used to keep the StorePool up to date.
func (sp *StorePool) deadReplicasGossipUpdate(_ string, content roachpb.Value) {
	var replicas roachpb.StoreDeadReplicas
	if err := content.GetProto(&replicas); err != nil {
		ctx := sp.AnnotateCtx(context.TODO())
		log.Error(ctx, err)
		return
	}

	sp.detailsMu.Lock()
	defer sp.detailsMu.Unlock()
	detail := sp.getStoreDetailLocked(replicas.StoreID)
	deadReplicas := make(map[roachpb.RangeID][]roachpb.ReplicaDescriptor)
	for _, r := range replicas.Replicas {
		deadReplicas[r.RangeID] = append(deadReplicas[r.RangeID], r.Replica)
	}
	detail.deadReplicas = deadReplicas
}

// updateLocalStoreAfterRebalance is used to update the local copy of the
// target store immediately after a rebalance.
func (sp *StorePool) updateLocalStoreAfterRebalance(
	storeID roachpb.StoreID, rangeInfo RangeInfo, changeType roachpb.ReplicaChangeType,
) {
	sp.detailsMu.Lock()
	defer sp.detailsMu.Unlock()
	detail := *sp.getStoreDetailLocked(storeID)
	if detail.desc == nil {
		// We don't have this store yet (this is normal when we're
		// starting up and don't have full information from the gossip
		// network). We can't update the local store at this time.
		return
	}
	switch changeType {
	case roachpb.ADD_REPLICA:
		detail.desc.Capacity.LogicalBytes += rangeInfo.LogicalBytes
		detail.desc.Capacity.WritesPerSecond += rangeInfo.WritesPerSecond
	case roachpb.REMOVE_REPLICA:
		if detail.desc.Capacity.LogicalBytes <= rangeInfo.LogicalBytes {
			detail.desc.Capacity.LogicalBytes = 0
		} else {
			detail.desc.Capacity.LogicalBytes -= rangeInfo.LogicalBytes
		}
		if detail.desc.Capacity.WritesPerSecond <= rangeInfo.WritesPerSecond {
			detail.desc.Capacity.WritesPerSecond = 0
		} else {
			detail.desc.Capacity.WritesPerSecond -= rangeInfo.WritesPerSecond
		}
	}
	sp.detailsMu.storeDetails[storeID] = &detail
}

// newStoreDetail makes a new storeDetail struct. It sets index to be -1 to
// ensure that it will be processed by a queue immediately.
func newStoreDetail() *storeDetail {
	return &storeDetail{
		deadReplicas: make(map[roachpb.RangeID][]roachpb.ReplicaDescriptor),
	}
}

// getStoreDetailLocked returns the store detail for the given storeID.
// The lock must be held *in write mode* even though this looks like a
// read-only method.
func (sp *StorePool) getStoreDetailLocked(storeID roachpb.StoreID) *storeDetail {
	detail, ok := sp.detailsMu.storeDetails[storeID]
	if !ok {
		// We don't have this store yet (this is normal when we're
		// starting up and don't have full information from the gossip
		// network). The first time this occurs, presume the store is
		// alive, but start the clock so it will become dead if enough
		// time passes without updates from gossip.
		detail = newStoreDetail()
		detail.lastUpdatedTime = sp.startTime
		sp.detailsMu.storeDetails[storeID] = detail
	}
	return detail
}

// getStoreDescriptor returns the latest store descriptor for the given
// storeID.
func (sp *StorePool) getStoreDescriptor(storeID roachpb.StoreID) (roachpb.StoreDescriptor, bool) {
	sp.detailsMu.RLock()
	defer sp.detailsMu.RUnlock()

	if detail, ok := sp.detailsMu.storeDetails[storeID]; ok && detail.desc != nil {
		return *detail.desc, true
	}
	return roachpb.StoreDescriptor{}, false
}

// decommissioningReplicas filters out replicas on decommissioning node/store
// from the provided repls and returns them in a slice.
func (sp *StorePool) decommissioningReplicas(
	rangeID roachpb.RangeID, repls []roachpb.ReplicaDescriptor,
) (decommissioningReplicas []roachpb.ReplicaDescriptor) {
	sp.detailsMu.Lock()
	defer sp.detailsMu.Unlock()

	now := sp.clock.PhysicalTime()
	for _, repl := range repls {
		detail := sp.getStoreDetailLocked(repl.StoreID)
		switch detail.status(now, TimeUntilStoreDead.Get(&sp.st.SV), rangeID, sp.nodeLivenessFn) {
		case storeStatusDecommissioning:
			decommissioningReplicas = append(decommissioningReplicas, repl)
		}
	}
	return
}

// liveAndDeadReplicas divides the provided repls slice into two slices: the
// first for live replicas, and the second for dead replicas.
// Replicas for which liveness or deadness cannot be ascertained are excluded
// from the returned slices.  Replicas on decommissioning node/store are
// considered live.
func (sp *StorePool) liveAndDeadReplicas(
	rangeID roachpb.RangeID, repls []roachpb.ReplicaDescriptor,
) (liveReplicas, deadReplicas []roachpb.ReplicaDescriptor) {
	sp.detailsMu.Lock()
	defer sp.detailsMu.Unlock()

	now := sp.clock.PhysicalTime()
	for _, repl := range repls {
		detail := sp.getStoreDetailLocked(repl.StoreID)
		// Mark replica as dead if store is dead.
		status := detail.status(now, TimeUntilStoreDead.Get(&sp.st.SV), rangeID, sp.nodeLivenessFn)
		switch status {
		case storeStatusDead:
			deadReplicas = append(deadReplicas, repl)
		case storeStatusReplicaCorrupted:
			// Check whether the replica we're examining has been marked as dead.
			var corrupt bool
			for _, deadRepl := range detail.deadReplicas[rangeID] {
				if deadRepl.ReplicaID == repl.ReplicaID {
					corrupt = true
					break
				}
			}
			// This replica is the corrupt replica, so consider the store dead.
			if corrupt {
				deadReplicas = append(deadReplicas, repl)
			} else {
				// Otherwise, consider the store live.
				liveReplicas = append(liveReplicas, repl)
			}
		case storeStatusAvailable, storeStatusThrottled, storeStatusDecommissioning:
			// We count both available and throttled stores to be live for the
			// purpose of computing quorum.
			// We count decommissioning replicas to be alive because they are readable
			// and should be used for up-replication if necessary.
			liveReplicas = append(liveReplicas, repl)
		case storeStatusUnknown:
		// No-op.
		default:
			log.Fatalf(context.TODO(), "unknown store status %d", status)
		}
	}
	return
}

// stat provides a running sample size and running stats.
type stat struct {
	n, mean float64
}

// Update adds the specified value to the stat, augmenting the running stats.
func (s *stat) update(x float64) {
	s.n++
	s.mean += (x - s.mean) / s.n
}

// StoreList holds a list of store descriptors and associated count and used
// stats for those stores.
type StoreList struct {
	stores []roachpb.StoreDescriptor

	// candidateRanges tracks range count stats for stores that are eligible to
	// be rebalance targets (their used capacity percentage must be lower than
	// maxFractionUsedThreshold).
	candidateRanges stat

	// candidateLeases tracks range lease stats for stores that are eligible to
	// be rebalance targets.
	candidateLeases stat

	// candidateLogicalBytes tracks disk usage stats for stores that are eligible
	// to be rebalance targets.
	candidateLogicalBytes stat

	// candidateWritesPerSecond tracks writes-per-second stats for stores that are
	// eligible to be rebalance targets.
	candidateWritesPerSecond stat
}

// Generates a new store list based on the passed in descriptors. It will
// maintain the order of those descriptors.
func makeStoreList(descriptors []roachpb.StoreDescriptor) StoreList {
	sl := StoreList{stores: descriptors}
	for _, desc := range descriptors {
		if maxCapacityCheck(desc) {
			sl.candidateRanges.update(float64(desc.Capacity.RangeCount))
		}
		sl.candidateLeases.update(float64(desc.Capacity.LeaseCount))
		sl.candidateLogicalBytes.update(float64(desc.Capacity.LogicalBytes))
		sl.candidateWritesPerSecond.update(desc.Capacity.WritesPerSecond)
	}
	return sl
}

func (sl StoreList) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf,
		"  candidate: avg-ranges=%v avg-leases=%v avg-disk-usage=%v avg-writes-per-second=%v",
		sl.candidateRanges.mean,
		sl.candidateLeases.mean,
		humanizeutil.IBytes(int64(sl.candidateLogicalBytes.mean)),
		sl.candidateWritesPerSecond.mean)
	if len(sl.stores) > 0 {
		fmt.Fprintf(&buf, "\n")
	} else {
		fmt.Fprintf(&buf, " <no candidates>")
	}
	for _, desc := range sl.stores {
		fmt.Fprintf(&buf, "  %d: ranges=%d leases=%d disk-usage=%s writes-per-second=%.2f\n",
			desc.StoreID, desc.Capacity.RangeCount,
			desc.Capacity.LeaseCount, humanizeutil.IBytes(desc.Capacity.LogicalBytes), desc.Capacity.WritesPerSecond)
	}
	return buf.String()
}

// filter takes a store list and filters it using the passed in constraints. It
// maintains the original order of the passed in store list.
func (sl StoreList) filter(constraints []config.Constraints) StoreList {
	if len(constraints) == 0 {
		return sl
	}
	var filteredDescs []roachpb.StoreDescriptor
	for _, store := range sl.stores {
		if ok := constraintsCheck(store, constraints); ok {
			filteredDescs = append(filteredDescs, store)
		}
	}
	return makeStoreList(filteredDescs)
}

type storeFilter int

const (
	_ storeFilter = iota
	// storeFilterNone requests that the storeList include all live stores. Dead,
	// unknown, and corrupted stores are always excluded from the storeList.
	storeFilterNone
	// storeFilterThrottled requests that the returned store list additionally
	// exclude stores that have been throttled for declining a snapshot. (See
	// storePool.throttle for details.) Throttled stores should not be considered
	// for replica rebalancing, for example, but can still be considered for lease
	// rebalancing.
	storeFilterThrottled
)

// getStoreList returns a storeList that contains all active stores that contain
// the required attributes and their associated stats. The storeList is filtered
// according to the provided storeFilter. It also returns the total number of
// alive and throttled stores. The passed in rangeID is used to check for
// corrupted replicas.
func (sp *StorePool) getStoreList(
	rangeID roachpb.RangeID, filter storeFilter,
) (StoreList, int, int) {
	sp.detailsMu.RLock()
	defer sp.detailsMu.RUnlock()

	var storeIDs roachpb.StoreIDSlice
	for storeID := range sp.detailsMu.storeDetails {
		storeIDs = append(storeIDs, storeID)
	}
	return sp.getStoreListFromIDsRLocked(storeIDs, rangeID, filter)
}

// getStoreListFromIDs is the same function as getStoreList but only returns stores
// from the subset of passed in store IDs.
func (sp *StorePool) getStoreListFromIDs(
	storeIDs roachpb.StoreIDSlice, rangeID roachpb.RangeID, filter storeFilter,
) (StoreList, int, int) {
	sp.detailsMu.RLock()
	defer sp.detailsMu.RUnlock()
	return sp.getStoreListFromIDsRLocked(storeIDs, rangeID, filter)
}

// getStoreListFromIDsRLocked is the same function as getStoreList but requires
// that the detailsMU read lock is held.
func (sp *StorePool) getStoreListFromIDsRLocked(
	storeIDs roachpb.StoreIDSlice, rangeID roachpb.RangeID, filter storeFilter,
) (StoreList, int, int) {
	if sp.deterministic {
		sort.Sort(storeIDs)
	} else {
		shuffle.Shuffle(storeIDs)
	}

	var aliveStoreCount int
	var throttledStoreCount int
	var storeDescriptors []roachpb.StoreDescriptor

	now := sp.clock.PhysicalTime()
	for _, storeID := range storeIDs {
		detail := sp.detailsMu.storeDetails[storeID]
		switch s := detail.status(now, TimeUntilStoreDead.Get(&sp.st.SV), rangeID, sp.nodeLivenessFn); s {
		case storeStatusThrottled:
			aliveStoreCount++
			throttledStoreCount++
			if filter != storeFilterThrottled {
				storeDescriptors = append(storeDescriptors, *detail.desc)
			}
		case storeStatusReplicaCorrupted:
			aliveStoreCount++
		case storeStatusAvailable:
			aliveStoreCount++
			storeDescriptors = append(storeDescriptors, *detail.desc)
		case storeStatusDead, storeStatusUnknown, storeStatusDecommissioning:
			// Do nothing; this node cannot be used.
		default:
			panic(fmt.Sprintf("unknown store status: %d", s))
		}
	}
	return makeStoreList(storeDescriptors), aliveStoreCount, throttledStoreCount
}

type throttleReason int

const (
	_ throttleReason = iota
	throttleDeclined
	throttleFailed
)

// throttle informs the store pool that the given remote store declined a
// snapshot or failed to apply one, ensuring that it will not be considered
// for up-replication or rebalancing until after the configured timeout period
// has elapsed. Declined being true indicates that the remote store explicitly
// declined a snapshot.
func (sp *StorePool) throttle(reason throttleReason, storeID roachpb.StoreID) {
	sp.detailsMu.Lock()
	defer sp.detailsMu.Unlock()
	detail := sp.getStoreDetailLocked(storeID)

	// If a snapshot is declined, be it due to an error or because it was
	// rejected, we mark the store detail as having been declined so it won't
	// be considered as a candidate for new replicas until after the configured
	// timeout period has passed.
	switch reason {
	case throttleDeclined:
		timeout := declinedReservationsTimeout.Get(&sp.st.SV)
		detail.throttledUntil = sp.clock.PhysicalTime().Add(timeout)
		if log.V(2) {
			ctx := sp.AnnotateCtx(context.TODO())
			log.Infof(ctx, "snapshot declined, s%d will be throttled for %s until %s",
				storeID, timeout, detail.throttledUntil)
		}
	case throttleFailed:
		timeout := failedReservationsTimeout.Get(&sp.st.SV)
		detail.throttledUntil = sp.clock.PhysicalTime().Add(timeout)
		if log.V(2) {
			ctx := sp.AnnotateCtx(context.TODO())
			log.Infof(ctx, "snapshot failed, s%d will be throttled for %s until %s",
				storeID, timeout, detail.throttledUntil)
		}
	}
}

// getLocalities returns the localities for the provided replicas.
// TODO(bram): consider storing a full list of all node to node diversity
// scores for faster lookups.
func (sp *StorePool) getLocalities(
	replicas []roachpb.ReplicaDescriptor,
) map[roachpb.NodeID]roachpb.Locality {
	sp.localitiesMu.RLock()
	defer sp.localitiesMu.RUnlock()
	localities := make(map[roachpb.NodeID]roachpb.Locality)
	for _, replica := range replicas {
		if locality, ok := sp.localitiesMu.nodeLocalities[replica.NodeID]; ok {
			localities[replica.NodeID] = locality.locality
		} else {
			localities[replica.NodeID] = roachpb.Locality{}
		}
	}
	return localities
}

// getNodeLocalityString returns the locality information for the given node
// in its string format.
func (sp *StorePool) getNodeLocalityString(nodeID roachpb.NodeID) string {
	sp.localitiesMu.RLock()
	defer sp.localitiesMu.RUnlock()
	locality, ok := sp.localitiesMu.nodeLocalities[nodeID]
	if !ok {
		return ""
	}
	return locality.str
}
