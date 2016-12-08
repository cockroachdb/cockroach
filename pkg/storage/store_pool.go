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
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

	// defaultFailedReservationsTimeout is the amount of time to consider the
	// store throttled for up-replication after a failed reservation call.
	defaultFailedReservationsTimeout = 5 * time.Second

	// defaultDeclinedReservationsTimeout is the amount of time to consider the
	// store throttled for up-replication after a reservation was declined.
	defaultDeclinedReservationsTimeout = 0 * time.Second
)

// A NodeLivenessFunc accepts a node ID, current time and threshold before
// a node is considered dead and returns whether or not the node is live.
type NodeLivenessFunc func(roachpb.NodeID, time.Time, time.Duration) bool

// MakeStorePoolNodeLivenessFunc returns a function which determines
// whether or not a node is alive based on information provided by
// the specified NodeLiveness.
func MakeStorePoolNodeLivenessFunc(nodeLiveness *NodeLiveness) NodeLivenessFunc {
	return func(nodeID roachpb.NodeID, now time.Time, threshold time.Duration) bool {
		// Mark the store dead if the node it's on is not live.
		liveness, err := nodeLiveness.GetLiveness(nodeID)
		if err != nil {
			return false
		}
		deadAsOf := liveness.Expiration.GoTime().Add(threshold)
		return now.Before(deadAsOf)
	}
}

// StorePoolNodeLivenessTrue is a NodeLivenessFunc which always returns true.
func StorePoolNodeLivenessTrue(_ roachpb.NodeID, _ time.Time, _ time.Duration) bool {
	return true
}

type storeDetail struct {
	desc *roachpb.StoreDescriptor
	// throttledUntil is when an throttled store can be considered available
	// again due to a failed or declined Reserve RPC.
	throttledUntil  time.Time
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
	// The store is not yet available or has been timed out.
	storeStatusDead storeStatus = iota
	// The store is alive but it is throttled.
	storeStatusThrottled
	// The store is alive but a replica for the same rangeID was recently
	// discovered to be corrupt.
	storeStatusReplicaCorrupted
	// The store is alive and available.
	storeStatusAvailable
)

func (sd *storeDetail) isDead(
	now time.Time, threshold time.Duration, livenessFn NodeLivenessFunc,
) bool {
	// The store is considered dead if it hasn't been updated via gossip
	// within the liveness threshold.
	deadAsOf := sd.lastUpdatedTime.Add(threshold)
	if now.After(deadAsOf) {
		return true
	}
	// If there's no descriptor (meaning no gossip ever arrived for this
	// store), we can't check the node liveness, so presume this node live.
	if sd.desc == nil {
		return false
	}
	// Even if the store has been updated via gossip, we still rely on
	// the node liveness to determine whether it is considered live.
	return !livenessFn(sd.desc.Node.NodeID, now, threshold)
}

// status returns the current status of the store.
func (sd *storeDetail) status(
	now time.Time, threshold time.Duration, rangeID roachpb.RangeID, nl NodeLivenessFunc,
) storeStatus {
	// The store must be alive and it must have a descriptor to be considered
	// alive.
	if sd.isDead(now, threshold, nl) || sd.desc == nil {
		return storeStatusDead
	}

	// The store must not be throttled from recently declining a reservation.
	if sd.isThrottled(now) {
		return storeStatusThrottled
	}

	// The store must not have a corrupt replica on it.
	if len(sd.deadReplicas[rangeID]) > 0 {
		return storeStatusReplicaCorrupted
	}

	return storeStatusAvailable
}

// StorePool maintains a list of all known stores in the cluster and
// information on their health.
type StorePool struct {
	log.AmbientContext

	clock                       *hlc.Clock
	timeUntilStoreDead          time.Duration
	failedReservationsTimeout   time.Duration
	declinedReservationsTimeout time.Duration
	deterministic               bool
	mu                          struct {
		syncutil.RWMutex
		nodeLiveness   NodeLivenessFunc
		storeDetails   map[roachpb.StoreID]*storeDetail
		nodeLocalities map[roachpb.NodeID]roachpb.Locality
	}
}

// NewStorePool creates a StorePool and registers the store updating callback
// with gossip.
func NewStorePool(
	ambient log.AmbientContext,
	g *gossip.Gossip,
	clock *hlc.Clock,
	nodeLiveness NodeLivenessFunc,
	timeUntilStoreDead time.Duration,
	deterministic bool,
) *StorePool {
	sp := &StorePool{
		AmbientContext:     ambient,
		clock:              clock,
		timeUntilStoreDead: timeUntilStoreDead,
		failedReservationsTimeout: envutil.EnvOrDefaultDuration("COCKROACH_FAILED_RESERVATION_TIMEOUT",
			defaultFailedReservationsTimeout),
		declinedReservationsTimeout: envutil.EnvOrDefaultDuration("COCKROACH_DECLINED_RESERVATION_TIMEOUT",
			defaultDeclinedReservationsTimeout),
		deterministic: deterministic,
	}
	sp.mu.nodeLiveness = nodeLiveness
	sp.mu.storeDetails = make(map[roachpb.StoreID]*storeDetail)
	sp.mu.nodeLocalities = make(map[roachpb.NodeID]roachpb.Locality)

	storeRegex := gossip.MakePrefixPattern(gossip.KeyStorePrefix)
	g.RegisterCallback(storeRegex, sp.storeGossipUpdate)
	deadReplicasRegex := gossip.MakePrefixPattern(gossip.KeyDeadReplicasPrefix)
	g.RegisterCallback(deadReplicasRegex, sp.deadReplicasGossipUpdate)

	return sp
}

func (sp *StorePool) String() string {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	ids := make(roachpb.StoreIDSlice, 0, len(sp.mu.storeDetails))
	for id := range sp.mu.storeDetails {
		ids = append(ids, id)
	}
	sort.Sort(ids)

	var buf bytes.Buffer
	now := sp.clock.Now().GoTime()

	for _, id := range ids {
		detail := sp.mu.storeDetails[id]
		fmt.Fprintf(&buf, "%d", id)
		if detail.isDead(now, sp.timeUntilStoreDead, sp.mu.nodeLiveness) {
			_, _ = buf.WriteString("*")
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

	sp.mu.Lock()
	defer sp.mu.Unlock()
	detail := sp.getStoreDetailLocked(storeDesc.StoreID)
	detail.desc = &storeDesc
	detail.lastUpdatedTime = sp.clock.Now().GoTime()
	sp.mu.nodeLocalities[storeDesc.Node.NodeID] = storeDesc.Node.Locality
}

// deadReplicasGossipUpdate is the gossip callback used to keep the StorePool up to date.
func (sp *StorePool) deadReplicasGossipUpdate(_ string, content roachpb.Value) {
	var replicas roachpb.StoreDeadReplicas
	if err := content.GetProto(&replicas); err != nil {
		ctx := sp.AnnotateCtx(context.TODO())
		log.Error(ctx, err)
		return
	}

	sp.mu.Lock()
	defer sp.mu.Unlock()
	detail := sp.getStoreDetailLocked(replicas.StoreID)
	deadReplicas := make(map[roachpb.RangeID][]roachpb.ReplicaDescriptor)
	for _, r := range replicas.Replicas {
		deadReplicas[r.RangeID] = append(deadReplicas[r.RangeID], r.Replica)
	}
	detail.deadReplicas = deadReplicas
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
	detail, ok := sp.mu.storeDetails[storeID]
	if !ok {
		// We don't have this store yet (this is normal when we're
		// starting up and don't have full information from the gossip
		// network). The first time this occurs, presume the store is
		// alive, but start the clock so it will become dead if enough
		// time passes without updates from gossip.
		detail = newStoreDetail()
		detail.lastUpdatedTime = sp.clock.Now().GoTime()
		sp.mu.storeDetails[storeID] = detail
	}
	return detail
}

// getStoreDescriptor returns the latest store descriptor for the given
// storeID.
func (sp *StorePool) getStoreDescriptor(storeID roachpb.StoreID) (roachpb.StoreDescriptor, bool) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	if detail, ok := sp.mu.storeDetails[storeID]; ok && detail.desc != nil {
		return *detail.desc, true
	}
	return roachpb.StoreDescriptor{}, false
}

// deadReplicas returns any replicas from the supplied slice that are
// located on dead stores or dead replicas for the provided rangeID.
func (sp *StorePool) deadReplicas(
	rangeID roachpb.RangeID, repls []roachpb.ReplicaDescriptor,
) []roachpb.ReplicaDescriptor {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	var deadReplicas []roachpb.ReplicaDescriptor
	now := sp.clock.Now().GoTime()
outer:
	for _, repl := range repls {
		detail := sp.getStoreDetailLocked(repl.StoreID)
		// Mark replica as dead if store is dead.
		if detail.isDead(now, sp.timeUntilStoreDead, sp.mu.nodeLiveness) {
			deadReplicas = append(deadReplicas, repl)
			continue
		}

		for _, deadRepl := range detail.deadReplicas[rangeID] {
			if deadRepl.ReplicaID == repl.ReplicaID {
				deadReplicas = append(deadReplicas, repl)
				continue outer
			}
		}
	}
	return deadReplicas
}

// stat provides a running sample size and running stats.
type stat struct {
	n, mean, s float64
}

// Update adds the specified value to the stat, augmenting the running stats.
func (s *stat) update(x float64) {
	s.n++
	oldMean := s.mean
	s.mean += (x - s.mean) / s.n

	// Update variable used to calculate running standard deviation. See: Knuth
	// TAOCP, vol 2, 3rd ed, page 232.
	s.s = s.s + (x-oldMean)*(x-s.mean)
}

// StoreList holds a list of store descriptors and associated count and used
// stats for those stores.
type StoreList struct {
	stores []roachpb.StoreDescriptor

	// candidateCount tracks range count stats for stores that are eligible to
	// be rebalance targets (their used capacity percentage must be lower than
	// maxFractionUsedThreshold).
	candidateCount stat

	// candidateLeases tracks range lease stats for stores that are eligible to
	// be rebalance targets.
	candidateLeases stat
}

// Generates a new store list based on the passed in descriptors. It will
// maintain the order of those descriptors.
func makeStoreList(descriptors []roachpb.StoreDescriptor) StoreList {
	sl := StoreList{stores: descriptors}
	for _, desc := range descriptors {
		if desc.Capacity.FractionUsed() <= maxFractionUsedThreshold {
			sl.candidateCount.update(float64(desc.Capacity.RangeCount))
		}
		sl.candidateLeases.update(float64(desc.Capacity.LeaseCount))
	}
	return sl
}

func (sl StoreList) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "  candidate: avg-ranges=%v avg-leases=%v\n",
		sl.candidateCount.mean, sl.candidateLeases.mean)
	for _, desc := range sl.stores {
		fmt.Fprintf(&buf, "  %d: ranges=%d leases=%d fraction-used=%.2f\n",
			desc.StoreID, desc.Capacity.RangeCount,
			desc.Capacity.LeaseCount, desc.Capacity.FractionUsed())
	}
	return buf.String()
}

// filter takes a store list and filters it using the passed in constraints. It
// maintains the original order of the passed in store list.
func (sl StoreList) filter(constraints config.Constraints) StoreList {
	if len(constraints.Constraints) == 0 {
		return sl
	}
	var filteredDescs []roachpb.StoreDescriptor
storeLoop:
	for _, store := range sl.stores {
		m := map[string]struct{}{}
		for _, s := range store.CombinedAttrs().Attrs {
			m[s] = struct{}{}
		}
		for _, c := range constraints.Constraints {
			if _, ok := m[c.Value]; !ok {
				continue storeLoop
			}
		}
		filteredDescs = append(filteredDescs, store)
	}

	return makeStoreList(filteredDescs)
}

// getStoreList returns a storeList that contains all active stores that
// contain the required attributes and their associated stats. It also returns
// the total number of alive and throttled stores.
func (sp *StorePool) getStoreList(rangeID roachpb.RangeID) (StoreList, int, int) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	var storeIDs roachpb.StoreIDSlice
	for storeID := range sp.mu.storeDetails {
		storeIDs = append(storeIDs, storeID)
	}

	if sp.deterministic {
		sort.Sort(storeIDs)
	} else {
		shuffle.Shuffle(storeIDs)
	}

	var aliveStoreCount int
	var throttledStoreCount int
	var storeDescriptors []roachpb.StoreDescriptor

	now := sp.clock.Now().GoTime()
	for _, storeID := range storeIDs {
		detail := sp.mu.storeDetails[storeID]
		switch detail.status(now, sp.timeUntilStoreDead, rangeID, sp.mu.nodeLiveness) {
		case storeStatusThrottled:
			aliveStoreCount++
			throttledStoreCount++
		case storeStatusReplicaCorrupted:
			aliveStoreCount++
		case storeStatusAvailable:
			aliveStoreCount++
			storeDescriptors = append(storeDescriptors, *detail.desc)
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
func (sp *StorePool) throttle(reason throttleReason, toStoreID roachpb.StoreID) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	detail := sp.getStoreDetailLocked(toStoreID)
	ctx := sp.AnnotateCtx(context.TODO())

	// If a snapshot is declined, be it due to an error or because it was
	// rejected, we mark the store detail as having been declined so it won't
	// be considered as a candidate for new replicas until after the configured
	// timeout period has passed.
	switch reason {
	case throttleDeclined:
		detail.throttledUntil = sp.clock.Now().GoTime().Add(sp.declinedReservationsTimeout)
		if log.V(2) {
			log.Infof(ctx, "snapshot declined, store:%s will be throttled for %s until %s",
				toStoreID, sp.declinedReservationsTimeout, detail.throttledUntil)
		}
	case throttleFailed:
		detail.throttledUntil = sp.clock.Now().GoTime().Add(sp.failedReservationsTimeout)
		if log.V(2) {
			log.Infof(ctx, "snapshot failed, store:%s will be throttled for %s until %s",
				toStoreID, sp.failedReservationsTimeout, detail.throttledUntil)
		}
	}
}

// getNodeLocalities returns the localities for the provided replicas.
// TODO(bram): consider storing a full list of all node to node diversity
// scores for faster lookups.
func (sp *StorePool) getNodeLocalities(
	replicas []roachpb.ReplicaDescriptor,
) map[roachpb.NodeID]roachpb.Locality {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	localities := make(map[roachpb.NodeID]roachpb.Locality)
	for _, replica := range replicas {
		if locality, ok := sp.mu.nodeLocalities[replica.NodeID]; ok {
			localities[replica.NodeID] = locality
		} else {
			localities[replica.NodeID] = roachpb.Locality{}
		}
	}
	return localities
}
