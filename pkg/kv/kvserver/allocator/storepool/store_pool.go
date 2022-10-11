// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storepool

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

const (
	// TestTimeUntilStoreDead is the test value for TimeUntilStoreDead to
	// quickly mark stores as dead.
	TestTimeUntilStoreDead = 5 * time.Millisecond

	// TestTimeUntilStoreDeadOff is the test value for TimeUntilStoreDead that
	// prevents the store pool from marking stores as dead.
	TestTimeUntilStoreDeadOff = 24 * time.Hour
)

// FailedReservationsTimeout specifies a duration during which the local
// replicate queue will not consider stores which have failed a reservation a
// viable target.
var FailedReservationsTimeout = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"server.failed_reservation_timeout",
	"the amount of time to consider the store throttled for up-replication after a failed reservation call",
	5*time.Second,
	settings.NonNegativeDuration,
)

const timeAfterStoreSuspectSettingName = "server.time_after_store_suspect"

// TimeAfterStoreSuspect measures how long we consider a store suspect since
// it's last failure.
var TimeAfterStoreSuspect = settings.RegisterDurationSetting(
	settings.TenantWritable,
	timeAfterStoreSuspectSettingName,
	"the amount of time we consider a store suspect for after it fails a node liveness heartbeat."+
		" A suspect node would not receive any new replicas or lease transfers, but will keep the replicas it has.",
	30*time.Second,
	settings.NonNegativeDuration,
	func(v time.Duration) error {
		// We enforce a maximum value of 5 minutes for this settings, as setting this
		// to high may result in a prolonged period of unavailability as a recovered
		// store will not be able to acquire leases or replicas for a long time.
		const maxTimeAfterStoreSuspect = 5 * time.Minute
		if v > maxTimeAfterStoreSuspect {
			return errors.Errorf("cannot set %s to more than %v: %v",
				timeAfterStoreSuspectSettingName, maxTimeAfterStoreSuspect, v)
		}
		return nil
	},
)

const timeUntilStoreDeadSettingName = "server.time_until_store_dead"

// TimeUntilStoreDead wraps "server.time_until_store_dead".
var TimeUntilStoreDead = func() *settings.DurationSetting {
	s := settings.RegisterDurationSetting(
		settings.TenantWritable,
		timeUntilStoreDeadSettingName,
		"the time after which if there is no new gossiped information about a store, it is considered dead",
		5*time.Minute,
		func(v time.Duration) error {
			// Setting this to less than the interval for gossiping stores is a big
			// no-no, since this value is compared to the age of the most recent gossip
			// from each store to determine whether that store is live. Put a buffer of
			// 15 seconds on top to allow time for gossip to propagate.
			const minTimeUntilStoreDead = gossip.StoresInterval + 15*time.Second
			if v < minTimeUntilStoreDead {
				return errors.Errorf("cannot set %s to less than %v: %v",
					timeUntilStoreDeadSettingName, minTimeUntilStoreDead, v)
			}
			return nil
		},
	)
	s.SetVisibility(settings.Public)
	return s
}()

// The NodeCountFunc returns a count of the total number of nodes the user
// intends for their to be in the cluster. The count includes dead nodes, but
// not decommissioned nodes.
type NodeCountFunc func() int

// A NodeLivenessFunc accepts a node ID and current time and returns whether or
// not the node is live. A node is considered dead if its liveness record has
// expired by more than TimeUntilStoreDead.
type NodeLivenessFunc func(
	nid roachpb.NodeID, now time.Time, timeUntilStoreDead time.Duration,
) livenesspb.NodeLivenessStatus

// MakeStorePoolNodeLivenessFunc returns a function which determines
// the status of a node based on information provided by the specified
// NodeLiveness.
func MakeStorePoolNodeLivenessFunc(nodeLiveness *liveness.NodeLiveness) NodeLivenessFunc {
	return func(
		nodeID roachpb.NodeID, now time.Time, timeUntilStoreDead time.Duration,
	) livenesspb.NodeLivenessStatus {
		liveness, ok := nodeLiveness.GetLiveness(nodeID)
		if !ok {
			return livenesspb.NodeLivenessStatus_UNKNOWN
		}
		return LivenessStatus(liveness.Liveness, now, timeUntilStoreDead)
	}
}

// LivenessStatus returns a NodeLivenessStatus enumeration value for the
// provided Liveness based on the provided timestamp and threshold.
//
// See the note on IsLive() for considerations on what should be passed in as
// `now`.
//
// The timeline of the states that a liveness goes through as time passes after
// the respective liveness record is written is the following:
//
//	-----|-------LIVE---|------UNAVAILABLE---|------DEAD------------> time
//	     tWrite         tExp                 tExp+threshold
//
// Explanation:
//
//   - Let's say a node write its liveness record at tWrite. It sets the
//     Expiration field of the record as tExp=tWrite+livenessThreshold.
//     The node is considered LIVE (or DECOMMISSIONING or DRAINING).
//   - At tExp, the IsLive() method starts returning false. The state becomes
//     UNAVAILABLE (or stays DECOMMISSIONING or DRAINING).
//   - Once threshold passes, the node is considered DEAD (or DECOMMISSIONED).
//
// NB: There's a bit of discrepancy between what "Decommissioned" represents, as
// seen by NodeStatusLiveness, and what "Decommissioned" represents as
// understood by MembershipStatus. Currently it's possible for a live node, that
// was marked as fully decommissioned, to have a NodeLivenessStatus of
// "Decommissioning". This was kept this way for backwards compatibility, and
// ideally we should remove usage of NodeLivenessStatus altogether. See #50707
// for more details.
func LivenessStatus(
	l livenesspb.Liveness, now time.Time, deadThreshold time.Duration,
) livenesspb.NodeLivenessStatus {
	if l.IsDead(now, deadThreshold) {
		if !l.Membership.Active() {
			return livenesspb.NodeLivenessStatus_DECOMMISSIONED
		}
		return livenesspb.NodeLivenessStatus_DEAD
	}
	if l.IsLive(now) {
		if !l.Membership.Active() {
			return livenesspb.NodeLivenessStatus_DECOMMISSIONING
		}
		if l.Draining {
			return livenesspb.NodeLivenessStatus_DRAINING
		}
		return livenesspb.NodeLivenessStatus_LIVE
	}
	return livenesspb.NodeLivenessStatus_UNAVAILABLE
}

// StoreDetail groups together store-relevant details.
type StoreDetail struct {
	Desc *roachpb.StoreDescriptor
	// ThrottledUntil is when a throttled store can be considered available again
	// due to a failed or declined snapshot.
	ThrottledUntil time.Time
	// throttledBecause is set to the most recent reason for which a store was
	// marked as throttled.
	throttledBecause string
	// LastUpdatedTime is set when a store is first consulted and every time
	// gossip arrives for a store.
	LastUpdatedTime time.Time
	// LastUnavailable is set when it's detected that a store was unavailable,
	// i.e. failed liveness.
	LastUnavailable time.Time
	// LastAvailable is set when it's detected that a store was available,
	// i.e. we got a liveness heartbeat.
	LastAvailable time.Time
}

// isThrottled returns whether the store is currently throttled.
func (sd StoreDetail) isThrottled(now time.Time) bool {
	return sd.ThrottledUntil.After(now)
}

// isSuspect returns whether the store is currently suspect. We measure that by
// looking at the time it was last unavailable making sure we have not seen any
// failures for a period of time defined by StoreSuspectDuration.
func (sd StoreDetail) isSuspect(now time.Time, suspectDuration time.Duration) bool {
	return sd.LastUnavailable.Add(suspectDuration).After(now)
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
	// The store is alive and available.
	storeStatusAvailable
	// The store is decommissioning.
	storeStatusDecommissioning
	// The store failed it's liveness heartbeat recently and is considered
	// suspect. Consequently, stores always move from `storeStatusUnknown`
	// (indicating a node that has a non-live node liveness record) to
	// `storeStatusSuspect`.
	storeStatusSuspect
	// The store is alive but is currently marked as draining, so it is not a
	// candidate for lease transfers or replica rebalancing.
	storeStatusDraining
)

func (sd *StoreDetail) status(
	now time.Time, threshold time.Duration, nl NodeLivenessFunc, suspectDuration time.Duration,
) storeStatus {
	// During normal operation, we expect the state transitions for stores to look like the following:
	//
	//                                           Successful heartbeats
	//                                          throughout the suspect
	//       +-----------------------+                 duration
	//       | storeStatusAvailable  |<-+------------------------------------+
	//       +-----------------------+  |                                    |
	//                                  |                                    |
	//                                  |                         +--------------------+
	//                                  |                         | storeStatusSuspect |
	//      +---------------------------+                         +--------------------+
	//      |      Failed liveness                                           ^
	//      |         heartbeat                                              |
	//      |                                                                |
	//      |                                                                |
	//      |  +----------------------+                                      |
	//      +->|  storeStatusUnknown  |--------------------------------------+
	//         +----------------------+          Successful liveness
	//                                                heartbeat
	//
	// The store is considered dead if it hasn't been updated via gossip
	// within the liveness threshold. Note that LastUpdatedTime is set
	// when the store detail is created and will have a non-zero value
	// even before the first gossip arrives for a store.
	deadAsOf := sd.LastUpdatedTime.Add(threshold)
	if now.After(deadAsOf) {
		// Wipe out the lastAvailable timestamp, so that once a node comes back
		// from the dead we dont consider it suspect.
		sd.LastAvailable = time.Time{}
		return storeStatusDead
	}
	// If there's no descriptor (meaning no gossip ever arrived for this
	// store), return unavailable.
	if sd.Desc == nil {
		return storeStatusUnknown
	}

	// Even if the store has been updated via gossip, we still rely on
	// the node liveness to determine whether it is considered live.
	//
	// Store statuses checked in the following order:
	// dead -> decommissioning -> unknown -> draining -> suspect -> available.
	switch nl(sd.Desc.Node.NodeID, now, threshold) {
	case livenesspb.NodeLivenessStatus_DEAD, livenesspb.NodeLivenessStatus_DECOMMISSIONED:
		return storeStatusDead
	case livenesspb.NodeLivenessStatus_DECOMMISSIONING:
		return storeStatusDecommissioning
	case livenesspb.NodeLivenessStatus_UNAVAILABLE:
		// We don't want to suspect a node on startup or when it's first added to a
		// cluster, because we dont know its liveness yet.
		if !sd.LastAvailable.IsZero() {
			sd.LastUnavailable = now
		}
		return storeStatusUnknown
	case livenesspb.NodeLivenessStatus_UNKNOWN:
		return storeStatusUnknown
	case livenesspb.NodeLivenessStatus_DRAINING:
		// Wipe out the lastAvailable timestamp, so if this node comes back after a
		// graceful restart it will not be considered as suspect. This is best effort
		// and we may not see a store in this state. To help with that we perform
		// a similar clear of lastAvailable on a DEAD store.
		sd.LastAvailable = time.Time{}
		return storeStatusDraining
	}

	if sd.isThrottled(now) {
		return storeStatusThrottled
	}

	if sd.isSuspect(now, suspectDuration) {
		return storeStatusSuspect
	}
	sd.LastAvailable = now
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
//
// TODO(irfansharif): Mediate access through a thin interface.
type StorePool struct {
	log.AmbientContext
	St *cluster.Settings // TODO(irfansharif): Shouldn't need to be exported.

	Clock          *hlc.Clock
	Gossip         *gossip.Gossip // TODO(irfansharif): Shouldn't need to be exported.
	nodeCountFn    NodeCountFunc
	NodeLivenessFn NodeLivenessFunc
	startTime      time.Time
	Deterministic  bool
	// We use separate mutexes for storeDetails and nodeLocalities because the
	// nodeLocalities map is used in the critical code path of Replica.Send()
	// and we'd rather not block that on something less important accessing
	// storeDetails.
	DetailsMu struct {
		syncutil.RWMutex
		StoreDetails map[roachpb.StoreID]*StoreDetail
	}
	localitiesMu struct {
		syncutil.RWMutex
		nodeLocalities map[roachpb.NodeID]localityWithString
	}

	// IsStoreReadyForRoutineReplicaTransfer returns true iff the store's node is
	// live (as indicated by its `NodeLivenessStatus`) and thus a legal candidate
	// to receive a replica. This is defined as a closure reference here instead
	// of a regular method so it can be overridden in tests.
	//
	// NB: What this method aims to capture is distinct from "dead" nodes. Nodes
	// are classified as "dead" if they haven't successfully heartbeat their
	// liveness record in the last `server.time_until_store_dead` seconds.
	//
	// Functionally, the distinction is that we simply avoid transferring replicas
	// to "non-ready" nodes (i.e. nodes that _currently_ have a non-live
	// `NodeLivenessStatus`), whereas we _actively move replicas off of "dead"
	// nodes_.
	IsStoreReadyForRoutineReplicaTransfer func(context.Context, roachpb.StoreID) bool
}

// NewStorePool creates a StorePool and registers the store updating callback
// with gossip.
func NewStorePool(
	ambient log.AmbientContext,
	st *cluster.Settings,
	g *gossip.Gossip,
	clock *hlc.Clock,
	nodeCountFn NodeCountFunc,
	nodeLivenessFn NodeLivenessFunc,
	deterministic bool,
) *StorePool {
	sp := &StorePool{
		AmbientContext: ambient,
		St:             st,
		Clock:          clock,
		Gossip:         g,
		nodeCountFn:    nodeCountFn,
		NodeLivenessFn: nodeLivenessFn,
		startTime:      clock.PhysicalTime(),
		Deterministic:  deterministic,
	}
	sp.IsStoreReadyForRoutineReplicaTransfer = sp.isStoreReadyForRoutineReplicaTransferInternal
	sp.DetailsMu.StoreDetails = make(map[roachpb.StoreID]*StoreDetail)
	sp.localitiesMu.nodeLocalities = make(map[roachpb.NodeID]localityWithString)

	// Enable redundant callbacks for the store keys because we use these
	// callbacks as a clock to determine when a store was last updated even if it
	// hasn't otherwise changed.
	storeRegex := gossip.MakePrefixPattern(gossip.KeyStoreDescPrefix)
	g.RegisterCallback(storeRegex, sp.storeGossipUpdate, gossip.Redundant)

	return sp
}

func (sp *StorePool) String() string {
	sp.DetailsMu.RLock()
	defer sp.DetailsMu.RUnlock()

	ids := make(roachpb.StoreIDSlice, 0, len(sp.DetailsMu.StoreDetails))
	for id := range sp.DetailsMu.StoreDetails {
		ids = append(ids, id)
	}
	sort.Sort(ids)

	var buf bytes.Buffer
	now := sp.Clock.Now().GoTime()
	timeUntilStoreDead := TimeUntilStoreDead.Get(&sp.St.SV)
	timeAfterStoreSuspect := TimeAfterStoreSuspect.Get(&sp.St.SV)

	for _, id := range ids {
		detail := sp.DetailsMu.StoreDetails[id]
		fmt.Fprintf(&buf, "%d", id)
		status := detail.status(now, timeUntilStoreDead, sp.NodeLivenessFn, timeAfterStoreSuspect)
		if status != storeStatusAvailable {
			fmt.Fprintf(&buf, " (status=%d)", status)
		}
		if detail.Desc != nil {
			fmt.Fprintf(&buf, ": range-count=%d fraction-used=%.2f",
				detail.Desc.Capacity.RangeCount, detail.Desc.Capacity.FractionUsed())
		}
		throttled := detail.ThrottledUntil.Sub(now)
		if throttled > 0 {
			fmt.Fprintf(&buf, " [throttled=%.1fs]", throttled.Seconds())
		}
		_, _ = buf.WriteString("\n")
	}
	return buf.String()
}

// storeGossipUpdate is the Gossip callback used to keep the StorePool up to date.
func (sp *StorePool) storeGossipUpdate(_ string, content roachpb.Value) {
	var storeDesc roachpb.StoreDescriptor
	if err := content.GetProto(&storeDesc); err != nil {
		ctx := sp.AnnotateCtx(context.TODO())
		log.Errorf(ctx, "%v", err)
		return
	}

	sp.DetailsMu.Lock()
	detail := sp.GetStoreDetailLocked(storeDesc.StoreID)
	detail.Desc = &storeDesc
	detail.LastUpdatedTime = sp.Clock.PhysicalTime()
	sp.DetailsMu.Unlock()

	sp.localitiesMu.Lock()
	sp.localitiesMu.nodeLocalities[storeDesc.Node.NodeID] =
		localityWithString{storeDesc.Node.Locality, storeDesc.Node.Locality.String()}
	sp.localitiesMu.Unlock()
}

// UpdateLocalStoreAfterRebalance is used to update the local copy of the
// target store immediately after a replica addition or removal.
func (sp *StorePool) UpdateLocalStoreAfterRebalance(
	storeID roachpb.StoreID,
	rangeUsageInfo allocator.RangeUsageInfo,
	changeType roachpb.ReplicaChangeType,
) {
	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()
	detail := *sp.GetStoreDetailLocked(storeID)
	if detail.Desc == nil {
		// We don't have this store yet (this is normal when we're
		// starting up and don't have full information from the gossip
		// network). We can't update the local store at this time.
		return
	}
	switch changeType {
	case roachpb.ADD_VOTER, roachpb.ADD_NON_VOTER:
		detail.Desc.Capacity.RangeCount++
		detail.Desc.Capacity.LogicalBytes += rangeUsageInfo.LogicalBytes
		detail.Desc.Capacity.WritesPerSecond += rangeUsageInfo.WritesPerSecond
	case roachpb.REMOVE_VOTER, roachpb.REMOVE_NON_VOTER:
		detail.Desc.Capacity.RangeCount--
		if detail.Desc.Capacity.LogicalBytes <= rangeUsageInfo.LogicalBytes {
			detail.Desc.Capacity.LogicalBytes = 0
		} else {
			detail.Desc.Capacity.LogicalBytes -= rangeUsageInfo.LogicalBytes
		}
		if detail.Desc.Capacity.WritesPerSecond <= rangeUsageInfo.WritesPerSecond {
			detail.Desc.Capacity.WritesPerSecond = 0
		} else {
			detail.Desc.Capacity.WritesPerSecond -= rangeUsageInfo.WritesPerSecond
		}
	default:
		return
	}
	sp.DetailsMu.StoreDetails[storeID] = &detail
}

// UpdateLocalStoreAfterRelocate is used to update the local copy of the
// previous and new replica stores immediately after a successful relocate
// range.
func (sp *StorePool) UpdateLocalStoreAfterRelocate(
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
	oldVoters, oldNonVoters []roachpb.ReplicaDescriptor,
	localStore roachpb.StoreID,
	rangeQPS float64,
) {
	if len(voterTargets) < 1 {
		return
	}
	leaseTarget := voterTargets[0]
	sp.UpdateLocalStoresAfterLeaseTransfer(localStore, leaseTarget.StoreID, rangeQPS)

	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()

	updateTargets := func(targets []roachpb.ReplicationTarget) {
		for _, target := range targets {
			if toDetail := sp.GetStoreDetailLocked(target.StoreID); toDetail != nil {
				toDetail.Desc.Capacity.RangeCount++
			}
		}
	}
	updatePrevious := func(previous []roachpb.ReplicaDescriptor) {
		for _, old := range previous {
			if toDetaill := sp.GetStoreDetailLocked(old.StoreID); toDetaill != nil {
				toDetaill.Desc.Capacity.RangeCount--
			}
		}
	}

	updateTargets(voterTargets)
	updateTargets(nonVoterTargets)
	updatePrevious(oldVoters)
	updatePrevious(oldNonVoters)
}

// UpdateLocalStoresAfterLeaseTransfer is used to update the local copies of the
// involved store descriptors immediately after a lease transfer.
func (sp *StorePool) UpdateLocalStoresAfterLeaseTransfer(
	from roachpb.StoreID, to roachpb.StoreID, rangeQPS float64,
) {
	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()

	fromDetail := *sp.GetStoreDetailLocked(from)
	if fromDetail.Desc != nil {
		fromDetail.Desc.Capacity.LeaseCount--
		if fromDetail.Desc.Capacity.QueriesPerSecond < rangeQPS {
			fromDetail.Desc.Capacity.QueriesPerSecond = 0
		} else {
			fromDetail.Desc.Capacity.QueriesPerSecond -= rangeQPS
		}
		sp.DetailsMu.StoreDetails[from] = &fromDetail
	}

	toDetail := *sp.GetStoreDetailLocked(to)
	if toDetail.Desc != nil {
		toDetail.Desc.Capacity.LeaseCount++
		toDetail.Desc.Capacity.QueriesPerSecond += rangeQPS
		sp.DetailsMu.StoreDetails[to] = &toDetail
	}
}

// newStoreDetail makes a new StoreDetail struct. It sets index to be -1 to
// ensure that it will be processed by a queue immediately.
func newStoreDetail() *StoreDetail {
	return &StoreDetail{}
}

// GetStores returns information on all the stores with descriptor in the pool.
// Stores without descriptor (a node that didn't come up yet after a cluster
// restart) will not be part of the returned set.
func (sp *StorePool) GetStores() map[roachpb.StoreID]roachpb.StoreDescriptor {
	sp.DetailsMu.RLock()
	defer sp.DetailsMu.RUnlock()
	stores := make(map[roachpb.StoreID]roachpb.StoreDescriptor, len(sp.DetailsMu.StoreDetails))
	for _, s := range sp.DetailsMu.StoreDetails {
		if s.Desc != nil {
			stores[s.Desc.StoreID] = *s.Desc
		}
	}
	return stores
}

// GetStoreDetailLocked returns the store detail for the given storeID.
// The lock must be held *in write mode* even though this looks like a
// read-only method.
func (sp *StorePool) GetStoreDetailLocked(storeID roachpb.StoreID) *StoreDetail {
	detail, ok := sp.DetailsMu.StoreDetails[storeID]
	if !ok {
		// We don't have this store yet (this is normal when we're
		// starting up and don't have full information from the gossip
		// network). The first time this occurs, presume the store is
		// alive, but start the clock so it will become dead if enough
		// time passes without updates from gossip.
		detail = newStoreDetail()
		detail.LastUpdatedTime = sp.startTime
		sp.DetailsMu.StoreDetails[storeID] = detail
	}
	return detail
}

// GetStoreDescriptor returns the latest store descriptor for the given
// storeID.
func (sp *StorePool) GetStoreDescriptor(storeID roachpb.StoreID) (roachpb.StoreDescriptor, bool) {
	sp.DetailsMu.RLock()
	defer sp.DetailsMu.RUnlock()

	if detail, ok := sp.DetailsMu.StoreDetails[storeID]; ok && detail.Desc != nil {
		return *detail.Desc, true
	}
	return roachpb.StoreDescriptor{}, false
}

// DecommissioningReplicas filters out replicas on decommissioning node/store
// from the provided repls and returns them in a slice.
func (sp *StorePool) DecommissioningReplicas(
	repls []roachpb.ReplicaDescriptor,
) (decommissioningReplicas []roachpb.ReplicaDescriptor) {
	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()

	// NB: We use clock.Now().GoTime() instead of clock.PhysicalTime() is order to
	// take clock signals from remote nodes into consideration.
	now := sp.Clock.Now().GoTime()
	timeUntilStoreDead := TimeUntilStoreDead.Get(&sp.St.SV)
	timeAfterStoreSuspect := TimeAfterStoreSuspect.Get(&sp.St.SV)

	for _, repl := range repls {
		detail := sp.GetStoreDetailLocked(repl.StoreID)
		switch detail.status(now, timeUntilStoreDead, sp.NodeLivenessFn, timeAfterStoreSuspect) {
		case storeStatusDecommissioning:
			decommissioningReplicas = append(decommissioningReplicas, repl)
		}
	}
	return
}

// ClusterNodeCount returns the number of nodes that are possible allocation
// targets. This includes dead nodes, but not decommissioning or decommissioned
// nodes.
func (sp *StorePool) ClusterNodeCount() int {
	return sp.nodeCountFn()
}

// IsDead determines if a store is dead. It will return an error if the store is
// not found in the store pool or the status is unknown. If the store is not dead,
// it returns the time to death.
func (sp *StorePool) IsDead(storeID roachpb.StoreID) (bool, time.Duration, error) {
	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()

	sd, ok := sp.DetailsMu.StoreDetails[storeID]
	if !ok {
		return false, 0, errors.Errorf("store %d was not found", storeID)
	}
	// NB: We use clock.Now().GoTime() instead of clock.PhysicalTime() is order to
	// take clock signals from remote nodes into consideration.
	now := sp.Clock.Now().GoTime()
	timeUntilStoreDead := TimeUntilStoreDead.Get(&sp.St.SV)

	deadAsOf := sd.LastUpdatedTime.Add(timeUntilStoreDead)
	if now.After(deadAsOf) {
		return true, 0, nil
	}
	// If there's no descriptor (meaning no gossip ever arrived for this
	// store), return unavailable.
	if sd.Desc == nil {
		return false, 0, errors.Errorf("store %d status unknown, cant tell if it's dead or alive", storeID)
	}
	return false, deadAsOf.Sub(now), nil
}

// IsUnknown returns true if the given store's status is `storeStatusUnknown`
// (i.e. it just failed a liveness heartbeat and we cannot ascertain its
// liveness or deadness at the moment) or an error if the store is not found in
// the pool.
func (sp *StorePool) IsUnknown(storeID roachpb.StoreID) (bool, error) {
	status, err := sp.storeStatus(storeID)
	if err != nil {
		return false, err
	}
	return status == storeStatusUnknown, nil
}

// IsDraining returns true if the given store's status is `storeStatusDraining`
// or an error if the store is not found in the pool.
func (sp *StorePool) IsDraining(storeID roachpb.StoreID) (bool, error) {
	status, err := sp.storeStatus(storeID)
	if err != nil {
		return false, err
	}
	return status == storeStatusDraining, nil
}

// IsLive returns true if the node is considered alive by the store pool or an error
// if the store is not found in the pool.
func (sp *StorePool) IsLive(storeID roachpb.StoreID) (bool, error) {
	status, err := sp.storeStatus(storeID)
	if err != nil {
		return false, err
	}
	return status == storeStatusAvailable, nil
}

func (sp *StorePool) storeStatus(storeID roachpb.StoreID) (storeStatus, error) {
	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()

	sd, ok := sp.DetailsMu.StoreDetails[storeID]
	if !ok {
		return storeStatusUnknown, errors.Errorf("store %d was not found", storeID)
	}
	// NB: We use clock.Now().GoTime() instead of clock.PhysicalTime() is order to
	// take clock signals from remote nodes into consideration.
	now := sp.Clock.Now().GoTime()
	timeUntilStoreDead := TimeUntilStoreDead.Get(&sp.St.SV)
	timeAfterStoreSuspect := TimeAfterStoreSuspect.Get(&sp.St.SV)
	return sd.status(now, timeUntilStoreDead, sp.NodeLivenessFn, timeAfterStoreSuspect), nil
}

// LiveAndDeadReplicas divides the provided repls slice into two slices: the
// first for live replicas, and the second for dead replicas.
//
// - Replicas for which liveness or deadness cannot be ascertained
// (storeStatusUnknown) are excluded from the returned slices.
//
// - Replicas on decommissioning node/store are considered live.
//
// - If `includeSuspectAndDrainingStores` is true, stores that are marked
// suspect (i.e. stores that have failed a liveness heartbeat in the recent
// past), and stores that are marked as draining are considered live. Otherwise,
// they are excluded from the returned slices.
func (sp *StorePool) LiveAndDeadReplicas(
	repls []roachpb.ReplicaDescriptor, includeSuspectAndDrainingStores bool,
) (liveReplicas, deadReplicas []roachpb.ReplicaDescriptor) {
	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()

	now := sp.Clock.Now().GoTime()
	timeUntilStoreDead := TimeUntilStoreDead.Get(&sp.St.SV)
	timeAfterStoreSuspect := TimeAfterStoreSuspect.Get(&sp.St.SV)

	for _, repl := range repls {
		detail := sp.GetStoreDetailLocked(repl.StoreID)
		// Mark replica as dead if store is dead.
		status := detail.status(now, timeUntilStoreDead, sp.NodeLivenessFn, timeAfterStoreSuspect)
		switch status {
		case storeStatusDead:
			deadReplicas = append(deadReplicas, repl)
		case storeStatusAvailable, storeStatusThrottled, storeStatusDecommissioning:
			// We count both available and throttled stores to be live for the
			// purpose of computing quorum.
			// We count decommissioning replicas to be alive because they are readable
			// and should be used for up-replication if necessary.
			liveReplicas = append(liveReplicas, repl)
		case storeStatusUnknown:
		// No-op.
		case storeStatusSuspect, storeStatusDraining:
			if includeSuspectAndDrainingStores {
				liveReplicas = append(liveReplicas, repl)
			}
		default:
			log.Fatalf(context.TODO(), "unknown store status %d", status)
		}
	}
	return
}

// Stat provides a running sample size and running stats.
type Stat struct {
	n, Mean float64
}

// Update adds the specified value to the Stat, augmenting the running stats.
func (s *Stat) update(x float64) {
	s.n++
	s.Mean += (x - s.Mean) / s.n
}

// StoreList holds a list of store descriptors and associated count and used
// stats for those stores.
type StoreList struct {
	Stores []roachpb.StoreDescriptor

	// CandidateRanges tracks range count stats for Stores that are eligible to
	// be rebalance targets (their used capacity percentage must be lower than
	// maxFractionUsedThreshold).
	CandidateRanges Stat

	// CandidateLeases tracks range lease stats for Stores that are eligible to
	// be rebalance targets.
	CandidateLeases Stat

	// candidateLogicalBytes tracks disk usage stats for Stores that are eligible
	// to be rebalance targets.
	candidateLogicalBytes Stat

	// CandidateQueriesPerSecond tracks queries-per-second stats for Stores that
	// are eligible to be rebalance targets.
	CandidateQueriesPerSecond Stat

	// candidateWritesPerSecond tracks writes-per-second stats for Stores that are
	// eligible to be rebalance targets.
	candidateWritesPerSecond Stat

	// candidateWritesPerSecond tracks L0 sub-level stats for Stores that are
	// eligible to be rebalance targets.
	CandidateL0Sublevels Stat
}

// MakeStoreList constructs a new store list based on the passed in descriptors.
// It will maintain the order of those descriptors.
func MakeStoreList(descriptors []roachpb.StoreDescriptor) StoreList {
	sl := StoreList{Stores: descriptors}
	for _, desc := range descriptors {
		if allocator.MaxCapacityCheck(desc) {
			sl.CandidateRanges.update(float64(desc.Capacity.RangeCount))
		}
		sl.CandidateLeases.update(float64(desc.Capacity.LeaseCount))
		sl.candidateLogicalBytes.update(float64(desc.Capacity.LogicalBytes))
		sl.CandidateQueriesPerSecond.update(desc.Capacity.QueriesPerSecond)
		sl.candidateWritesPerSecond.update(desc.Capacity.WritesPerSecond)
		sl.CandidateL0Sublevels.update(float64(desc.Capacity.L0Sublevels))
	}
	return sl
}

func (sl StoreList) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf,
		"  candidate: avg-ranges=%v avg-leases=%v avg-disk-usage=%v avg-queries-per-second=%v",
		sl.CandidateRanges.Mean,
		sl.CandidateLeases.Mean,
		humanizeutil.IBytes(int64(sl.candidateLogicalBytes.Mean)),
		sl.CandidateQueriesPerSecond.Mean,
	)
	if len(sl.Stores) > 0 {
		fmt.Fprintf(&buf, "\n")
	} else {
		fmt.Fprintf(&buf, " <no candidates>")
	}
	for _, desc := range sl.Stores {
		fmt.Fprintf(&buf, "  %d: ranges=%d leases=%d disk-usage=%s queries-per-second=%.2f l0-sublevels=%d\n",
			desc.StoreID, desc.Capacity.RangeCount,
			desc.Capacity.LeaseCount, humanizeutil.IBytes(desc.Capacity.LogicalBytes),
			desc.Capacity.QueriesPerSecond,
			desc.Capacity.L0Sublevels,
		)
	}
	return buf.String()
}

// ExcludeInvalid takes a store list and removes Stores that would be explicitly invalid
// under the given set of constraints. It maintains the original order of the
// passed in store list.
func (sl StoreList) ExcludeInvalid(constraints []roachpb.ConstraintsConjunction) StoreList {
	if len(constraints) == 0 {
		return sl
	}
	var filteredDescs []roachpb.StoreDescriptor
	for _, store := range sl.Stores {
		if ok := allocator.IsStoreValid(store, constraints); ok {
			filteredDescs = append(filteredDescs, store)
		}
	}
	return MakeStoreList(filteredDescs)
}

// ToMap returns the set of known stores as a map keyed by the store ID, with
// the value being the store descriptor.
func (sl StoreList) ToMap() map[roachpb.StoreID]*roachpb.StoreDescriptor {
	storeMap := make(map[roachpb.StoreID]*roachpb.StoreDescriptor)
	for i := range sl.Stores {
		storeMap[sl.Stores[i].StoreID] = &sl.Stores[i]
	}
	return storeMap
}

// StoreFilter is one of StoreFilter{None,Throttled,Suspect}, controlling what
// stores are excluded from the storeList.
type StoreFilter int

const (
	_ StoreFilter = iota
	// StoreFilterNone requests that the storeList include all live stores. Dead,
	// unknown, and corrupted stores are always excluded from the storeList.
	StoreFilterNone
	// StoreFilterThrottled requests that the returned store list additionally
	// exclude stores that have been throttled for declining a snapshot. (See
	// storePool.throttle for details.) Throttled stores should not be considered
	// for replica rebalancing, for example, but can still be considered for lease
	// rebalancing.
	StoreFilterThrottled
	// StoreFilterSuspect requests that the returned store list additionally
	// exclude stores that have been suspected as unhealthy. We dont want unhealthy
	// stores to be considered for rebalancing or for lease transfers. i.e. we dont
	// actively shift leases or replicas away from them, but we dont allow them to
	// get any new ones until they get better.
	StoreFilterSuspect
)

// ThrottledStoreReasons is the set of reasons why stores have been throttled.
type ThrottledStoreReasons []string

// GetStoreList returns a storeList that contains all active stores that contain
// the required attributes and their associated stats. The storeList is filtered
// according to the provided storeFilter. It also returns the total number of
// alive stores and a list of throttled stores with a reason for why they're
// throttled.
func (sp *StorePool) GetStoreList(filter StoreFilter) (StoreList, int, ThrottledStoreReasons) {
	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()

	var storeIDs roachpb.StoreIDSlice
	for storeID := range sp.DetailsMu.StoreDetails {
		storeIDs = append(storeIDs, storeID)
	}
	return sp.getStoreListFromIDsLocked(storeIDs, filter)
}

// GetStoreListFromIDs is the same function as GetStoreList but only returns stores
// from the subset of passed in store IDs.
func (sp *StorePool) GetStoreListFromIDs(
	storeIDs roachpb.StoreIDSlice, filter StoreFilter,
) (StoreList, int, ThrottledStoreReasons) {
	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()
	return sp.getStoreListFromIDsLocked(storeIDs, filter)
}

// getStoreListFromIDsRLocked is the same function as GetStoreList but requires
// that the detailsMU read lock is held.
func (sp *StorePool) getStoreListFromIDsLocked(
	storeIDs roachpb.StoreIDSlice, filter StoreFilter,
) (StoreList, int, ThrottledStoreReasons) {
	if sp.Deterministic {
		sort.Sort(storeIDs)
	} else {
		shuffle.Shuffle(storeIDs)
	}

	var aliveStoreCount int
	var throttled ThrottledStoreReasons
	var storeDescriptors []roachpb.StoreDescriptor

	now := sp.Clock.Now().GoTime()
	timeUntilStoreDead := TimeUntilStoreDead.Get(&sp.St.SV)
	timeAfterStoreSuspect := TimeAfterStoreSuspect.Get(&sp.St.SV)

	for _, storeID := range storeIDs {
		detail, ok := sp.DetailsMu.StoreDetails[storeID]
		if !ok {
			// Do nothing; this store is not in the StorePool.
			continue
		}
		switch s := detail.status(now, timeUntilStoreDead, sp.NodeLivenessFn, timeAfterStoreSuspect); s {
		case storeStatusThrottled:
			aliveStoreCount++
			throttled = append(throttled, detail.throttledBecause)
			if filter != StoreFilterThrottled {
				storeDescriptors = append(storeDescriptors, *detail.Desc)
			}
		case storeStatusAvailable:
			aliveStoreCount++
			storeDescriptors = append(storeDescriptors, *detail.Desc)
		case storeStatusDraining:
			throttled = append(throttled, fmt.Sprintf("s%d: draining", storeID))
		case storeStatusSuspect:
			aliveStoreCount++
			throttled = append(throttled, fmt.Sprintf("s%d: suspect", storeID))
			if filter != StoreFilterThrottled && filter != StoreFilterSuspect {
				storeDescriptors = append(storeDescriptors, *detail.Desc)
			}
		case storeStatusDead, storeStatusUnknown, storeStatusDecommissioning:
			// Do nothing; this store cannot be used.
		default:
			panic(fmt.Sprintf("unknown store status: %d", s))
		}
	}
	return MakeStoreList(storeDescriptors), aliveStoreCount, throttled
}

// ThrottleReason encodes the reason for throttling a given store.
type ThrottleReason int

const (
	_ ThrottleReason = iota
	// ThrottleFailed is used when we're throttling as a result of a failed
	// operation.
	ThrottleFailed
)

// Throttle informs the store pool that the given remote store declined a
// snapshot or failed to apply one, ensuring that it will not be considered
// for up-replication or rebalancing until after the configured timeout period
// has elapsed. Declined being true indicates that the remote store explicitly
// declined a snapshot.
func (sp *StorePool) Throttle(reason ThrottleReason, why string, storeID roachpb.StoreID) {
	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()
	detail := sp.GetStoreDetailLocked(storeID)
	detail.throttledBecause = why

	// If a snapshot is declined, we mark the store detail as having been declined
	// so it won't be considered as a candidate for new replicas until after the
	// configured timeout period has passed.
	switch reason {
	case ThrottleFailed:
		timeout := FailedReservationsTimeout.Get(&sp.St.SV)
		detail.ThrottledUntil = sp.Clock.PhysicalTime().Add(timeout)
		if log.V(2) {
			ctx := sp.AnnotateCtx(context.TODO())
			log.Infof(ctx, "snapshot failed (%s), s%d will be throttled for %s until %s",
				why, storeID, timeout, detail.ThrottledUntil)
		}
	default:
		log.Warningf(sp.AnnotateCtx(context.TODO()), "unknown throttle reason %v", reason)
	}
}

// GetLocalitiesByStore returns the localities for the provided replicas. In
// this case we consider the node part of the failure domain and add it to
// the locality data.
func (sp *StorePool) GetLocalitiesByStore(
	replicas []roachpb.ReplicaDescriptor,
) map[roachpb.StoreID]roachpb.Locality {
	sp.localitiesMu.RLock()
	defer sp.localitiesMu.RUnlock()
	localities := make(map[roachpb.StoreID]roachpb.Locality)
	for _, replica := range replicas {
		nodeTier := roachpb.Tier{Key: "node", Value: replica.NodeID.String()}
		if locality, ok := sp.localitiesMu.nodeLocalities[replica.NodeID]; ok {
			localities[replica.StoreID] = locality.locality.AddTier(nodeTier)
		} else {
			localities[replica.StoreID] = roachpb.Locality{
				Tiers: []roachpb.Tier{nodeTier},
			}
		}
	}
	return localities
}

// GetLocalitiesByNode returns the localities for the provided replicas. In this
// case we only consider the locality by node, where the node itself is not
// part of the failure domain.
// TODO(bram): consider storing a full list of all node to node diversity
// scores for faster lookups.
func (sp *StorePool) GetLocalitiesByNode(
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

// GetNodeLocalityString returns the locality information for the given node
// in its string format.
func (sp *StorePool) GetNodeLocalityString(nodeID roachpb.NodeID) string {
	sp.localitiesMu.RLock()
	defer sp.localitiesMu.RUnlock()
	locality, ok := sp.localitiesMu.nodeLocalities[nodeID]
	if !ok {
		return ""
	}
	return locality.str
}

func (sp *StorePool) isStoreReadyForRoutineReplicaTransferInternal(
	ctx context.Context, targetStoreID roachpb.StoreID,
) bool {
	status, err := sp.storeStatus(targetStoreID)
	if err != nil {
		return false
	}
	switch status {
	case storeStatusThrottled, storeStatusAvailable:
		log.VEventf(ctx, 3,
			"s%d is a live target, candidate for rebalancing", targetStoreID)
		return true
	case storeStatusDead, storeStatusUnknown, storeStatusDecommissioning, storeStatusSuspect, storeStatusDraining:
		log.VEventf(ctx, 3,
			"not considering non-live store s%d (%v)", targetStoreID, status)
		return false
	default:
		panic(fmt.Sprintf("unknown store status: %d", status))
	}
}

// TestingGetStoreList exposes getStoreList for testing only, but with a
// hardcoded storeFilter of storeFilterNone.
func (sp *StorePool) TestingGetStoreList() (StoreList, int, int) {
	list, available, throttled := sp.GetStoreList(StoreFilterNone)
	return list, available, len(throttled)
}

// TestingStores returns a copy of sl.stores.
func (sl *StoreList) TestingStores() []roachpb.StoreDescriptor {
	stores := make([]roachpb.StoreDescriptor, len(sl.Stores))
	copy(stores, sl.Stores)
	return stores
}
