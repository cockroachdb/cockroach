// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storepool

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
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
	"github.com/cockroachdb/redact"
)

// FailedReservationsTimeout specifies a duration during which the local
// replicate queue will not consider stores which have failed a reservation a
// viable target.
var FailedReservationsTimeout = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"server.failed_reservation_timeout",
	"the amount of time to consider the store throttled for up-replication after a failed reservation call",
	5*time.Second,
	settings.NonNegativeDuration,
	settings.WithName("server.failed_reservation.timeout"),
)

// The NodeCountFunc returns a count of the total number of nodes the user
// intends for their to be in the cluster. The count includes dead nodes, but
// not decommissioned nodes.
type NodeCountFunc func() int

// A NodeLivenessFunc accepts a node ID and current time and returns whether or
// not the node is live. A node is considered dead if its liveness record has
// expired by more than TimeUntilNodeDead.
type NodeLivenessFunc func(nid roachpb.NodeID) livenesspb.NodeLivenessStatus

// MakeStorePoolNodeLivenessFunc returns a function which determines
// the status of a node based on information provided by the specified
// NodeLiveness.
func MakeStorePoolNodeLivenessFunc(nodeLiveness *liveness.NodeLiveness) NodeLivenessFunc {
	return func(nodeID roachpb.NodeID) livenesspb.NodeLivenessStatus {
		return nodeLiveness.GetNodeVitalityFromCache(nodeID).LivenessStatus()
	}
}

// StoreDetail groups together store-relevant details.
type StoreDetail struct {
	Desc *roachpb.StoreDescriptor
	// ThrottledUntil is when a throttled store can be considered available again
	// due to a failed or declined snapshot.
	ThrottledUntil hlc.Timestamp
	// throttledBecause is set to the most recent reason for which a store was
	// marked as throttled.
	throttledBecause string
	// LastUpdatedTime is set when a store is first consulted and every time
	// gossip arrives for a store.
	LastUpdatedTime hlc.Timestamp
	// LastUnavailable is set when it's detected that a store was unavailable,
	// i.e. failed liveness.
	LastUnavailable hlc.Timestamp
}

// storeStatus is the current status of a store.
type storeStatus int

// These are the possible values for a storeStatus.
const (
	_ storeStatus = iota
	// The store's node is not live or no gossip has been received from
	// the store for more than the timeUntilNodeDead threshold.
	storeStatusDead
	// The store isn't available because it hasn't gossiped yet. This
	// status lasts until either gossip is received from the store or
	// the timeUntilNodeDead threshold has passed, at which point its
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

func (ss storeStatus) String() string {
	if ss < storeStatusDead || ss > storeStatusDraining {
		panic(fmt.Sprintf("unknown store status: %d", ss))
	}
	return [...]string{"",
		"dead", "unknown", "throttled", "available",
		"decommissioning", "suspect", "draining"}[ss]
}

// SafeValue implements the redact.SafeValue interface.
func (ss storeStatus) SafeValue() {}

func (sd *StoreDetail) status(
	now hlc.Timestamp,
	deadThreshold time.Duration,
	nl NodeLivenessFunc,
	suspectDuration time.Duration,
) storeStatus {
	// During normal operation, we expect the state transitions for stores to look like the following:
	//
	//      +-----------------------+
	//   +- |  storeStatusUnknown   |
	//   |  +-----------------------+             Successful heartbeats
	//   |                                        throughout the suspect
	//   |      +-----------------------+         duration
	//   +----->| storeStatusAvailable  |<-+---------------------------+
	//          +-----------------------+  |                           |
	//                                     |                   +--------------------+
	//                                     |                   | storeStatusSuspect |
	//      +------------------------------+                   +--------------------+
	//      |      Failed liveness                                     ^
	//      |      heartbeat                                           |
	//      |                                                          |
	//      |  +-------------------------+                             |
	//      +->|  storeStatusUnavailable |-----------------------------+
	//         +-------------------------+    Successful liveness
	//                                        heartbeat
	//

	// The store is considered dead if it hasn't been updated via gossip
	// within the liveness threshold. Note that LastUpdatedTime is set
	// when the store detail is created and will have a non-zero value
	// even before the first gossip arrives for a store.
	deadAsOf := sd.LastUpdatedTime.AddDuration(deadThreshold)
	if now.After(deadAsOf) {
		sd.LastUnavailable = now
		return storeStatusDead
	}
	// If there's no descriptor (meaning no gossip ever arrived for this
	// store), return unavailable.
	if sd.Desc == nil {
		return storeStatusUnknown
	}

	// Even if the store has been updated via gossip, we still rely on
	// the node liveness to determine whether it is considered available.
	//
	// Store statuses checked in the following order:
	// dead -> decommissioning -> unknown -> draining -> suspect -> available.
	switch nl(sd.Desc.Node.NodeID) {
	case livenesspb.NodeLivenessStatus_DEAD, livenesspb.NodeLivenessStatus_DECOMMISSIONED:
		sd.LastUnavailable = now
		return storeStatusDead
	case livenesspb.NodeLivenessStatus_DECOMMISSIONING:
		return storeStatusDecommissioning
	case livenesspb.NodeLivenessStatus_UNAVAILABLE:
		sd.LastUnavailable = now
		return storeStatusUnknown
	case livenesspb.NodeLivenessStatus_UNKNOWN:
		return storeStatusUnknown
	case livenesspb.NodeLivenessStatus_DRAINING:
		sd.LastUnavailable = now
		return storeStatusDraining
	}

	// A store is throttled if it has missed receiving snapshots recently.
	if sd.ThrottledUntil.After(now) {
		return storeStatusThrottled
	}

	// Check whether the store is currently suspect. We measure that by
	// looking at the time it was last unavailable making sure we have not seen any
	// failures for a period of time defined by StoreSuspectDuration.
	if sd.LastUnavailable.AddDuration(suspectDuration).After(now) {
		return storeStatusSuspect
	}

	// Clear out the LastUnavailable once we return available status.
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

// CapacityChangeFn is a function which may be called on capacity changes, by
// the storepool.
type CapacityChangeFn func(
	storeID roachpb.StoreID,
	old, cur roachpb.StoreCapacity,
)

// AllocatorStorePool provides an interface for use by the allocator to a list
// of all known stores in the cluster and information on their health.
type AllocatorStorePool interface {
	fmt.Stringer
	redact.SafeFormatter

	// ClusterNodeCount returns the number of nodes that are possible allocation
	// targets.
	// See comment on StorePool.ClusterNodeCount().
	ClusterNodeCount() int

	// IsDeterministic returns true iff the pool is configured to be deterministic.
	IsDeterministic() bool

	// IsStoreReadyForRoutineReplicaTransfer returns true iff the store's node is
	// live (as indicated by its `NodeLivenessStatus`) and thus a legal candidate
	// to receive a replica.
	// See comment on StorePool.IsStoreReadyForRoutineReplicaTransfer(..).
	IsStoreReadyForRoutineReplicaTransfer(ctx context.Context, targetStoreID roachpb.StoreID) bool

	// Clock returns the store pool's clock.
	// TODO(sarkesian): If possible, this should be removed.
	Clock() *hlc.Clock

	// DecommissioningReplicas selects the replicas on decommissioning
	// node/stores from the provided list.
	DecommissioningReplicas(repls []roachpb.ReplicaDescriptor) []roachpb.ReplicaDescriptor

	// GetLocalitiesByNode returns the localities for the provided replicas by NodeID.
	// See comment on StorePool.GetLocalitiesByNode(..).
	GetLocalitiesByNode(replicas []roachpb.ReplicaDescriptor) map[roachpb.NodeID]roachpb.Locality

	// GetLocalitiesByStore returns the localities for the provided replicas by StoreID.
	// See comment on StorePool.GetLocalitiesByStore(..).
	GetLocalitiesByStore(replicas []roachpb.ReplicaDescriptor) map[roachpb.StoreID]roachpb.Locality

	// GetStores returns information on all the stores with descriptor in the pool.
	// See comment on StorePool.GetStores().
	GetStores() map[roachpb.StoreID]roachpb.StoreDescriptor

	// GetStoreDescriptor returns the latest store descriptor for the given
	// storeID.
	GetStoreDescriptor(storeID roachpb.StoreID) (roachpb.StoreDescriptor, bool)

	// GetStoreList returns a storeList of active stores based on a filter.
	// See comment on StorePool.GetStoreList(..).
	GetStoreList(filter StoreFilter) (StoreList, int, ThrottledStoreReasons)

	// GetStoreListFromIDs is the same function as GetStoreList but only returns stores
	// from the subset of passed in store IDs.
	GetStoreListFromIDs(
		storeIDs roachpb.StoreIDSlice,
		filter StoreFilter,
	) (StoreList, int, ThrottledStoreReasons)

	// GetStoreListForTargets is the same as GetStoreList, but only returns stores
	// from the subset of stores present in the passed in replication targets,
	// converting to a StoreList.
	GetStoreListForTargets(
		candidates []roachpb.ReplicationTarget,
		filter StoreFilter,
	) (StoreList, int, ThrottledStoreReasons)

	// LiveAndDeadReplicas divides the provided repls slice into two slices: the
	// first for live replicas, and the second for dead replicas.
	// See comment on StorePool.LiveAndDeadReplicas(..).
	LiveAndDeadReplicas(
		repls []roachpb.ReplicaDescriptor,
		includeSuspectAndDrainingStores bool,
	) (liveReplicas, deadReplicas []roachpb.ReplicaDescriptor)

	// UpdateLocalStoreAfterRebalance is used to update the local copy of the
	// target store immediately after a replica addition or removal.
	UpdateLocalStoreAfterRebalance(
		storeID roachpb.StoreID,
		rangeUsageInfo allocator.RangeUsageInfo,
		changeType roachpb.ReplicaChangeType,
	)

	// UpdateLocalStoresAfterLeaseTransfer is used to update the local copies of the
	// involved store descriptors immediately after a lease transfer.
	UpdateLocalStoresAfterLeaseTransfer(
		from roachpb.StoreID,
		to roachpb.StoreID,
		rangeUsageInfo allocator.RangeUsageInfo,
	)

	// UpdateLocalStoreAfterRelocate is used to update the local copy of the
	// previous and new replica stores immediately after a successful relocate
	// range.
	UpdateLocalStoreAfterRelocate(
		voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
		oldVoters, oldNonVoters []roachpb.ReplicaDescriptor,
		localStore roachpb.StoreID,
		rangeUsageInfo allocator.RangeUsageInfo,
	)

	// SetOnCapacityChange installs a callback to be called when any store
	// capacity changes in the storepool. This currently doesn't consider local
	// updates (UpdateLocalStoreAfterRelocate, UpdateLocalStoreAfterRebalance,
	// UpdateLocalStoresAfterLeaseTransfer) as capacity changes.
	SetOnCapacityChange(fn CapacityChangeFn)
}

// StorePool maintains a list of all known stores in the cluster and
// information on their health.
type StorePool struct {
	log.AmbientContext
	st *cluster.Settings

	clock          *hlc.Clock
	gossip         *gossip.Gossip
	nodeCountFn    NodeCountFunc
	NodeLivenessFn NodeLivenessFunc
	startTime      hlc.Timestamp
	deterministic  bool

	// We use separate mutexes for storeDetails and nodeLocalities because the
	// nodeLocalities map is used in the critical code path of Replica.Send()
	// and we'd rather not block that on something less important accessing
	// storeDetails.
	// NB: Exported for use in tests and allocator simulator.
	DetailsMu struct {
		syncutil.RWMutex
		StoreDetails map[roachpb.StoreID]*StoreDetail
	}
	localitiesMu struct {
		syncutil.RWMutex
		nodeLocalities map[roachpb.NodeID]localityWithString
	}

	changeMu struct {
		syncutil.Mutex
		onChange []CapacityChangeFn
	}

	// OverrideIsStoreReadyForRoutineReplicaTransferFn, if set, is used in
	// IsStoreReadyForRoutineReplicaTransfer. This is defined as a closure reference here instead
	// of a regular method so it can be overridden in tests.
	// TODO(sarkesian): Consider moving to a TestingKnobs struct.
	OverrideIsStoreReadyForRoutineReplicaTransferFn func(context.Context, roachpb.StoreID) bool
}

var _ AllocatorStorePool = &StorePool{}

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
		st:             st,
		clock:          clock,
		gossip:         g,
		nodeCountFn:    nodeCountFn,
		NodeLivenessFn: nodeLivenessFn,
		startTime:      clock.Now(),
		deterministic:  deterministic,
	}
	sp.DetailsMu.StoreDetails = make(map[roachpb.StoreID]*StoreDetail)
	sp.localitiesMu.nodeLocalities = make(map[roachpb.NodeID]localityWithString)
	sp.changeMu.onChange = []CapacityChangeFn{}

	// Enable redundant callbacks for the store keys because we use these
	// callbacks as a clock to determine when a store was last updated even if it
	// hasn't otherwise changed.
	storeRegex := gossip.MakePrefixPattern(gossip.KeyStoreDescPrefix)
	g.RegisterCallback(storeRegex, sp.storeGossipUpdate, gossip.Redundant)

	return sp
}

func (sp *StorePool) String() string {
	return redact.StringWithoutMarkers(sp)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (sp *StorePool) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Print(sp.statusString(sp.NodeLivenessFn))
}

func (sp *StorePool) statusString(nl NodeLivenessFunc) redact.RedactableString {
	sp.DetailsMu.RLock()
	defer sp.DetailsMu.RUnlock()

	ids := make(roachpb.StoreIDSlice, 0, len(sp.DetailsMu.StoreDetails))
	for id := range sp.DetailsMu.StoreDetails {
		ids = append(ids, id)
	}
	sort.Sort(ids)

	var buf redact.StringBuilder
	now := sp.clock.Now()
	timeUntilNodeDead := liveness.TimeUntilNodeDead.Get(&sp.st.SV)
	timeAfterNodeSuspect := liveness.TimeAfterNodeSuspect.Get(&sp.st.SV)

	for _, id := range ids {
		detail := sp.DetailsMu.StoreDetails[id]
		buf.Print(id)
		status := detail.status(now, timeUntilNodeDead, nl, timeAfterNodeSuspect)
		if status != storeStatusAvailable {
			buf.Printf(" (status=%s)", status)
		}
		if detail.Desc != nil {
			buf.Printf(": range-count=%d fraction-used=%.2f",
				detail.Desc.Capacity.RangeCount,
				detail.Desc.Capacity.FractionUsed())
		}
		if detail.ThrottledUntil.After(now) {
			buf.Printf(" [throttled=%v]", humanizeutil.Duration(
				detail.ThrottledUntil.GoTime().Sub(now.GoTime())))
		}
		buf.SafeRune('\n')
	}
	return buf.RedactableString()
}

// storeGossipUpdate is the Gossip callback used to keep the StorePool up to date.
func (sp *StorePool) storeGossipUpdate(_ string, content roachpb.Value) {
	var storeDesc roachpb.StoreDescriptor

	if err := content.GetProto(&storeDesc); err != nil {
		ctx := sp.AnnotateCtx(context.TODO())
		log.Errorf(ctx, "%v", err)
		return
	}

	sp.storeDescriptorUpdate(storeDesc)
}

// storeDescriptorUpdate takes a store descriptor and updates the corresponding
// details for the store in the storepool.
func (sp *StorePool) storeDescriptorUpdate(storeDesc roachpb.StoreDescriptor) {
	// We keep copies of the capacity and storeID to pass into the
	// capacityChanged callback.
	var oldCapacity roachpb.StoreCapacity
	storeID := storeDesc.StoreID
	curCapacity := storeDesc.Capacity

	now := sp.clock.Now()

	sp.DetailsMu.Lock()
	detail := sp.GetStoreDetailLocked(storeID)
	if detail.Desc != nil {
		oldCapacity = detail.Desc.Capacity
	}
	detail.Desc = &storeDesc
	detail.LastUpdatedTime = now
	sp.DetailsMu.Unlock()

	sp.localitiesMu.Lock()
	sp.localitiesMu.nodeLocalities[storeDesc.Node.NodeID] =
		localityWithString{storeDesc.Node.Locality, storeDesc.Node.Locality.String()}
	sp.localitiesMu.Unlock()

	if oldCapacity != curCapacity {
		sp.capacityChanged(storeID, curCapacity, oldCapacity)
	}
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
	// Only apply the raft cpu delta on rebalance. This estimate assumes that
	// the raft cpu usage is approximately equal across replicas for a range.
	switch changeType {
	case roachpb.ADD_VOTER, roachpb.ADD_NON_VOTER:
		detail.Desc.Capacity.RangeCount++
		detail.Desc.Capacity.LogicalBytes += rangeUsageInfo.LogicalBytes
		detail.Desc.Capacity.WritesPerSecond += rangeUsageInfo.WritesPerSecond
		if detail.Desc.Capacity.CPUPerSecond >= 0 {
			detail.Desc.Capacity.CPUPerSecond += rangeUsageInfo.RaftCPUNanosPerSecond
		}
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
		// When CPU attribution is unsupported, the store will set the
		// CPUPerSecond of its store capacity to be -1.
		if detail.Desc.Capacity.CPUPerSecond >= 0 {
			if detail.Desc.Capacity.CPUPerSecond <= rangeUsageInfo.RaftCPUNanosPerSecond {
				detail.Desc.Capacity.CPUPerSecond = 0
			} else {
				detail.Desc.Capacity.CPUPerSecond -= rangeUsageInfo.RaftCPUNanosPerSecond
			}
		}
	default:
		return
	}
	sp.DetailsMu.StoreDetails[storeID] = &detail
}

// UpdateLocalStoreAfterRelocate is used to update the local copy of the
// previous and new replica stores immediately after a successful relocate
// range.
//
// TODO(kvoli): We do not update the logical bytes or writes per second here.
// Once #91593 is in, update these methods to instead take a general purpose
// representation. This is less relevant at the moment.
func (sp *StorePool) UpdateLocalStoreAfterRelocate(
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
	oldVoters, oldNonVoters []roachpb.ReplicaDescriptor,
	localStore roachpb.StoreID,
	rangeUsageInfo allocator.RangeUsageInfo,
) {
	if len(voterTargets) < 1 {
		return
	}
	leaseTarget := voterTargets[0]
	sp.UpdateLocalStoresAfterLeaseTransfer(localStore, leaseTarget.StoreID, rangeUsageInfo)

	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()

	// Only apply the raft cpu delta on rebalance. This estimate assumes that
	// the raft cpu usage is approximately equal across replicas for a range.
	// TODO(kvoli): Separate into LH vs Replica, similar to the comment on
	// range_usage_info.
	updateTargets := func(targets []roachpb.ReplicationTarget) {
		for _, target := range targets {
			if toDetail := sp.GetStoreDetailLocked(target.StoreID); toDetail.Desc != nil {
				toDetail.Desc.Capacity.RangeCount++
				if toDetail.Desc.Capacity.CPUPerSecond >= 0 {
					toDetail.Desc.Capacity.CPUPerSecond += rangeUsageInfo.RaftCPUNanosPerSecond
				}
			}
		}
	}
	updatePrevious := func(previous []roachpb.ReplicaDescriptor) {
		for _, old := range previous {
			if toDetail := sp.GetStoreDetailLocked(old.StoreID); toDetail.Desc != nil {
				toDetail.Desc.Capacity.RangeCount--
				// When CPU attribution is unsupported, the store will set the
				// CPUPerSecond of its store capacity to be -1.
				if toDetail.Desc.Capacity.CPUPerSecond < 0 {
					continue
				}
				if toDetail.Desc.Capacity.CPUPerSecond <= rangeUsageInfo.RaftCPUNanosPerSecond {
					toDetail.Desc.Capacity.CPUPerSecond = 0
				} else {
					toDetail.Desc.Capacity.CPUPerSecond -= rangeUsageInfo.RaftCPUNanosPerSecond
				}
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
	from roachpb.StoreID, to roachpb.StoreID, rangeUsageInfo allocator.RangeUsageInfo,
) {
	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()

	fromDetail := *sp.GetStoreDetailLocked(from)
	if fromDetail.Desc != nil {
		fromDetail.Desc.Capacity.LeaseCount--
		if fromDetail.Desc.Capacity.QueriesPerSecond < rangeUsageInfo.QueriesPerSecond {
			fromDetail.Desc.Capacity.QueriesPerSecond = 0
		} else {
			fromDetail.Desc.Capacity.QueriesPerSecond -= rangeUsageInfo.QueriesPerSecond
		}
		// When CPU attribution is unsupported, the store will set the
		// CPUPerSecond of its store capacity to be -1.
		if fromDetail.Desc.Capacity.CPUPerSecond >= 0 {
			// Only apply the request cpu (leaseholder + follower-reads) delta on
			// transfers. Note this does not correctly account for follower reads
			// remaining on the prior leaseholder after lease transfer. Instead,
			// only a cpu delta specific to the lease should be applied.
			if fromDetail.Desc.Capacity.CPUPerSecond <= rangeUsageInfo.RequestCPUNanosPerSecond {
				fromDetail.Desc.Capacity.CPUPerSecond = 0
			} else {
				fromDetail.Desc.Capacity.CPUPerSecond -= rangeUsageInfo.RequestCPUNanosPerSecond
			}
		}

		sp.DetailsMu.StoreDetails[from] = &fromDetail
	}

	toDetail := *sp.GetStoreDetailLocked(to)
	if toDetail.Desc != nil {
		toDetail.Desc.Capacity.LeaseCount++
		toDetail.Desc.Capacity.QueriesPerSecond += rangeUsageInfo.QueriesPerSecond
		// When CPU attribution is unsupported, the store will set the
		// CPUPerSecond of its store capacity to be -1.
		if toDetail.Desc.Capacity.CPUPerSecond >= 0 {
			toDetail.Desc.Capacity.CPUPerSecond += rangeUsageInfo.RequestCPUNanosPerSecond
		}
		sp.DetailsMu.StoreDetails[to] = &toDetail
	}
}

// newStoreDetail makes a new StoreDetail struct.
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

// GetStoreDetailLocked returns the store detail for the given storeID. The
// lock must be held *in write mode* even though this looks like a read-only
// method. The store detail returned is a mutable reference.
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
	return sp.decommissioningReplicasWithLiveness(repls, sp.NodeLivenessFn)
}

// decommissioningReplicasWithLiveness filters out replicas on decommissioning node/store
// from the provided repls and returns them in a slice, using the provided NodeLivenessFunc.
func (sp *StorePool) decommissioningReplicasWithLiveness(
	repls []roachpb.ReplicaDescriptor, nl NodeLivenessFunc,
) (decommissioningReplicas []roachpb.ReplicaDescriptor) {
	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()

	// NB: We use clock.Now() instead of clock.PhysicalTime() is order to
	// take clock signals from remote nodes into consideration.
	now := sp.clock.Now()
	timeUntilNodeDead := liveness.TimeUntilNodeDead.Get(&sp.st.SV)
	timeAfterNodeSuspect := liveness.TimeAfterNodeSuspect.Get(&sp.st.SV)

	for _, repl := range repls {
		detail := sp.GetStoreDetailLocked(repl.StoreID)
		switch detail.status(now, timeUntilNodeDead, nl, timeAfterNodeSuspect) {
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

// Clock returns the store pool's clock.
func (sp *StorePool) Clock() *hlc.Clock {
	return sp.clock
}

// IsDeterministic returns true iff the pool is configured to be deterministic.
func (sp *StorePool) IsDeterministic() bool {
	return sp.deterministic
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
	// NB: We use clock.Now() instead of clock.PhysicalTime() is order to
	// take clock signals from remote nodes into consideration.
	now := sp.clock.Now()
	timeUntilNodeDead := liveness.TimeUntilNodeDead.Get(&sp.st.SV)

	deadAsOf := sd.LastUpdatedTime.AddDuration(timeUntilNodeDead)
	if now.After(deadAsOf) {
		return true, 0, nil
	}
	// If there's no descriptor (meaning no gossip ever arrived for this
	// store), return unavailable.
	if sd.Desc == nil {
		return false, 0, errors.Errorf("store %d status unknown, cant tell if it's dead or alive", storeID)
	}
	return false, deadAsOf.GoTime().Sub(now.GoTime()), nil
}

// IsUnknown returns true if the given store's status is `storeStatusUnknown`
// (i.e. it just failed a liveness heartbeat and we cannot ascertain its
// liveness or deadness at the moment) or an error if the store is not found in
// the pool.
func (sp *StorePool) IsUnknown(storeID roachpb.StoreID) (bool, error) {
	status, err := sp.storeStatus(storeID, sp.NodeLivenessFn)
	if err != nil {
		return false, err
	}
	return status == storeStatusUnknown, nil
}

// IsDraining returns true if the given store's status is `storeStatusDraining`
// or an error if the store is not found in the pool.
func (sp *StorePool) IsDraining(storeID roachpb.StoreID) (bool, error) {
	status, err := sp.storeStatus(storeID, sp.NodeLivenessFn)
	if err != nil {
		return false, err
	}
	return status == storeStatusDraining, nil
}

// IsLive returns true if the node is considered alive by the store pool or an error
// if the store is not found in the pool.
func (sp *StorePool) IsLive(storeID roachpb.StoreID) (bool, error) {
	status, err := sp.storeStatus(storeID, sp.NodeLivenessFn)
	if err != nil {
		return false, err
	}
	return status == storeStatusAvailable, nil
}

// IsStoreHealthy returns whether we believe this store can serve requests
// reliably. A healthy store can be used for follower snapshot transmission or
// follower reads. A healthy store does not imply that replicas can be moved to
// this store.
func (sp *StorePool) IsStoreHealthy(storeID roachpb.StoreID) bool {
	status, err := sp.storeStatus(storeID, sp.NodeLivenessFn)
	if err != nil {
		return false
	}
	switch status {
	case storeStatusAvailable, storeStatusDecommissioning, storeStatusDraining:
		return true
	default:
		return false
	}
}

func (sp *StorePool) storeStatus(
	storeID roachpb.StoreID, nl NodeLivenessFunc,
) (storeStatus, error) {
	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()

	sd, ok := sp.DetailsMu.StoreDetails[storeID]
	if !ok {
		return storeStatusUnknown, errors.Errorf("store %d was not found", storeID)
	}
	// NB: We use clock.Now() instead of clock.PhysicalTime() is order to
	// take clock signals from remote nodes into consideration.
	now := sp.clock.Now()
	timeUntilNodeDead := liveness.TimeUntilNodeDead.Get(&sp.st.SV)
	timeAfterNodeSuspect := liveness.TimeAfterNodeSuspect.Get(&sp.st.SV)
	return sd.status(now, timeUntilNodeDead, nl, timeAfterNodeSuspect), nil
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
	return sp.liveAndDeadReplicasWithLiveness(repls, sp.NodeLivenessFn, includeSuspectAndDrainingStores)
}

// liveAndDeadReplicasWithLiveness divides the provided repls slice into two slices: the
// first for live replicas, and the second for dead replicas, using the
// provided NodeLivenessFunc.
// See comment on StorePool.LiveAndDeadReplicas(..).
func (sp *StorePool) liveAndDeadReplicasWithLiveness(
	repls []roachpb.ReplicaDescriptor, nl NodeLivenessFunc, includeSuspectAndDrainingStores bool,
) (liveReplicas, deadReplicas []roachpb.ReplicaDescriptor) {
	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()

	now := sp.clock.Now()
	timeUntilNodeDead := liveness.TimeUntilNodeDead.Get(&sp.st.SV)
	timeAfterNodeSuspect := liveness.TimeAfterNodeSuspect.Get(&sp.st.SV)

	for _, repl := range repls {
		detail := sp.GetStoreDetailLocked(repl.StoreID)
		// Mark replica as dead if store is dead.
		status := detail.status(now, timeUntilNodeDead, nl, timeAfterNodeSuspect)
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

// SetOnCapacityChange installs a callback to be called when any store
// capacity changes in the storepool. This currently doesn't consider local
// updates (UpdateLocalStoreAfterRelocate, UpdateLocalStoreAfterRebalance,
// UpdateLocalStoresAfterLeaseTransfer) as capacity changes.
func (sp *StorePool) SetOnCapacityChange(fn CapacityChangeFn) {
	sp.changeMu.Lock()
	defer sp.changeMu.Unlock()

	sp.changeMu.onChange = append(sp.changeMu.onChange, fn)
}

func (sp *StorePool) capacityChanged(storeID roachpb.StoreID, prev, cur roachpb.StoreCapacity) {
	sp.changeMu.Lock()
	defer sp.changeMu.Unlock()

	for _, fn := range sp.changeMu.onChange {
		fn(storeID, prev, cur)
	}
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

	// CandidateCPU tracks store-cpu-per-second stats for Stores that are
	// eligible to be rebalance targets.
	CandidateCPU Stat

	// CandidateQueriesPerSecond tracks queries-per-second stats for Stores that
	// are eligible to be rebalance targets.
	CandidateQueriesPerSecond Stat

	// candidateWritesPerSecond tracks writes-per-second stats for Stores that are
	// eligible to be rebalance targets.
	candidateWritesPerSecond Stat

	// CandidateIOOverloadScores tracks the IO overload stats for Stores that are
	// eligible to be rebalance candidates.
	CandidateIOOverloadScores Stat

	// CandidateMaxIOOverloadScores tracks the max IO overload stats for Stores
	// that are eligible to be rebalance candidates.
	CandidateMaxIOOverloadScores Stat
}

// MakeStoreList constructs a new store list based on the passed in descriptors.
// It will maintain the order of those descriptors.
func MakeStoreList(descriptors []roachpb.StoreDescriptor) StoreList {
	sl := StoreList{Stores: descriptors}
	for _, desc := range descriptors {
		sl.CandidateRanges.update(float64(desc.Capacity.RangeCount))
		sl.CandidateLeases.update(float64(desc.Capacity.LeaseCount))
		sl.candidateLogicalBytes.update(float64(desc.Capacity.LogicalBytes))
		sl.CandidateQueriesPerSecond.update(desc.Capacity.QueriesPerSecond)
		sl.candidateWritesPerSecond.update(desc.Capacity.WritesPerSecond)
		sl.CandidateCPU.update(desc.Capacity.CPUPerSecond)
		score, _ := desc.Capacity.IOThreshold.Score()
		sl.CandidateIOOverloadScores.update(score)
		maxScore, _ := desc.Capacity.IOThresholdMax.Score()
		sl.CandidateMaxIOOverloadScores.update(maxScore)
	}
	return sl
}

func (sl StoreList) String() string {
	return redact.StringWithoutMarkers(sl)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (sl StoreList) SafeFormat(w redact.SafePrinter, _ rune) {
	var buf redact.StringBuilder
	buf.Printf(
		"  candidate: avg-ranges=%.2f avg-leases=%.2f avg-disk-usage=%s "+
			"avg-queries-per-second=%.2f avg-store-cpu-per-second=%s "+
			"avg-io-overload=%.2f(max=%.2f)",
		sl.CandidateRanges.Mean,
		sl.CandidateLeases.Mean,
		humanizeutil.IBytes(int64(sl.candidateLogicalBytes.Mean)),
		sl.CandidateQueriesPerSecond.Mean,
		humanizeutil.Duration(time.Duration(int64(sl.CandidateCPU.Mean))),
		sl.CandidateIOOverloadScores.Mean,
		sl.CandidateMaxIOOverloadScores.Mean,
	)
	if len(sl.Stores) > 0 {
		buf.Printf("\n")
	} else {
		buf.Printf(" <no candidates>")
	}
	for _, desc := range sl.Stores {
		ioScore, _ := desc.Capacity.IOThreshold.Score()
		maxIOScore, _ := desc.Capacity.IOThresholdMax.Score()
		buf.Printf(
			"  %v: ranges=%d leases=%d disk-usage=%s queries-per-second=%.2f "+
				"store-cpu-per-second=%s io-overload=%.2f(max=%.2f)\n",
			desc.StoreID, desc.Capacity.RangeCount,
			desc.Capacity.LeaseCount, humanizeutil.IBytes(desc.Capacity.LogicalBytes),
			desc.Capacity.QueriesPerSecond,
			humanizeutil.Duration(time.Duration(int64(desc.Capacity.CPUPerSecond))),
			ioScore,
			maxIOScore,
		)
	}
	w.Print(buf)
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

// LoadMeans returns the mean for each load dimension tracked in the storelist.
func (sl StoreList) LoadMeans() load.Load {
	dims := load.Vector{}
	dims[load.Queries] = sl.CandidateQueriesPerSecond.Mean
	dims[load.CPU] = sl.CandidateCPU.Mean
	return dims
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

// FindStoreByID iterates over the store list and returns the descriptor for
// the store with storeID. If no such store is found, this function instead
// returns an empty descriptor and false. This fn is O(n) w.r.t the number of
// stores in the list.
func (sl StoreList) FindStoreByID(storeID roachpb.StoreID) (roachpb.StoreDescriptor, bool) {
	var desc roachpb.StoreDescriptor
	var found bool

	for i := range sl.Stores {
		if sl.Stores[i].StoreID == storeID {
			desc, found = sl.Stores[i], true
			break
		}
	}
	return desc, found
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

	storeIDs := make(roachpb.StoreIDSlice, 0, len(sp.DetailsMu.StoreDetails))
	for storeID := range sp.DetailsMu.StoreDetails {
		storeIDs = append(storeIDs, storeID)
	}
	return sp.getStoreListFromIDsLocked(storeIDs, sp.NodeLivenessFn, filter)
}

// GetStoreListFromIDs is the same function as GetStoreList but only returns stores
// from the subset of passed in store IDs.
func (sp *StorePool) GetStoreListFromIDs(
	storeIDs roachpb.StoreIDSlice, filter StoreFilter,
) (StoreList, int, ThrottledStoreReasons) {
	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()
	return sp.getStoreListFromIDsLocked(storeIDs, sp.NodeLivenessFn, filter)
}

// GetStoreListForTargets is the same as GetStoreList, but only returns stores
// from the subset of stores present in the passed in replication targets,
// converting to a StoreList.
func (sp *StorePool) GetStoreListForTargets(
	candidates []roachpb.ReplicationTarget, filter StoreFilter,
) (StoreList, int, ThrottledStoreReasons) {
	sp.DetailsMu.Lock()
	defer sp.DetailsMu.Unlock()

	storeIDs := make(roachpb.StoreIDSlice, 0, len(candidates))
	for _, tgt := range candidates {
		storeIDs = append(storeIDs, tgt.StoreID)
	}

	return sp.getStoreListFromIDsLocked(storeIDs, sp.NodeLivenessFn, filter)
}

// getStoreListFromIDsRLocked is the same function as GetStoreList but requires
// that the detailsMU read lock is held.
func (sp *StorePool) getStoreListFromIDsLocked(
	storeIDs roachpb.StoreIDSlice, nl NodeLivenessFunc, filter StoreFilter,
) (StoreList, int, ThrottledStoreReasons) {
	if sp.deterministic {
		sort.Sort(storeIDs)
	} else {
		shuffle.Shuffle(storeIDs)
	}

	var aliveStoreCount int
	var throttled ThrottledStoreReasons
	var storeDescriptors []roachpb.StoreDescriptor

	now := sp.clock.Now()
	timeUntilNodeDead := liveness.TimeUntilNodeDead.Get(&sp.st.SV)
	timeAfterNodeSuspect := liveness.TimeAfterNodeSuspect.Get(&sp.st.SV)

	for _, storeID := range storeIDs {
		detail, ok := sp.DetailsMu.StoreDetails[storeID]
		if !ok {
			// Do nothing; this store is not in the StorePool.
			continue
		}
		switch s := detail.status(now, timeUntilNodeDead, nl, timeAfterNodeSuspect); s {
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
		timeout := FailedReservationsTimeout.Get(&sp.st.SV)
		detail.ThrottledUntil = sp.clock.Now().AddDuration(timeout)
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

// GetLocalitiesPerReplica computes the localities for the provided replicas.
// It returns a map from the ReplicaDescriptor to the Locality of the Node.
func (sp *StorePool) GetLocalitiesPerReplica(
	replicas ...roachpb.ReplicaDescriptor,
) map[roachpb.ReplicaID]roachpb.Locality {
	sp.localitiesMu.RLock()
	defer sp.localitiesMu.RUnlock()
	localities := make(map[roachpb.ReplicaID]roachpb.Locality)
	for _, replica := range replicas {
		if locality, ok := sp.localitiesMu.nodeLocalities[replica.NodeID]; ok {
			localities[replica.ReplicaID] = locality.locality
		} else {
			localities[replica.ReplicaID] = roachpb.Locality{}
		}
	}
	return localities
}

// getNodeLocalityWithString returns the locality information and the string
// format for the given node.
func (sp *StorePool) getNodeLocalityWithString(nodeID roachpb.NodeID) localityWithString {
	nodeLocality := localityWithString{}
	sp.localitiesMu.RLock()
	defer sp.localitiesMu.RUnlock()
	if locality, ok := sp.localitiesMu.nodeLocalities[nodeID]; ok {
		nodeLocality = locality
	}
	// Return an empty localityWithString struct if nothing is found.
	return nodeLocality
}

// GetNodeLocalityString returns the locality information for the given node
// in its string format.
func (sp *StorePool) GetNodeLocalityString(nodeID roachpb.NodeID) string {
	return sp.getNodeLocalityWithString(nodeID).str
}

// GetNodeLocality returns the locality information for the given node.
func (sp *StorePool) GetNodeLocality(nodeID roachpb.NodeID) roachpb.Locality {
	return sp.getNodeLocalityWithString(nodeID).locality
}

// IsStoreReadyForRoutineReplicaTransfer returns true iff the store's node is
// live (as indicated by its `NodeLivenessStatus`) and thus a legal candidate
// to receive a replica.
//
// NB: What this method aims to capture is distinct from "dead" nodes. Nodes
// are classified as "dead" if they haven't successfully heartbeat their
// liveness record in the last `server.time_until_store_dead` seconds.
//
// Functionally, the distinction is that we simply avoid transferring replicas
// to "non-ready" nodes (i.e. nodes that _currently_ have a non-live
// `NodeLivenessStatus`), whereas we _actively move replicas off of "dead"
// nodes_.
func (sp *StorePool) IsStoreReadyForRoutineReplicaTransfer(
	ctx context.Context, targetStoreID roachpb.StoreID,
) bool {
	if sp.OverrideIsStoreReadyForRoutineReplicaTransferFn != nil {
		return sp.OverrideIsStoreReadyForRoutineReplicaTransferFn(ctx, targetStoreID)
	}
	return sp.isStoreReadyForRoutineReplicaTransferInternal(ctx, targetStoreID, sp.NodeLivenessFn)
}

func (sp *StorePool) isStoreReadyForRoutineReplicaTransferInternal(
	ctx context.Context, targetStoreID roachpb.StoreID, nl NodeLivenessFunc,
) bool {
	status, err := sp.storeStatus(targetStoreID, nl)
	if err != nil {
		return false
	}
	switch status {
	case storeStatusThrottled, storeStatusAvailable:
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
