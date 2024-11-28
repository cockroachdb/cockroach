// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package livenesspb

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NodeVitalityInterface is the interface code that uses NodeLiveness should
// use. Only these three exposed methods are intended to be publicly used by the
// reset of the codebase. Various tests mock out different parts of this
// interface, and TestNodeVitality is intended to be used in tests that need a
// specific NodeLiveness set up.
type NodeVitalityInterface interface {
	GetNodeVitalityFromCache(roachpb.NodeID) NodeVitality
	ScanNodeVitalityFromCache() NodeVitalityMap
	ScanNodeVitalityFromKV(context.Context) (NodeVitalityMap, error)
}

// IsLive returns whether the node is considered live at the given time.
//
// NOTE: If one is interested whether the Liveness is valid currently, then the
// timestamp passed in should be the known high-water mark of all the clocks of
// the nodes in the cluster. For example, if the liveness expires at ts 100, our
// physical clock is at 90, but we know that another node's clock is at 110,
// then it's preferable (more consistent across nodes) for the liveness to be
// considered expired. For that purpose, it's better to pass in
// clock.Now().GoTime() rather than clock.PhysicalNow() - the former takes into
// consideration clock signals from other nodes, the latter doesn't.
func (l Liveness) IsLive(now hlc.Timestamp) bool {
	return now.Less(l.Expiration.ToTimestamp())
}

// Compare returns an integer comparing two pieces of liveness information,
// based on which liveness information is more recent.
func (l Liveness) Compare(o Liveness) int {
	// Compare Epoch, and if no change there, Expiration.
	if l.Epoch != o.Epoch {
		if l.Epoch < o.Epoch {
			return -1
		}
		return +1
	}
	if l.Expiration != o.Expiration {
		if l.Expiration.Less(o.Expiration) {
			return -1
		}
		return +1
	}
	return 0
}

func (l Liveness) String() string {
	return redact.StringWithoutMarkers(l)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (l Liveness) SafeFormat(s redact.SafePrinter, _ rune) {
	s.Printf("liveness(nid:%d epo:%d exp:%s", l.NodeID, l.Epoch, l.Expiration)
	if l.Draining || l.Membership.Decommissioning() || l.Membership.Decommissioned() {
		s.Printf(" drain:%t membership:%s", l.Draining, l.Membership)
	}
	s.Printf(")")
}

// Decommissioning is a shorthand to check if the membership status is DECOMMISSIONING.
func (c MembershipStatus) Decommissioning() bool { return c == MembershipStatus_DECOMMISSIONING }

// Decommissioned is a shorthand to check if the membership status is DECOMMISSIONED.
func (c MembershipStatus) Decommissioned() bool { return c == MembershipStatus_DECOMMISSIONED }

// Active is a shorthand to check if the membership status is ACTIVE.
func (c MembershipStatus) Active() bool { return c == MembershipStatus_ACTIVE }

// SafeValue implements the redact.SafeValue interface.
func (MembershipStatus) SafeValue() {}

func (c MembershipStatus) String() string {
	// NB: These strings must not be changed, since the CLI matches on them.
	switch c {
	case MembershipStatus_ACTIVE:
		return "active"
	case MembershipStatus_DECOMMISSIONING:
		return "decommissioning"
	case MembershipStatus_DECOMMISSIONED:
		return "decommissioned"
	default:
		err := "unknown membership status, expected one of [active,decommissioning,decommissioned]"
		panic(err)
	}
}

// ValidateTransition validates transitions of the liveness record,
// returning an error if the proposed transition is invalid. Ignoring no-ops
// (which also includes decommissioning a decommissioned node) the valid state
// transitions for Membership are as follows:
//
//	Decommissioning  => Active
//	Active           => Decommissioning
//	Decommissioning  => Decommissioned
//
// This returns an error if the transition is invalid, and false if the
// transition is unnecessary (since it would be a no-op).
func ValidateTransition(old Liveness, newStatus MembershipStatus) (bool, error) {
	if (old == Liveness{}) {
		return false, errors.AssertionFailedf("invalid old liveness record; found to be empty")
	}

	if old.Membership == newStatus {
		// No-op.
		return false, nil
	}

	if old.Membership.Decommissioned() && newStatus.Decommissioning() {
		// No-op as it would just move directly back to decommissioned.
		return false, nil
	}

	if newStatus.Active() && !old.Membership.Decommissioning() {
		err := fmt.Sprintf("can only recommission a decommissioning node; n%d found to be %s",
			old.NodeID, old.Membership.String())
		return false, status.Error(codes.FailedPrecondition, err)
	}

	// We don't assert on the new membership being "decommissioning" as all
	// previous states are valid (again, consider no-ops).

	if newStatus.Decommissioned() && !old.Membership.Decommissioning() {
		err := fmt.Sprintf("can only fully decommission an already decommissioning node; n%d found to be %s",
			old.NodeID, old.Membership.String())
		return false, status.Error(codes.FailedPrecondition, err)
	}

	return true, nil
}

// IsLiveMapEntry encapsulates data about current liveness for a
// node based on the liveness range.
// TODO(abaptist): This should only be used for epoch leases as it uses an
// overly strict version of liveness. Once epoch leases are removed, this will
// be also.
type IsLiveMapEntry struct {
	Liveness
	IsLive bool
}

// IsLiveMap is a type alias for a map from NodeID to IsLiveMapEntry.
type IsLiveMap map[roachpb.NodeID]IsLiveMapEntry

// NodeVitality should be used any place other than epoch leases where it is
// necessary to determine if a node is currently alive and what its health is.
// Aliveness and deadness are concepts that refer to our best guess of the
// health of this node. An alive node is one that should be treated as healthy
// for most decisions. A dead node is one that should be treated as unavailable
// for most decisions. A node can be neither dead or alive, if it has gone
// offline recently or if failing to consistently gossip. Different areas of the
// code have the flexibility to treat this condition differently.
type NodeVitality struct {
	nodeID roachpb.NodeID
	// draining is whether this node currently draining.
	draining bool
	// membership is whether the node is active or in a state of decommissioning.
	membership MembershipStatus
	// connected is whether we are currently directly connect to this node.
	connected bool

	// When the record is created. Records are not held for long, but they should
	// always give consistent results when asked.
	now                  hlc.Timestamp
	timeUntilNodeDead    time.Duration
	timeAfterNodeSuspect time.Duration

	// Data that comes from the node/store descriptor cache.
	descUpdateTime      hlc.Timestamp
	descUnavailableTime hlc.Timestamp

	// Data that comes from the liveness range
	livenessExpiration hlc.Timestamp
	livenessEpoch      int64
}

type NodeVitalityMap map[roachpb.NodeID]NodeVitality

type VitalityUsage int

const (
	_ VitalityUsage = iota
	IsAliveNotification
	EpochLease
	Rebalance
	Admin
	DistSQL
	Upgrade
	ConsistencyQueue
	AdminHealthCheck
	DecommissionCheck
	SpanConfigConformance
	ReplicaProgress
	Metrics
	LeaseCampaign
	LeaseCampaignWeak
	RangeQuiesience
	NetworkMap
	LossOfQuorum
	ReplicaGCQueue
	DistSender
	// TestingIsAliveAndHasHeartbeated is intended to be used by tests that want
	// to ensure that a node is alive and has heartbeated its liveness record
	// already.
	//
	// This is intended to be used in place of IsAliveNotification, whose usages
	// aren't very principled, and sometimes expect that a node has heartbeated
	// its liveness record (by checking the epoch is != 0). Previously, we could
	// get away with these unprincipled usages because epoch based leases would
	// proactively heartbeat liveness records when acquiring leases. However, when
	// using leader leases, this doesn't happen, which then makes these tests
	// flaky.
	//
	// At some point, we'll want to revisit IsAliveNotification and make sure
	// returning true when the liveness epoch is 0 is indeed well motivated.
	TestingIsAliveAndHasHeartbeated
)

func (nv NodeVitality) IsLive(usage VitalityUsage) bool {
	switch usage {
	case IsAliveNotification:
		return nv.isAlive()
	case EpochLease:
		return nv.isAliveEpoch()
	case Rebalance:
		return nv.isAlive()
	case DistSQL:
		return nv.isAliveAndConnected()
	case Upgrade:
		return nv.isAlive()
	case ConsistencyQueue:
		return nv.isAvailableNotDraining()
	case AdminHealthCheck:
		return nv.isAvailableNotDraining()
	case DecommissionCheck:
		return nv.isAlive()
	case SpanConfigConformance:
		return nv.isAlive()
	case ReplicaProgress:
		return nv.isAliveAndConnected()
	case Metrics:
		return nv.isAlive()
	case LeaseCampaign:
		{
			if !nv.isValid() {
				// If we don't know about the leader in our liveness map, then we err on the side
				// of caution and don't campaign, so assume it is alive.
				return true
			}
			return nv.isAliveAndConnected()
		}
	case LeaseCampaignWeak:
		{
			if !nv.isValid() {
				// If we don't know about the leader in our liveness map, then we err on the side
				// of caution and don't campaign, so assume it is alive.
				return true
			}
			return nv.now.Less(nv.livenessExpiration)
		}
	case RangeQuiesience:
		{
			if !nv.isValid() {
				// If we don't know about the node, then we err on the side
				// of caution and don't quiesce, so assume it is alive.
				return true
			}
			return nv.isAliveAndConnected()
		}
	case NetworkMap:
		return nv.connected
	case LossOfQuorum:
		return nv.isAlive()
	case ReplicaGCQueue:
		return nv.isAlive()
	case DistSender:
		return nv.isAvailableNotDraining()
	case TestingIsAliveAndHasHeartbeated:
		return nv.testingIsAliveAndHasHeartbeated()
	}

	// TODO(baptist): Should be an assertion that we don't know this uasge.
	return false
}

// IsAvailableNotDraining returns whether or not the specified node is available
// to serve requests (i.e. it is live and not decommissioned) and is not in the
// process of draining/decommissioning. Note that draining/decommissioning nodes
// could still be leaseholders for ranges until drained, so this should not be
// used when the caller needs to be able to contact leaseholders directly.
// Returns false if the node is not in the local liveness table.
func (nv NodeVitality) isAvailableNotDraining() bool {
	return nv.isValid() &&
		nv.isAlive() &&
		!nv.membership.Decommissioning() &&
		!nv.membership.Decommissioned() &&
		!nv.draining
}

func (nv NodeVitality) isAliveAndConnected() bool {
	return nv.isAvailableNotDraining() && nv.connected
}

// isAliveEpoch is used for epoch leases. It is similar to isAlive, but doesn't
// treat epoch 0 as alive, and doesn't care about the store descriptor updates.
func (nv NodeVitality) isAliveEpoch() bool {
	if !nv.isValid() || nv.IsDecommissioned() {
		return false
	}

	return nv.now.Less(nv.livenessExpiration)
}

// isAlive is used for many cases. It returns true if the node descriptor has
// been gossipped recently and the liveness expiration is in the future. Also
// excludes decommissioned nodes, however that check is usually redundant as
// they shouldn't be gossiping after they are decommissioned.
func (nv NodeVitality) isAlive() bool {
	if !nv.isValid() || nv.IsDecommissioned() {
		return false
	}

	// If there is a valid descriptor, check that it is being updated. If we don't
	// have one it may be because we haven't gotten the first gossip update yet.
	if nv.descUpdateTime.IsSet() && nv.now.After(nv.descUpdateTime.AddDuration(nv.timeUntilNodeDead)) {
		// If the store descriptor is not being updated, we mark the node as dead
		// regardless of what liveness says.
		return false
	}
	// If the descriptor was recently unavailable, treat the node as suspect and
	// don't report as alive. This handles recent restarts or missed updates.
	if nv.descUnavailableTime.IsSet() && nv.descUnavailableTime.AddDuration(nv.timeAfterNodeSuspect).After(nv.now) {
		return false
	}

	// If we have a 0 epoch, the expiration time won't be written, so we assume
	// that it is alive since the store descriptor is being updated.
	if nv.livenessEpoch == 0 {
		return true
	}
	return nv.now.Less(nv.livenessExpiration)
}

// testingIsAliveAndHasHeartbeated is like isAlive except it also ensures that
// the node has heartbeated its liveness epoch at least once. Also see the
// comment on TestingIsAliveAndHasHeartbeated.
func (nv NodeVitality) testingIsAliveAndHasHeartbeated() bool {
	return nv.isAlive() && nv.livenessEpoch != 0
}

func (nv NodeVitality) IsDecommissioning() bool {
	return nv.isValid() && nv.membership.Decommissioning()
}

func (nv NodeVitality) IsDecommissioned() bool {
	return nv.isValid() && nv.membership.Decommissioned()
}

// MembershipStatus returns the current membership status of this node.
// It is preferable to use isDecommissioning or isDecommissined since they will
// check if the entry is valid first.
func (nv NodeVitality) MembershipStatus() MembershipStatus {
	return nv.membership
}

func (nv NodeVitality) IsDraining() bool {
	return nv.draining
}

// IsValid returns whether this entry was found.
func (nv NodeVitality) isValid() bool {
	return nv != NodeVitality{}
}

// GetInternalLiveness should be used for only two purposes:
// 1) Compatibility with existing APIs that expose the Liveness record
// 2) Epoch leases
// Avoid using this method as the Liveness expiration is not always populated.
func (nv NodeVitality) GetInternalLiveness() Liveness {
	return Liveness{
		NodeID:     nv.nodeID,
		Epoch:      nv.livenessEpoch,
		Expiration: nv.livenessExpiration.ToLegacyTimestamp(),
		Draining:   nv.draining,
		Membership: nv.membership,
	}
}

// GenLiveness is used to generate a liveness record that is similar to the
// actual liveness record, but has the epoch and expiration filled in with
// meaningful values.
// TODO(baptist): If we are not updating the expiration use the store descriptor
// time.
// TODO(baptist): Track down all uses of this and remove them.
func (nv NodeVitality) GenLiveness() Liveness {
	return Liveness{
		NodeID:     nv.nodeID,
		Epoch:      nv.livenessEpoch,
		Expiration: nv.livenessExpiration.ToLegacyTimestamp(),
		Draining:   nv.draining,
		Membership: nv.membership,
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
// TODO(baptist): Remove NodeLivenessStatus and all usages. The logic in this
// method is somewhat convoluted but this should be changed as part of a
// allocator refactor, not as part of liveness.
func (nv NodeVitality) LivenessStatus() NodeLivenessStatus {
	// If we don't have a liveness expiration time, treat the status as unknown.
	// This is different than unavailable as it doesn't transition through being
	// marked as suspect. In unavailable we still won't transfer leases or
	// replicas to it in this state. A node that is in UNKNOWN status can
	// immediately transition to Available once it passes a liveness heartbeat.
	if !nv.isValid() {
		return NodeLivenessStatus_UNKNOWN
	}

	isDead := false
	isAlive := false

	if nv.descUpdateTime.IsSet() && nv.now.After(nv.descUpdateTime.AddDuration(nv.timeUntilNodeDead)) {
		isDead = true
	}

	// If it expired longer than timeUntiLNodeDead ago, then treat as DEAD,
	// otherwise it is UNAVAILABLE. If livenessEpoch is 0, we don't want to assume
	// its dead since it hasn't had a chance to update its expiration yet.
	if nv.livenessEpoch > 0 && nv.now.After(nv.livenessExpiration.AddDuration(nv.timeUntilNodeDead)) {
		isDead = true
	}

	// Expiration on liveness record is still valid.
	if nv.now.Less(nv.livenessExpiration) {
		isAlive = true
	}

	if nv.descUpdateTime.IsSet() && nv.now.Less(nv.descUpdateTime.AddDuration(nv.timeAfterNodeSuspect)) {
		isAlive = true
	}

	// If the descriptor was recently unavailable, treat the node as unavailable (not alive).
	if nv.descUnavailableTime.IsSet() && nv.descUnavailableTime.AddDuration(nv.timeAfterNodeSuspect).After(nv.now) {
		isAlive = false
	}

	if nv.membership == MembershipStatus_DECOMMISSIONED {
		if isAlive {
			// Despite having marked the node as fully decommissioned, through
			// this NodeLivenessStatus API we still surface the node as
			// "Decommissioning". See #50707 for more details.
			return NodeLivenessStatus_DECOMMISSIONING
		}
		return NodeLivenessStatus_DECOMMISSIONED
	}

	// Somewhat arbitrarily, being unavailable trumps decommissioning.
	if !isAlive && !isDead {
		return NodeLivenessStatus_UNAVAILABLE
	}

	if nv.membership == MembershipStatus_DECOMMISSIONING {
		// We return decommissioned here even though it hasn't fully transitioned in
		// the membership table. Decommissioning has had bugs where it gets stuck in
		// the decommissioning state.
		if isDead {
			return NodeLivenessStatus_DECOMMISSIONED
		}
		return NodeLivenessStatus_DECOMMISSIONING
	}

	if isDead {
		return NodeLivenessStatus_DEAD
	}

	// We check this after dead because for a dead node, we dead is more important
	// than draining.
	if nv.draining {
		return NodeLivenessStatus_DRAINING
	}

	return NodeLivenessStatus_LIVE

}

// CreateNodeVitality creates a NodeVitality record based on a liveness record
// and information whether it should be treated as dead or alive. Computing
// whether it is dead or alive requires external data sources so the information
// must be passed in.
func (l Liveness) CreateNodeVitality(
	now hlc.Timestamp,
	descUpdateTime hlc.Timestamp,
	descUnavailableTime hlc.Timestamp,
	connected bool,
	timeUntilNodeDead time.Duration,
	timeAfterNodeSuspect time.Duration,
) NodeVitality {
	// Dead means that there is low chance this node is online.
	// Alive means that there is a high probability the node is online.
	// A node can be neither dead nor alive (but not both).
	return NodeVitality{
		nodeID:               l.NodeID,
		draining:             l.Draining,
		membership:           l.Membership,
		connected:            connected,
		now:                  now,
		descUpdateTime:       descUpdateTime,
		descUnavailableTime:  descUnavailableTime,
		timeUntilNodeDead:    timeUntilNodeDead,
		timeAfterNodeSuspect: timeAfterNodeSuspect,
		livenessExpiration:   l.Expiration.ToTimestamp(),
		livenessEpoch:        l.Epoch,
	}
}
