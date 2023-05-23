// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package livenesspb

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

// IsDead returns true if the liveness expired more than threshold ago.
//
// Note that, because of threshold, IsDead() is not the inverse of IsLive().
func (l Liveness) IsDead(now hlc.Timestamp, threshold time.Duration) bool {
	expiration := l.Expiration.ToTimestamp().AddDuration(threshold)
	return !now.Less(expiration)
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
	if !l.Expiration.EqOrdering(o.Expiration) {
		if l.Expiration.Less(o.Expiration) {
			return -1
		}
		return +1
	}
	return 0
}

func (l Liveness) String() string {
	var extra string
	if l.Draining || l.Membership.Decommissioning() || l.Membership.Decommissioned() {
		extra = fmt.Sprintf(" drain:%t membership:%s", l.Draining, l.Membership.String())
	}
	return fmt.Sprintf("liveness(nid:%d epo:%d exp:%s%s)", l.NodeID, l.Epoch, l.Expiration, extra)
}

// Decommissioning is a shorthand to check if the membership status is DECOMMISSIONING.
func (c MembershipStatus) Decommissioning() bool { return c == MembershipStatus_DECOMMISSIONING }

// Decommissioned is a shorthand to check if the membership status is DECOMMISSIONED.
func (c MembershipStatus) Decommissioned() bool { return c == MembershipStatus_DECOMMISSIONED }

// Active is a shorthand to check if the membership status is ACTIVE.
func (c MembershipStatus) Active() bool { return c == MembershipStatus_ACTIVE }

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

// VitalityStatus tracks the current health of a node.
type VitalityStatus int

const (
	_ VitalityStatus = iota
	// VitalityAlive means the node is able to publish gossip updates.
	VitalityAlive
	// VitalitySuspect means the node is able to currently publish gossip updates, but has been recently unavailable.
	VitalitySuspect
	// VitalitySuspect means the node is not currently publishing gossip updates.
	VitalityUnavailable
	// VitalityDead means The node has not published a gossip update for an extended period.
	VitalityDead
)

// NodeVitality should be used any place other than epoch leases where it is
// necessary to determine if a node is currently alive and what its health is.
// Aliveness and deadness are concepts that refer to our best guess of the
// health of this node. An alive node is one that should be treated as healthy
// for most decisions. A dead node is one that should be treated as unavailable
// for most decisions. A node can be neither dead or alive, if it has gone
// offline recently or if failing to consistently gossip. Different areas of the
// code have the flexibility to treat this condition differently.
type NodeVitality struct {
	// draining is whether this node currently draining.
	draining bool
	// membership is whether the node is active or in a state of decommissioning.
	membership MembershipStatus
	// The best guess of the current health of this node.
	health VitalityStatus
	// valid is whether a valid NodeVitality record exists for this node. It is
	// possible for newly created nodes to have invalid vitality records.
	valid bool
	// The underlying liveness record this NodeVitality record was created from.
	// Most code should not access this directly, however epoch leases need the
	// underlying liveness record. Additionally some admin methods directly report
	// the Liveness protobuf record to the end user.
	// TODO(baptist): Remove me.
	Liveness Liveness
}

type NodeStatusMap map[roachpb.NodeID]NodeVitality

// IsAvailableNotDraining returns whether or not the specified node is available
// to serve requests (i.e. it is live and not decommissioned) and is not in the
// process of draining/decommissioning. Note that draining/decommissioning nodes
// could still be leaseholders for ranges until drained, so this should not be
// used when the caller needs to be able to contact leaseholders directly.
// Returns false if the node is not in the local liveness table.
func (nv NodeVitality) IsAvailableNotDraining() bool {
	return nv.valid &&
		nv.IsAlive() &&
		!nv.membership.Decommissioning() &&
		!nv.membership.Decommissioned() &&
		!nv.draining
}

func (nv NodeVitality) IsAlive() bool {
	return nv.valid && nv.health == VitalityAlive && !nv.IsDecommissioned()
}

func (nv NodeVitality) IsDecommissioned() bool {
	return nv.valid && nv.membership.Decommissioned()
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
// TODO(baptist): Remove NodeLivenessStatus and all usages.
func (nv NodeVitality) LivenessStatus() NodeLivenessStatus {
	// If we don't have a liveness expiration time, treat the status as unknown.
	// This is different than unavailable as it doesn't transition through being
	// marked as suspect. In unavailable we still won't transfer leases or
	// replicas to it in this state. A node that is in UNKNOWN status can
	// immediately transition to Available once it passes a liveness heartbeat.
	if !nv.valid {
		return NodeLivenessStatus_UNKNOWN
	}

	if nv.health == VitalityDead {
		if !nv.membership.Active() {
			return NodeLivenessStatus_DECOMMISSIONED
		}
		return NodeLivenessStatus_DEAD
	}
	if nv.health == VitalityAlive {
		if !nv.membership.Active() {
			return NodeLivenessStatus_DECOMMISSIONING
		}
		if nv.draining {
			return NodeLivenessStatus_DRAINING
		}
		return NodeLivenessStatus_LIVE
	}
	// Not yet dead, but has not heartbeated recently enough to be alive either.
	return NodeLivenessStatus_UNAVAILABLE
}

// CreateNodeVitality creates a NodeVitality record based on a liveness record
// and information whether it should be treated as dead or alive. Computing
// whether it is dead or alive requires external data sources so the information
// must be passed in.
func (l Liveness) CreateNodeVitality(health VitalityStatus) NodeVitality {

	// Dead means that there is low chance this node is online.
	// Alive means that there is a high probability the node is online.
	// A node can be neither dead nor alive (but not both).
	return NodeVitality{
		draining:   l.Draining,
		membership: l.Membership,
		health:     health,
		valid:      true,
		Liveness:   l,
	}
}

// These should not be used as they are only for testing. NodeStatusEntries are
// generally meant to be immutable and passed by value.

// TestDecommission marks a given node as decommissioned.
func (nv *NodeVitality) TestDecommission() {
	nv.membership = MembershipStatus_DECOMMISSIONED
}

// TestDownNode marks a given node as down (not alive, but not dead).
func (nv *NodeVitality) TestDownNode() {
	nv.health = VitalityUnavailable
}

// TestRestartNode marks a node as alive.
func (nv *NodeVitality) TestRestartNode() {
	nv.health = VitalityAlive
}
