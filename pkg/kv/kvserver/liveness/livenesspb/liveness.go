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
func (l *Liveness) IsLive(now hlc.Timestamp) bool {
	return now.Less(l.Expiration.ToTimestamp())
}

// IsDead returns true if the liveness expired more than threshold ago.
//
// Note that, because of threshold, IsDead() is not the inverse of IsLive().
func (l *Liveness) IsDead(now hlc.Timestamp, threshold time.Duration) bool {
	expiration := l.Expiration.ToTimestamp().AddDuration(threshold)
	return !now.Less(expiration)
}

// Compare returns an integer comparing two pieces of liveness information,
// based on which liveness information is more recent.
func (l *Liveness) Compare(o Liveness) int {
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

// NodeStatus should be used any place other than epoch leases where it is
// necessary to determine if a node is currently alive and what its health is.
type NodeStatus struct {
	draining   bool
	membership MembershipStatus
	isDead     bool
	isAlive    bool
	valid      bool
	Liveness   Liveness
}

type NodeStatusMap map[roachpb.NodeID]NodeStatus

// IsAvailableNotDraining returns whether or not the specified node is available
// to serve requests (i.e. it is live and not decommissioned) and is not in the
// process of draining/decommissioning. Note that draining/decommissioning nodes
// could still be leaseholders for ranges until drained, so this should not be
// used when the caller needs to be able to contact leaseholders directly.
// Returns false if the node is not in the local liveness table.
func (ne NodeStatus) IsAvailableNotDraining() bool {
	return ne.valid &&
		ne.isAlive &&
		!ne.membership.Decommissioning() &&
		!ne.membership.Decommissioned() &&
		!ne.draining
}

func (ne NodeStatus) IsAlive() bool {
	return ne.valid && ne.isAlive && !ne.IsDecommissioned()
}

func (ne NodeStatus) IsDecommissioned() bool {
	return ne.valid && ne.membership.Decommissioned()
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
func (ne NodeStatus) LivenessStatus() NodeLivenessStatus {
	// If we don't have a liveness expiration time, treat the status as unknown.
	// This is different than unavailable as it doesn't transition through being
	// marked as suspect. In unavailable we still won't transfer leases or
	// replicas to it in this state. A node that is in UNKNOWN status can
	// immediately transition to Available once it passes a liveness heartbeat.
	if !ne.valid {
		return NodeLivenessStatus_UNKNOWN
	}

	if ne.isDead {
		if !ne.membership.Active() {
			return NodeLivenessStatus_DECOMMISSIONED
		}
		return NodeLivenessStatus_DEAD
	}
	if ne.isAlive {
		if !ne.membership.Active() {
			return NodeLivenessStatus_DECOMMISSIONING
		}
		if ne.draining {
			return NodeLivenessStatus_DRAINING
		}
		return NodeLivenessStatus_LIVE
	}
	// Not yet dead, but has not heartbeated recently enough to be alive either.
	return NodeLivenessStatus_UNAVAILABLE
}

func (l Liveness) CreateNodeStatus(isLive bool, isDead bool) NodeStatus {
	// Dead means that there is low chance this node is online.
	// Alive means that there is a high probability the node is online.
	// A node can be neither dead nor alive (but not both).
	return NodeStatus{
		draining:   l.Draining,
		membership: l.Membership,
		isDead:     isDead,
		isAlive:    isLive,
		valid:      true,
		Liveness:   l,
	}
}

// These should not be used as they are only for testing. NodeStatusEntries are
// generally meant to be immutable and passed by value.

// TestDecommission marks a given node as decommissioned
func (ne *NodeStatus) TestDecommission() {
	ne.membership = MembershipStatus_DECOMMISSIONED
}

// TestDownNode marks a given node as down.
func (ne *NodeStatus) TestDownNode() {
	ne.isAlive = false
}

// TestRestartNode increments the epoch for a given node and marks it as
// alive.
func (ne *NodeStatus) TestRestartNode() {
	ne.isAlive = true
}
