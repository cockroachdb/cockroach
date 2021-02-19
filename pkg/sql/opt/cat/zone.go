// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cat

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// Zone is an interface to zone configuration information used by the optimizer.
// The optimizer prefers indexes with constraints that best match the locality
// of the gateway node that plans the query.
type Zone interface {
	// ReplicaConstraintsCount returns the number of replica constraint sets that
	// are part of this zone.
	//
	// TODO(aayush/rytaft): Go through the callers of the methods here and decide
	// the right semantics for handling the new `voter_constraints` attribute.
	ReplicaConstraintsCount() int

	// ReplicaConstraints returns the ith set of replica constraints in the zone,
	// where i < ReplicaConstraintsCount.
	ReplicaConstraints(i int) ReplicaConstraints

	// VoterConstraintsCount returns the number of voter replica constraint sets
	// that are part of this zone.
	VoterConstraintsCount() int

	// VoterConstraint returns the ith set of voter replica constraints in the
	// zone, where i < VoterConstraintsCount.
	VoterConstraint(i int) ReplicaConstraints

	// LeasePreferenceCount returns the number of lease preferences that are part
	// of this zone.
	LeasePreferenceCount() int

	// LeasePreference returns the ith lease preference in the zone, where
	// i < LeasePreferenceCount.
	LeasePreference(i int) ConstraintSet
}

// ConstraintSet is a set of constraints that apply to a range, restricting
// which nodes can host that range or stating which nodes are preferred as the
// leaseholder.
type ConstraintSet interface {
	// ConstraintCount returns the number of constraints in the set.
	ConstraintCount() int

	// Constraint returns the ith constraint in the set, where
	// i < ConstraintCount.
	Constraint(i int) Constraint
}

// ReplicaConstraints is a set of constraints that apply to one or more replicas
// of a range, restricting which nodes can host that range. For example, if a
// table range has three replicas, then two of the replicas might be pinned to
// nodes in one region, whereas the third might be pinned to another region.
type ReplicaConstraints interface {
	ConstraintSet

	// ReplicaCount returns the number of replicas that should abide by this set
	// of constraints. If 0, then the constraints apply to all replicas of the
	// range (and there can be only one ReplicaConstraints in the Zone).
	ReplicaCount() int32
}

// Constraint governs placement of range replicas on nodes. A constraint can
// either be required or prohibited. A required constraint's key/value pair must
// match one of the tiers of a node's locality for the range to locate there.
// A prohibited constraint's key/value pair must *not* match any of the tiers of
// a node's locality for the range to locate there. For example:
//
//   +region=east     Range can only be placed on nodes in region=east locality.
//   -region=west     Range cannot be placed on nodes in region=west locality.
//
type Constraint interface {
	// IsRequired is true if this is a required constraint, or false if this is
	// a prohibited constraint (signified by initial + or - character).
	IsRequired() bool

	// GetKey returns the constraint's string key (to left of =).
	GetKey() string

	// GetValue returns the constraint's string value (to right of =).
	GetValue() string
}

// FormatZone nicely formats a catalog zone using a treeprinter for debugging
// and testing.
func FormatZone(zone Zone, tp treeprinter.Node) {
	if zone.ReplicaConstraintsCount() == 0 && zone.VoterConstraintsCount() == 0 &&
		zone.LeasePreferenceCount() == 0 {
		return
	}
	zoneChild := tp.Childf("ZONE")

	replicaChild := zoneChild
	if zone.ReplicaConstraintsCount() > 1 {
		replicaChild = replicaChild.Childf("replica constraints")
	}
	for i, n := 0, zone.ReplicaConstraintsCount(); i < n; i++ {
		replConstraint := zone.ReplicaConstraints(i)
		constraintStr := formatConstraintSet(replConstraint)
		if zone.ReplicaConstraintsCount() > 1 {
			numReplicas := replConstraint.ReplicaCount()
			replicaChild.Childf("%d replicas: %s", numReplicas, constraintStr)
		} else {
			replicaChild.Childf("constraints: %s", constraintStr)
		}
	}

	voterChild := zoneChild
	if zone.VoterConstraintsCount() > 1 {
		voterChild = voterChild.Childf("voter replica constraints")
	}
	for i, n := 0, zone.VoterConstraintsCount(); i < n; i++ {
		voterConstraint := zone.VoterConstraint(i)
		constraintStr := formatConstraintSet(voterConstraint)
		if zone.VoterConstraintsCount() > 1 {
			numReplicas := voterConstraint.ReplicaCount()
			replicaChild.Childf("%d voter replicas: %s", numReplicas, constraintStr)
		} else {
			replicaChild.Childf("voter constraints: %s", constraintStr)
		}
	}

	leaseChild := zoneChild
	if zone.LeasePreferenceCount() > 1 {
		leaseChild = leaseChild.Childf("lease preferences")
	}
	for i, n := 0, zone.LeasePreferenceCount(); i < n; i++ {
		leasePref := zone.LeasePreference(i)
		constraintStr := formatConstraintSet(leasePref)
		if zone.LeasePreferenceCount() > 1 {
			leaseChild.Child(constraintStr)
		} else {
			leaseChild.Childf("lease preference: %s", constraintStr)
		}
	}
}

func formatConstraintSet(set ConstraintSet) string {
	var buf bytes.Buffer
	buf.WriteRune('[')
	for i, n := 0, set.ConstraintCount(); i < n; i++ {
		constraint := set.Constraint(i)
		if i != 0 {
			buf.WriteRune(',')
		}
		if constraint.IsRequired() {
			buf.WriteRune('+')
		} else {
			buf.WriteRune('-')
		}
		if constraint.GetKey() != "" {
			fmt.Fprintf(&buf, "%s=%s", constraint.GetKey(), constraint.GetValue())
		} else {
			buf.WriteString(constraint.GetValue())
		}
	}
	buf.WriteRune(']')
	return buf.String()
}
