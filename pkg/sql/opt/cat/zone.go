// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cat

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
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

	// SubzoneCount returns the number of subzones that are part of this zone.
	SubzoneCount() int

	// Subzone returns the ith subzone in the zone, where i < SubzoneCount.
	Subzone(i int) Subzone

	// Equal returns whether the two zone configurations are equal.
	Equal(o Zone) bool

	// InheritFromParent hydrates a zone's missing fields from its parent,
	// returning a new zone. The receiver is not mutated.
	InheritFromParent(parent Zone) Zone
}

// Subzone is an interface to zone configuration information that applies to
// either a SQL table index or a partition of a SQL table index.
type Subzone interface {
	// Index returns the ID of the SQL table index that the subzone represents.
	Index() StableID

	// Partition returns the name of the partition of the SQL table index that the
	// subzone represents. The return value is empty when the subzone represents
	// the entire index.
	Partition() string

	// Zone returns the Zone that applies to this Subzone.
	Zone() Zone
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
//	+region=east     Range can only be placed on nodes in region=east locality.
//	-region=west     Range cannot be placed on nodes in region=west locality.
type Constraint interface {
	// IsRequired is true if this is a required constraint, or false if this is
	// a prohibited constraint (signified by initial + or - character).
	IsRequired() bool

	// GetKey returns the constraint's string key (to left of =).
	GetKey() string

	// GetValue returns the constraint's string value (to right of =).
	GetValue() string
}

// The following types serve as concrete implementations of the preceding
// interfaces. They do not need to live in pkg/sql/opt/cat. For instance, they
// could be moved to pkg/sql/opt_catalog.go, like many of the other types that
// implement this package's interfaces. They are currently placed in this
// package to avoid needing to re-implement the zone config interfaces in
// pkg/sql/opt/testutils/testcat with near-identical types.

// catZone implements Zone for a zone configuration.
type catZone zonepb.ZoneConfig

// AsZone returns a Zone corresponding to the provided ZoneConfig.
func AsZone(z *zonepb.ZoneConfig) Zone {
	return (*catZone)(z)
}

// EmptyZone returns an empty Zone.
func EmptyZone() Zone {
	// NOTE: none of the methods on Zone or its child interfaces mutate their
	// receiver, so we could allocate this empty zone config once and return the
	// same reference to all callers of this method. However, this is not used on
	// a hot-path, so we play it safe.
	return AsZone(&zonepb.ZoneConfig{})
}

var _ Zone = &catZone{}

// ReplicaConstraintsCount is part of the Zone interface.
func (z *catZone) ReplicaConstraintsCount() int {
	return len(z.Constraints)
}

// ReplicaConstraints is part of the Zone interface.
func (z *catZone) ReplicaConstraints(i int) ReplicaConstraints {
	return (*catConstraintsConjunction)(&z.Constraints[i])
}

// VoterConstraintsCount is part of the Zone interface.
func (z *catZone) VoterConstraintsCount() int {
	return len(z.VoterConstraints)
}

// VoterConstraint is part of the Zone interface.
func (z *catZone) VoterConstraint(i int) ReplicaConstraints {
	return (*catConstraintsConjunction)(&z.VoterConstraints[i])
}

// LeasePreferenceCount is part of the Zone interface.
func (z *catZone) LeasePreferenceCount() int {
	return len(z.LeasePreferences)
}

// LeasePreference is part of the Zone interface.
func (z *catZone) LeasePreference(i int) ConstraintSet {
	return (*catConstraintSet)(&z.LeasePreferences[i])
}

// SubzoneCount is part of the Zone interface.
func (z *catZone) SubzoneCount() int {
	return len(z.Subzones)
}

// Subzone is part of the Zone interface.
func (z *catZone) Subzone(i int) Subzone {
	return (*catSubzone)(&z.Subzones[i])
}

// Equal is part of the Zone interface.
func (z *catZone) Equal(o Zone) bool {
	return (*zonepb.ZoneConfig)(z).Equal((*zonepb.ZoneConfig)(o.(*catZone)))
}

// InheritFromParent is part of the Zone interface.
func (z *catZone) InheritFromParent(parent Zone) Zone {
	cpy := zonepb.ZoneConfig(*z)
	cpy.InheritFromParent((*zonepb.ZoneConfig)(parent.(*catZone)))
	return AsZone(&cpy)
}

// catSubzone implements Subzone for a zone configuration subzone.
type catSubzone zonepb.Subzone

var _ Subzone = &catSubzone{}

// Index is part of the Subzone interface.
func (s *catSubzone) Index() StableID {
	return StableID(s.IndexID)
}

// Partition is part of the Subzone interface.
func (s *catSubzone) Partition() string {
	return s.PartitionName
}

// Zone is part of the Subzone interface.
func (s *catSubzone) Zone() Zone {
	return AsZone(&s.Config)
}

// catConstraintSet implements ConstraintSet for a zone configuration lease
// preference.
type catConstraintSet zonepb.LeasePreference

var _ ConstraintSet = &catConstraintSet{}

// ConstraintCount is part of the LeasePreference interface.
func (l *catConstraintSet) ConstraintCount() int {
	return len(l.Constraints)
}

// Constraint is part of the LeasePreference interface.
func (l *catConstraintSet) Constraint(i int) Constraint {
	return (*catConstraints)(&l.Constraints[i])
}

// catConstraintsConjunction implements ReplicaConstraints for a zone
// configuration constraint conjunction.
type catConstraintsConjunction zonepb.ConstraintsConjunction

var _ ReplicaConstraints = &catConstraintsConjunction{}

// ReplicaCount is part of the ReplicaConstraints interface.
func (c *catConstraintsConjunction) ReplicaCount() int32 {
	return c.NumReplicas
}

// ConstraintCount is part of the ReplicaConstraints interface.
func (c *catConstraintsConjunction) ConstraintCount() int {
	return len(c.Constraints)
}

// Constraint is part of the ReplicaConstraints interface.
func (c *catConstraintsConjunction) Constraint(i int) Constraint {
	return (*catConstraints)(&c.Constraints[i])
}

// catConstraints implements Constraint for a zone configuration constraint.
type catConstraints zonepb.Constraint

var _ Constraint = &catConstraints{}

// IsRequired is part of the Constraint interface.
func (c *catConstraints) IsRequired() bool {
	return c.Type == zonepb.Constraint_REQUIRED
}

// GetKey is part of the Constraint interface.
func (c *catConstraints) GetKey() string {
	return c.Key
}

// GetValue is part of the Constraint interface.
func (c *catConstraints) GetValue() string {
	return c.Value
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
			voterChild.Childf("%d voter replicas: %s", numReplicas, constraintStr)
		} else {
			voterChild.Childf("voter constraints: %s", constraintStr)
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
