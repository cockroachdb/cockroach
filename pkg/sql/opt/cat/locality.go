// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
)

const regionKey = "region"

// isConstraintLocal returns isLocal=true and ok=true if the given constraint is
// a required constraint matching the given localRegion. Returns isLocal=false
// and ok=true if the given constraint is a prohibited constraint matching the
// given local region or if it is a required constraint matching a different
// region. Any other scenario returns ok=false, since this constraint gives no
// information about whether the constrained replicas are local or remote.
func isConstraintLocal(constraint Constraint, localRegion string) (isLocal bool, ok bool) {
	if constraint.GetKey() != regionKey {
		// We only care about constraints on the region.
		return false /* isLocal */, false /* ok */
	}
	if constraint.GetValue() == localRegion {
		if constraint.IsRequired() {
			// The local region is required.
			return true /* isLocal */, true /* ok */
		}
		// The local region is prohibited.
		return false /* isLocal */, true /* ok */
	}
	if constraint.IsRequired() {
		// A remote region is required.
		return false /* isLocal */, true /* ok */
	}
	// A remote region is prohibited, so this constraint gives no information
	// about whether the constrained replicas are local or remote.
	return false /* isLocal */, false /* ok */
}

// IsZoneLocal returns true if the given zone config indicates that the replicas
// it constrains will be primarily located in the localRegion.
func IsZoneLocal(zone Zone, localRegion string) bool {
	// First count the number of local and remote replica constraints. If all
	// are local or all are remote, we can return early.
	local, remote := 0, 0
	for i, n := 0, zone.ReplicaConstraintsCount(); i < n; i++ {
		replicaConstraint := zone.ReplicaConstraints(i)
		for j, m := 0, replicaConstraint.ConstraintCount(); j < m; j++ {
			constraint := replicaConstraint.Constraint(j)
			if isLocal, ok := isConstraintLocal(constraint, localRegion); ok {
				if isLocal {
					local++
				} else {
					remote++
				}
			}
		}
	}
	if local > 0 && remote == 0 {
		return true
	}
	if remote > 0 && local == 0 {
		return false
	}

	// Next check the voter replica constraints. Once again, if all are local or
	// all are remote, we can return early.
	local, remote = 0, 0
	for i, n := 0, zone.VoterConstraintsCount(); i < n; i++ {
		replicaConstraint := zone.VoterConstraint(i)
		for j, m := 0, replicaConstraint.ConstraintCount(); j < m; j++ {
			constraint := replicaConstraint.Constraint(j)
			if isLocal, ok := isConstraintLocal(constraint, localRegion); ok {
				if isLocal {
					local++
				} else {
					remote++
				}
			}
		}
	}
	if local > 0 && remote == 0 {
		return true
	}
	if remote > 0 && local == 0 {
		return false
	}

	// Use the lease preferences as a tie breaker. We only really care about the
	// first one, since subsequent lease preferences only apply in edge cases.
	if zone.LeasePreferenceCount() > 0 {
		leasePref := zone.LeasePreference(0)
		for i, n := 0, leasePref.ConstraintCount(); i < n; i++ {
			constraint := leasePref.Constraint(i)
			if isLocal, ok := isConstraintLocal(constraint, localRegion); ok {
				return isLocal
			}
		}
	}

	return false
}

// HasMixOfLocalAndRemotePartitions tests if the given index has at least one
// local and one remote partition as used in the current evaluation context.
// This function also returns the set of local partitions when the number of
// partitions in the index is 2 or greater and the local gateway region can be
// determined.
func HasMixOfLocalAndRemotePartitions(
	evalCtx *tree.EvalContext, index Index,
) (localPartitions *util.FastIntSet, ok bool) {
	if index == nil || index.PartitionCount() < 2 {
		return nil, false
	}
	var localRegion string
	if localRegion, ok = evalCtx.GetLocalRegion(); !ok {
		return nil, false
	}
	foundLocal := false
	foundRemote := false
	localPartitions = &util.FastIntSet{}
	for i, n := 0, index.PartitionCount(); i < n; i++ {
		part := index.Partition(i)
		if IsZoneLocal(part.Zone(), localRegion) {
			foundLocal = true
			localPartitions.Add(i)
		} else {
			foundRemote = true
		}
	}
	return localPartitions, foundLocal && foundRemote
}
