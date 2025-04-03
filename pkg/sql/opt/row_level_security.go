// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opt

import (
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

// RowLevelSecurityMeta contains metadata pertaining to row-level security
// policies that were enforced when building the query plan.
type RowLevelSecurityMeta struct {
	// IsInitialized indicates that the struct has been initialized. This gets
	// lazily initialized, only when the query plan building comes across a table
	// that is enabled for row-level security.
	IsInitialized bool

	// User is the user that constructed the metadata. This is important since
	// RLS policies differ based on the role of the user executing the query.
	User username.SQLUsername

	// HasAdminRole is true if the current user was part of the admin role when
	// creating the query plan.
	HasAdminRole bool

	// NoPoliciesApplied is set to true if one of the tables didn't have any
	// applicable policies and so all rows are filtered out.
	NoPoliciesApplied bool

	// PoliciesApplied is the set of policies that were applied for each relation
	// in the query.
	PoliciesApplied map[TableID]PoliciesApplied
}

func (r *RowLevelSecurityMeta) MaybeInit(user username.SQLUsername, hasAdminRole bool) {
	if r.IsInitialized {
		return
	}
	r.User = user
	r.HasAdminRole = hasAdminRole
	r.PoliciesApplied = make(map[TableID]PoliciesApplied)
	r.IsInitialized = true
}

// Clear unsets the initialized property. This is used as a test helper.
func (r *RowLevelSecurityMeta) Clear() {
	r = &RowLevelSecurityMeta{}
}

// AddTableUse indicates that an RLS-enabled table was encountered while
// building the query plan. If any policies are in use, they will be added
// via the AddPolicyUse call.
func (r *RowLevelSecurityMeta) AddTableUse(tableID TableID, isTableOwnerAndNotForced bool) {
	if _, found := r.PoliciesApplied[tableID]; !found {
		r.PoliciesApplied[tableID] = PoliciesApplied{
			NoForceExempt: isTableOwnerAndNotForced,
			Filter:        PolicyIDSet{},
			Check:         PolicyIDSet{},
		}
	}
}

// AddPoliciesUsed indicates that the given set of policy IDs for a table were
// applied in the query. The applyFilterExpr parameter determines if the policies
// were used to filter existing rows. If false, it implies the policy was used
// to build a check constraint.
func (r *RowLevelSecurityMeta) AddPoliciesUsed(
	tableID TableID, policies PolicyIDSet, applyFilterExpr bool,
) {
	a := r.PoliciesApplied[tableID]
	if applyFilterExpr {
		s := r.PoliciesApplied[tableID].Filter
		a.Filter = s.Union(policies)
	} else {
		s := r.PoliciesApplied[tableID].Check
		a.Check = s.Union(policies)
	}
	r.PoliciesApplied[tableID] = a
}

// PoliciesApplied stores the set of policies that were applied to a table.
type PoliciesApplied struct {
	// NoForceExempt is true if the policies were exempt because they were the
	// table owner and force RLS wasn't set.
	NoForceExempt bool
	// Filter is the set of policy IDs that were applied to filter out existing
	// rows. The USING expression in each policy is used to derive the filter.
	Filter PolicyIDSet
	// Check is the set of policy IDs that were applied as check constraints to
	// block new rows that violate the policy. The check constraint was built
	// with the WITH CHECK expression or USING expression of each policy.
	Check PolicyIDSet
}

func (p *PoliciesApplied) Copy() PoliciesApplied {
	return PoliciesApplied{
		NoForceExempt: p.NoForceExempt,
		Filter:        p.Filter.Copy(),
		Check:         p.Check.Copy(),
	}
}

// PolicyIDSet stores an unordered set of policy ids.
type PolicyIDSet struct {
	set intsets.Fast
}

// Add adds an id to the set. No-op if the id is already in the set.
func (s *PolicyIDSet) Add(id descpb.PolicyID) {
	s.set.Add(int(id))
}

// Union returns the union of d and o as a new set.
func (s PolicyIDSet) Union(other PolicyIDSet) PolicyIDSet {
	return PolicyIDSet{
		set: s.set.Union(other.set),
	}
}

// Len returns the number of the policyIDs in the set.
func (s PolicyIDSet) Len() int {
	return s.set.Len()
}

// Copy makes a deep copy of the set.
func (s PolicyIDSet) Copy() PolicyIDSet {
	return PolicyIDSet{set: s.set.Copy()}
}

// Contains checks if the set contains the given id.
func (s PolicyIDSet) Contains(id descpb.PolicyID) bool {
	return s.set.Contains(int(id))
}
