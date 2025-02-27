// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cat

import (
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// PolicyCommandScope specifies the scope of SQL commands to which a policy applies.
// It determines whether a policy is enforced for specific operations or if an operation
// is exempt from row-level security. The operations checked must align with the policy
// commands defined in the CREATE POLICY SQL syntax.
type PolicyCommandScope uint8

const (
	// PolicyScopeSelect indicates that the policy applies to SELECT operations.
	PolicyScopeSelect PolicyCommandScope = iota
	// PolicyScopeInsert indicates that the policy applies to INSERT operations.
	PolicyScopeInsert
	// PolicyScopeUpdate indicates that the policy applies to UPDATE operations.
	PolicyScopeUpdate
	// PolicyScopeDelete indicates that the policy applies to DELETE operations.
	PolicyScopeDelete
	// PolicyScopeExempt indicates that the operation is exempt from row-level security policies.
	PolicyScopeExempt
)

// Policy defines an interface for a row-level security (RLS) policy on a table.
// Policies use expressions to filter rows during read operations and/or restrict
// new rows during write operations.
type Policy struct {
	// Name is the name of the policy. The name is unique within a table
	// and cannot be qualified.
	Name tree.Name
	// ID is the unique identifier for this policy within the table.
	ID descpb.PolicyID
	// UsingExpr is the optional filter expression evaluated on rows during
	// read operations. If the policy does not define a USING expression, this is
	// an empty string.
	UsingExpr string
	// WithCheckExpr is the optional validation expression applied to new rows
	// during write operations. If the policy does not define a WITH CHECK expression,
	// this is an empty string.
	WithCheckExpr string
	// Command is the command that the policy was defined for.
	Command catpb.PolicyCommand
	// roles are the roles the applies to. If the policy applies to all roles (aka
	// public), this will be nil.
	roles map[string]struct{}
}

// Policies contains the policies for a single table.
type Policies struct {
	Permissive  []Policy
	Restrictive []Policy
}

// InitRoles builds up the list of roles in the policy.
func (p *Policy) InitRoles(roleNames []string) {
	if len(roleNames) == 0 {
		p.roles = nil
		return
	}
	roles := make(map[string]struct{})
	for _, r := range roleNames {
		if r == username.PublicRole {
			// If the public role is defined, there is no need to check the
			// remaining roles since the policy applies to everyone. We will clear
			// out the roles map to signal that all roles apply.
			roles = nil
			break
		}
		roles[r] = struct{}{}
	}
	p.roles = roles
}

// AppliesToRole checks whether the policy applies to the give role.
func (p *Policy) AppliesToRole(user username.SQLUsername) bool {
	// If no roles are specified, assume the policy applies to all users (public role).
	if p.roles == nil {
		return true
	}
	_, found := p.roles[user.Normalized()]
	return found
}
