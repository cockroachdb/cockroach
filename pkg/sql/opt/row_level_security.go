// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opt

import "github.com/cockroachdb/cockroach/pkg/security/username"

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
}

func (r *RowLevelSecurityMeta) MaybeInit(user username.SQLUsername, hasAdminRole bool) {
	if r.IsInitialized {
		return
	}
	r.User = user
	r.HasAdminRole = hasAdminRole
	r.IsInitialized = true
}

// Clear unsets the initialized property. This is used as a test helper.
func (r *RowLevelSecurityMeta) Clear() {
	r = &RowLevelSecurityMeta{}
}
