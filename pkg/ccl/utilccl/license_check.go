// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utilccl

// TestingEnableEnterprise allows overriding the license check in tests. This
// function was deprecated when the core license was removed. We no longer
// distinguish between features enabled only for enterprise. All features are
// enabled, and if a license policy is violated, we throttle connections.
// Callers can safely remove any reference to this function.
//
// Deprecated
func TestingEnableEnterprise() func() {
	return func() {}
}

// TestingDisableEnterprise allows re-enabling the license check in tests.
//
// See description in TestingEnableEnterprise for rationale about deprecation.
//
// Deprecated
func TestingDisableEnterprise() func() {
	return func() {}
}
