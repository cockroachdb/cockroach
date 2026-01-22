// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecorderRecordsMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	recorder := NewRecorder(registry, "test-instance")

	// Test authentication metrics
	recorder.RecordOktaExchange("success", 100*time.Millisecond)
	recorder.RecordTokenIssued("user")
	recorder.RecordTokenRevoked("admin")
	recorder.RecordUserNotProvisioned()
	recorder.RecordTokenValidation("success", 50*time.Millisecond)

	// Test authorization metrics
	recorder.RecordAuthzDecision("allow", "", "/api/clusters", "jwt")
	recorder.RecordAuthzLatency("/api/clusters", 10*time.Millisecond)

	// Test SCIM user metrics
	recorder.RecordSCIMRequest("create_user", "success", 200*time.Millisecond)
	recorder.RecordSCIMUserProvisioned()
	recorder.RecordSCIMUserDeactivated()
	recorder.RecordSCIMUserReactivated()
	recorder.RecordSCIMError("validation")

	// Test SCIM group metrics
	recorder.RecordSCIMGroupCreated()
	recorder.RecordSCIMGroupUpdated()
	recorder.RecordSCIMGroupDeleted()
	recorder.RecordSCIMGroupMemberAdded(5)
	recorder.RecordSCIMGroupMemberRemoved(3)

	// Test service account metrics
	recorder.RecordServiceAccountCreated()
	recorder.RecordServiceAccountDeleted()

	// Test token cleanup metrics
	recorder.RecordTokenCleanup("revoked", 10, 500*time.Millisecond)

	// Test gauge metrics
	recorder.SetUsersTotal(100, 5)
	recorder.SetGroupsTotal(20)
	recorder.SetServiceAccountsTotal(15, 3)
	recorder.SetTokensTotal("user", "valid", 50)
	recorder.SetTokensTotal("service-account", "revoked", 2)
	recorder.ResetTokensTotal()

	// Gather and verify metrics exist
	families, err := registry.Gather()
	require.NoError(t, err)
	assert.NotEmpty(t, families, "expected metrics to be registered")

	// Verify we have a reasonable number of metric families
	// (counters, histograms, and gauges)
	assert.Greater(t, len(families), 20, "expected at least 20 metric families")
}

func TestNoOpRecorderDoesNotPanic(t *testing.T) {
	recorder := &NoOpRecorder{}

	assert.NotPanics(t, func() {
		// Test all authentication methods
		recorder.RecordOktaExchange("success", time.Second)
		recorder.RecordTokenIssued("user")
		recorder.RecordTokenRevoked("admin")
		recorder.RecordUserNotProvisioned()
		recorder.RecordTokenValidation("success", time.Millisecond)

		// Test all authorization methods
		recorder.RecordAuthzDecision("allow", "", "/api/test", "jwt")
		recorder.RecordAuthzLatency("/api/test", time.Millisecond)

		// Test all SCIM user methods
		recorder.RecordSCIMRequest("create_user", "success", time.Second)
		recorder.RecordSCIMUserProvisioned()
		recorder.RecordSCIMUserDeactivated()
		recorder.RecordSCIMUserReactivated()
		recorder.RecordSCIMError("validation")

		// Test all SCIM group methods
		recorder.RecordSCIMGroupCreated()
		recorder.RecordSCIMGroupUpdated()
		recorder.RecordSCIMGroupDeleted()
		recorder.RecordSCIMGroupMemberAdded(5)
		recorder.RecordSCIMGroupMemberRemoved(3)

		// Test all service account methods
		recorder.RecordServiceAccountCreated()
		recorder.RecordServiceAccountDeleted()

		// Test token cleanup methods
		recorder.RecordTokenCleanup("revoked", 10, time.Second)

		// Test all gauge methods
		recorder.SetUsersTotal(100, 5)
		recorder.SetGroupsTotal(20)
		recorder.SetServiceAccountsTotal(15, 3)
		recorder.SetTokensTotal("user", "valid", 50)
		recorder.ResetTokensTotal()
	})
}

func TestRecorderImplementsInterface(t *testing.T) {
	registry := prometheus.NewRegistry()
	recorder := NewRecorder(registry, "test-instance")

	// Verify that Recorder implements IRecorder interface
	var _ IRecorder = recorder

	// Verify that NoOpRecorder implements IRecorder interface
	var _ IRecorder = &NoOpRecorder{}
}

func TestMetricsLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	recorder := NewRecorder(registry, "test-instance")

	// Test different label values
	testCases := []struct {
		name string
		fn   func()
	}{
		{
			name: "okta exchange results",
			fn: func() {
				recorder.RecordOktaExchange("success", time.Millisecond)
				recorder.RecordOktaExchange("error", time.Millisecond)
			},
		},
		{
			name: "token validation results",
			fn: func() {
				recorder.RecordTokenValidation("success", time.Millisecond)
				recorder.RecordTokenValidation("expired", time.Millisecond)
				recorder.RecordTokenValidation("revoked", time.Millisecond)
				recorder.RecordTokenValidation("invalid", time.Millisecond)
				recorder.RecordTokenValidation("user_inactive", time.Millisecond)
			},
		},
		{
			name: "principal types",
			fn: func() {
				recorder.RecordTokenIssued("user")
				recorder.RecordTokenIssued("service-account")
			},
		},
		{
			name: "revocation reasons",
			fn: func() {
				recorder.RecordTokenRevoked("self")
				recorder.RecordTokenRevoked("admin")
				recorder.RecordTokenRevoked("user_deactivated")
				recorder.RecordTokenRevoked("cleanup")
			},
		},
		{
			name: "SCIM operations",
			fn: func() {
				recorder.RecordSCIMRequest("create_user", "success", time.Millisecond)
				recorder.RecordSCIMRequest("patch_user", "error", time.Millisecond)
				recorder.RecordSCIMRequest("delete_group", "success", time.Millisecond)
			},
		},
		{
			name: "authorization decisions",
			fn: func() {
				recorder.RecordAuthzDecision("allow", "", "/api/test", "jwt")
				recorder.RecordAuthzDecision("deny", "insufficient_permissions", "/api/admin", "bearer")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.NotPanics(t, tc.fn)
		})
	}

	// Verify metrics were recorded
	families, err := registry.Gather()
	require.NoError(t, err)
	assert.NotEmpty(t, families)
}
