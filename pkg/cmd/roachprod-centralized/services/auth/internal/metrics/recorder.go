// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrics

import "time"

// IRecorder defines the interface for recording auth service metrics.
// This interface is implemented by Recorder and can be mocked in tests.
type IRecorder interface {
	// Authentication
	RecordOktaExchange(result string, latency time.Duration)
	RecordTokenIssued(principalType string)
	RecordTokenRevoked(reason string)
	RecordUserNotProvisioned()
	RecordTokenValidation(result string, latency time.Duration)
	RecordAuthentication(result, authMethod string, latency time.Duration)

	// Authorization
	RecordAuthzDecision(result, reason, endpoint, provider string)
	RecordAuthzLatency(endpoint string, latency time.Duration)

	// SCIM Users
	RecordSCIMRequest(operation, result string, latency time.Duration)
	RecordSCIMUserProvisioned()
	RecordSCIMUserDeactivated()
	RecordSCIMUserReactivated()
	RecordSCIMError(errorType string)

	// SCIM Groups
	RecordSCIMGroupCreated()
	RecordSCIMGroupUpdated()
	RecordSCIMGroupDeleted()
	RecordSCIMGroupMemberAdded(count int)
	RecordSCIMGroupMemberRemoved(count int)

	// Service Accounts
	RecordServiceAccountCreated()
	RecordServiceAccountDeleted()

	// Token Cleanup
	RecordTokenCleanup(status string, count int, latency time.Duration)

	// Gauge updates (called periodically)
	SetUsersTotal(active, inactive int)
	SetGroupsTotal(count int)
	SetServiceAccountsTotal(enabled, disabled int)
	SetTokensTotal(tokenType, status string, count int)
	ResetTokensTotal() // Reset all token gauges before update
}

// NoOpRecorder is a no-op implementation of IRecorder for when metrics are disabled.
type NoOpRecorder struct{}

func (n *NoOpRecorder) RecordOktaExchange(string, time.Duration)           {}
func (n *NoOpRecorder) RecordTokenIssued(string)                           {}
func (n *NoOpRecorder) RecordTokenRevoked(string)                          {}
func (n *NoOpRecorder) RecordUserNotProvisioned()                          {}
func (n *NoOpRecorder) RecordTokenValidation(string, time.Duration)        {}
func (n *NoOpRecorder) RecordAuthentication(string, string, time.Duration) {}
func (n *NoOpRecorder) RecordAuthzDecision(string, string, string, string) {}
func (n *NoOpRecorder) RecordAuthzLatency(string, time.Duration)           {}
func (n *NoOpRecorder) RecordSCIMRequest(string, string, time.Duration)    {}
func (n *NoOpRecorder) RecordSCIMUserProvisioned()                         {}
func (n *NoOpRecorder) RecordSCIMUserDeactivated()                         {}
func (n *NoOpRecorder) RecordSCIMUserReactivated()                         {}
func (n *NoOpRecorder) RecordSCIMError(string)                             {}
func (n *NoOpRecorder) RecordSCIMGroupCreated()                            {}
func (n *NoOpRecorder) RecordSCIMGroupUpdated()                            {}
func (n *NoOpRecorder) RecordSCIMGroupDeleted()                            {}
func (n *NoOpRecorder) RecordSCIMGroupMemberAdded(int)                     {}
func (n *NoOpRecorder) RecordSCIMGroupMemberRemoved(int)                   {}
func (n *NoOpRecorder) RecordServiceAccountCreated()                       {}
func (n *NoOpRecorder) RecordServiceAccountDeleted()                       {}
func (n *NoOpRecorder) RecordTokenCleanup(string, int, time.Duration)      {}
func (n *NoOpRecorder) SetUsersTotal(int, int)                             {}
func (n *NoOpRecorder) SetGroupsTotal(int)                                 {}
func (n *NoOpRecorder) SetServiceAccountsTotal(int, int)                   {}
func (n *NoOpRecorder) SetTokensTotal(string, string, int)                 {}
func (n *NoOpRecorder) ResetTokensTotal()                                  {}
