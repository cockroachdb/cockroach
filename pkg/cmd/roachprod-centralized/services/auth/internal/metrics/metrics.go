// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrics

import (
	"fmt"
	"time"

	configtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const serviceName = "auth"

// Recorder implements IRecorder with Prometheus metrics.
type Recorder struct {
	// Authentication metrics
	oktaExchangeTotal       *prometheus.CounterVec
	oktaExchangeLatency     prometheus.Histogram
	tokenIssuedTotal        *prometheus.CounterVec
	tokenRevokedTotal       *prometheus.CounterVec
	userNotProvisionedTotal prometheus.Counter
	tokenValidationTotal    *prometheus.CounterVec
	tokenValidationLatency  prometheus.Histogram
	authenticationTotal     *prometheus.CounterVec
	authenticationLatency   *prometheus.HistogramVec

	// Authorization metrics
	authzDecisionTotal *prometheus.CounterVec
	authzLatency       *prometheus.HistogramVec

	// SCIM User metrics
	scimRequestsTotal         *prometheus.CounterVec
	scimRequestLatency        *prometheus.HistogramVec
	scimUsersProvisionedTotal prometheus.Counter
	scimUsersDeactivatedTotal prometheus.Counter
	scimUsersReactivatedTotal prometheus.Counter
	scimSyncErrorsTotal       *prometheus.CounterVec

	// SCIM Group metrics
	scimGroupsCreatedTotal       prometheus.Counter
	scimGroupsUpdatedTotal       prometheus.Counter
	scimGroupsDeletedTotal       prometheus.Counter
	scimGroupMembersAddedTotal   prometheus.Counter
	scimGroupMembersRemovedTotal prometheus.Counter

	// Service Account metrics
	serviceAccountsCreatedTotal prometheus.Counter
	serviceAccountsDeletedTotal prometheus.Counter

	// Token cleanup metrics
	tokenCleanupTotal   *prometheus.CounterVec
	tokenCleanupLatency prometheus.Histogram

	// Gauge metrics (periodically updated)
	usersTotal           *prometheus.GaugeVec
	groupsTotal          prometheus.Gauge
	serviceAccountsTotal *prometheus.GaugeVec
	tokensTotal          *prometheus.GaugeVec
}

// NewRecorder creates a new Prometheus metrics recorder for the auth service.
func NewRecorder(registerer prometheus.Registerer, instanceID string) *Recorder {
	factory := promauto.With(registerer)
	namespace := configtypes.MetricsNamespace
	instanceLabels := prometheus.Labels{"instance": instanceID}

	return &Recorder{
		// --- Authentication metrics ---
		oktaExchangeTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_auth_okta_exchange_total", serviceName),
				Help:        "Total Okta token exchanges by result",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
			[]string{"result"},
		),
		oktaExchangeLatency: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:        fmt.Sprintf("%s_auth_okta_exchange_latency_seconds", serviceName),
				Help:        "Latency of Okta token exchange operations",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
				Buckets:     prometheus.ExponentialBuckets(0.01, 2, 10), // 10ms to ~10s
			},
		),
		tokenIssuedTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_auth_token_issued_total", serviceName),
				Help:        "Total tokens issued by principal type",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
			[]string{"principal_type"},
		),
		tokenRevokedTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_auth_token_revoked_total", serviceName),
				Help:        "Total tokens revoked by reason",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
			[]string{"reason"},
		),
		userNotProvisionedTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_auth_user_not_provisioned_total", serviceName),
				Help:        "Auth failures due to unprovisioned users",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
		),
		tokenValidationTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_auth_token_validation_total", serviceName),
				Help:        "Token validations by result",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
			[]string{"result"}, // success, expired, revoked, invalid, user_inactive
		),
		tokenValidationLatency: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:        fmt.Sprintf("%s_auth_token_validation_latency_seconds", serviceName),
				Help:        "Latency of token validation operations",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
				Buckets:     prometheus.ExponentialBuckets(0.0001, 2, 12), // 0.1ms to ~400ms
			},
		),
		authenticationTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_auth_total", serviceName),
				Help:        "Authentication attempts by result and auth method",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
			[]string{"result", "auth_method"}, // result: success, error; auth_method: user, service-account, jwt
		),
		authenticationLatency: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:        fmt.Sprintf("%s_auth_latency_seconds", serviceName),
				Help:        "Full authentication latency including token validation and permission loading",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
				Buckets:     prometheus.ExponentialBuckets(0.001, 2, 12), // 1ms to ~4s
			},
			[]string{"result", "auth_method"},
		),

		// --- Authorization metrics ---
		authzDecisionTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_authz_decision_total", serviceName),
				Help:        "Authorization decisions by result and context",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
			[]string{"result", "reason", "endpoint", "provider"},
		),
		authzLatency: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:        fmt.Sprintf("%s_authz_latency_seconds", serviceName),
				Help:        "Authorization latency by endpoint",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
				Buckets:     prometheus.ExponentialBuckets(0.0001, 2, 12),
			},
			[]string{"endpoint"},
		),

		// --- SCIM User metrics ---
		scimRequestsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_scim_requests_total", serviceName),
				Help:        "SCIM requests by operation and result",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
			[]string{"operation", "result"},
		),
		scimRequestLatency: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:        fmt.Sprintf("%s_scim_request_latency_seconds", serviceName),
				Help:        "SCIM request latency by operation",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
				Buckets:     prometheus.ExponentialBuckets(0.001, 2, 12),
			},
			[]string{"operation"},
		),
		scimUsersProvisionedTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_scim_users_provisioned_total", serviceName),
				Help:        "Total users provisioned via SCIM",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
		),
		scimUsersDeactivatedTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_scim_users_deactivated_total", serviceName),
				Help:        "Total users deactivated via SCIM",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
		),
		scimUsersReactivatedTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_scim_users_reactivated_total", serviceName),
				Help:        "Total users reactivated via SCIM",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
		),
		scimSyncErrorsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_scim_sync_errors_total", serviceName),
				Help:        "SCIM sync errors by type",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
			[]string{"error_type"},
		),

		// --- SCIM Group metrics ---
		scimGroupsCreatedTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_scim_groups_created_total", serviceName),
				Help:        "Total groups created via SCIM",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
		),
		scimGroupsUpdatedTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_scim_groups_updated_total", serviceName),
				Help:        "Total groups updated via SCIM (PUT/PATCH)",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
		),
		scimGroupsDeletedTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_scim_groups_deleted_total", serviceName),
				Help:        "Total groups deleted via SCIM",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
		),
		scimGroupMembersAddedTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_scim_group_members_added_total", serviceName),
				Help:        "Total group membership additions via SCIM",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
		),
		scimGroupMembersRemovedTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_scim_group_members_removed_total", serviceName),
				Help:        "Total group membership removals via SCIM",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
		),

		// --- Service Account metrics ---
		serviceAccountsCreatedTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_service_accounts_created_total", serviceName),
				Help:        "Total service accounts created",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
		),
		serviceAccountsDeletedTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_service_accounts_deleted_total", serviceName),
				Help:        "Total service accounts deleted",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
		),

		// --- Token cleanup metrics ---
		tokenCleanupTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_token_cleanup_total", serviceName),
				Help:        "Tokens cleaned up by status",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
			[]string{"status"},
		),
		tokenCleanupLatency: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:        fmt.Sprintf("%s_token_cleanup_latency_seconds", serviceName),
				Help:        "Latency of token cleanup operations",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
				Buckets:     prometheus.ExponentialBuckets(0.01, 2, 8),
			},
		),

		// --- Gauge metrics (current state) ---
		usersTotal: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        fmt.Sprintf("%s_users_total", serviceName),
				Help:        "Current number of users by status",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
			[]string{"status"}, // active, inactive
		),
		groupsTotal: factory.NewGauge(
			prometheus.GaugeOpts{
				Name:        fmt.Sprintf("%s_groups_total", serviceName),
				Help:        "Current number of groups",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
		),
		serviceAccountsTotal: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        fmt.Sprintf("%s_service_accounts_total", serviceName),
				Help:        "Current number of service accounts by enabled status",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
			[]string{"enabled"}, // true, false
		),
		tokensTotal: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        fmt.Sprintf("%s_tokens_total", serviceName),
				Help:        "Current number of tokens by type and status",
				Namespace:   namespace,
				ConstLabels: instanceLabels,
			},
			[]string{"type", "status"}, // type: user, service-account; status: valid, revoked
		),
	}
}

// --- IRecorder implementation ---

func (r *Recorder) RecordOktaExchange(result string, latency time.Duration) {
	r.oktaExchangeTotal.WithLabelValues(result).Inc()
	r.oktaExchangeLatency.Observe(latency.Seconds())
}

func (r *Recorder) RecordTokenIssued(principalType string) {
	r.tokenIssuedTotal.WithLabelValues(principalType).Inc()
}

func (r *Recorder) RecordTokenRevoked(reason string) {
	r.tokenRevokedTotal.WithLabelValues(reason).Inc()
}

func (r *Recorder) RecordUserNotProvisioned() {
	r.userNotProvisionedTotal.Inc()
}

func (r *Recorder) RecordTokenValidation(result string, latency time.Duration) {
	r.tokenValidationTotal.WithLabelValues(result).Inc()
	r.tokenValidationLatency.Observe(latency.Seconds())
}

func (r *Recorder) RecordAuthentication(result, authMethod string, latency time.Duration) {
	r.authenticationTotal.WithLabelValues(result, authMethod).Inc()
	r.authenticationLatency.WithLabelValues(result, authMethod).Observe(latency.Seconds())
}

func (r *Recorder) RecordAuthzDecision(result, reason, endpoint, provider string) {
	r.authzDecisionTotal.WithLabelValues(result, reason, endpoint, provider).Inc()
}

func (r *Recorder) RecordAuthzLatency(endpoint string, latency time.Duration) {
	r.authzLatency.WithLabelValues(endpoint).Observe(latency.Seconds())
}

func (r *Recorder) RecordSCIMRequest(operation, result string, latency time.Duration) {
	r.scimRequestsTotal.WithLabelValues(operation, result).Inc()
	r.scimRequestLatency.WithLabelValues(operation).Observe(latency.Seconds())
}

func (r *Recorder) RecordSCIMUserProvisioned() {
	r.scimUsersProvisionedTotal.Inc()
}

func (r *Recorder) RecordSCIMUserDeactivated() {
	r.scimUsersDeactivatedTotal.Inc()
}

func (r *Recorder) RecordSCIMUserReactivated() {
	r.scimUsersReactivatedTotal.Inc()
}

func (r *Recorder) RecordSCIMError(errorType string) {
	r.scimSyncErrorsTotal.WithLabelValues(errorType).Inc()
}

func (r *Recorder) RecordSCIMGroupCreated() {
	r.scimGroupsCreatedTotal.Inc()
}

func (r *Recorder) RecordSCIMGroupUpdated() {
	r.scimGroupsUpdatedTotal.Inc()
}

func (r *Recorder) RecordSCIMGroupDeleted() {
	r.scimGroupsDeletedTotal.Inc()
}

func (r *Recorder) RecordSCIMGroupMemberAdded(count int) {
	r.scimGroupMembersAddedTotal.Add(float64(count))
}

func (r *Recorder) RecordSCIMGroupMemberRemoved(count int) {
	r.scimGroupMembersRemovedTotal.Add(float64(count))
}

func (r *Recorder) RecordServiceAccountCreated() {
	r.serviceAccountsCreatedTotal.Inc()
}

func (r *Recorder) RecordServiceAccountDeleted() {
	r.serviceAccountsDeletedTotal.Inc()
}

func (r *Recorder) RecordTokenCleanup(status string, count int, latency time.Duration) {
	r.tokenCleanupTotal.WithLabelValues(status).Add(float64(count))
	r.tokenCleanupLatency.Observe(latency.Seconds())
}

func (r *Recorder) SetUsersTotal(active, inactive int) {
	r.usersTotal.WithLabelValues("active").Set(float64(active))
	r.usersTotal.WithLabelValues("inactive").Set(float64(inactive))
}

func (r *Recorder) SetGroupsTotal(count int) {
	r.groupsTotal.Set(float64(count))
}

func (r *Recorder) SetServiceAccountsTotal(enabled, disabled int) {
	r.serviceAccountsTotal.WithLabelValues("true").Set(float64(enabled))
	r.serviceAccountsTotal.WithLabelValues("false").Set(float64(disabled))
}

func (r *Recorder) SetTokensTotal(tokenType, status string, count int) {
	r.tokensTotal.WithLabelValues(tokenType, status).Set(float64(count))
}

func (r *Recorder) ResetTokensTotal() {
	// Reset known combinations to 0 before update
	for _, tokenType := range []string{"user", "service-account"} {
		for _, status := range []string{"valid", "revoked"} {
			r.tokensTotal.WithLabelValues(tokenType, status).Set(0)
		}
	}
}
