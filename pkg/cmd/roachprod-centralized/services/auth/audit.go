// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

// Audit event types (from design doc)
const (
	// Authentication events
	AuditOktaExchangeStart  = "auth.okta_exchange.start"
	AuditOktaExchangeFinish = "auth.okta_exchange.finish"
	AuditAuthnValidated     = "authn.validated"

	// Token events
	AuditTokenIssued           = "token.issued"
	AuditTokenRevoked          = "token.revoked"
	AuditTokenValidationFailed = "token.validation.failed"
	AuditTokenCleanedUp        = "token.cleaned_up"

	// SCIM user events
	AuditSCIMUserCreated     = "scim.user.created"
	AuditSCIMUserUpdated     = "scim.user.updated"
	AuditSCIMUserDeactivated = "scim.user.deactivated"
	AuditSCIMUserReactivated = "scim.user.reactivated"
	AuditSCIMUserDeleted     = "scim.user.deleted"

	// SCIM group events
	AuditSCIMGroupCreated        = "scim.group.created"
	AuditSCIMGroupUpdated        = "scim.group.updated"
	AuditSCIMGroupDeleted        = "scim.group.deleted"
	AuditSCIMGroupMembersUpdated = "scim.group.members.updated"

	// Service account events
	AuditSACreated = "sa.created"
	AuditSAUpdated = "sa.updated"
	AuditSADeleted = "sa.deleted"

	// Service account origin events
	AuditSAOriginAdded   = "sa.origin.added"
	AuditSAOriginRemoved = "sa.origin.removed"

	// Service account permission events
	AuditSAPermissionAdded     = "sa.permission.added"
	AuditSAPermissionRemoved   = "sa.permission.removed"
	AuditSAPermissionsReplaced = "sa.permissions.replaced"

	// Group permission events (admin)
	AuditGroupPermissionCreated   = "admin.group_permission.created"
	AuditGroupPermissionUpdated   = "admin.group_permission.updated"
	AuditGroupPermissionDeleted   = "admin.group_permission.deleted"
	AuditGroupPermissionsReplaced = "admin.group_permissions.replaced"
)

// auditEvent logs an audit event with structured fields.
// This follows the audit spec from the design doc.
//
// IMPORTANT: No PII (personally identifiable information) should be logged.
// Use IDs, token suffixes, and aggregated data only.
//
// result must be one of: "success", "deny", "error"
func (s *Service) auditEvent(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	eventType string,
	result string,
	details map[string]interface{},
) {
	// Build structured log with all required fields
	fields := []interface{}{
		"event_type", eventType,
		"event_result", result,
	}

	// Ensure details map is not nil
	if details == nil {
		details = make(map[string]interface{})
	}

	// Inject principal identifier if available
	if principal != nil {
		details["principal"] = getPrincipalIdentifier(principal)
	}

	// Add details (caller ensures no PII)
	for k, v := range details {
		fields = append(fields, k, v)
	}

	// Log at appropriate level based on result
	switch result {
	case "success":
		l.Info("audit event", fields...)
	case "deny":
		l.Warn("audit event", fields...)
	case "error":
		l.Error("audit event", fields...)
	default:
		l.Error("audit event with invalid result", append(fields, "invalid_result", result)...)
	}
}
