// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// User represents a human user in the system.
// SCIM tags map SCIM 2.0 field names to internal database fields for user provisioning.
type User struct {
	ID          uuid.UUID  `json:"id" db:"id" scim:"id"`
	OktaUserID  string     `json:"okta_user_id" db:"okta_user_id" scim:"externalId" binding:"required"`
	Email       string     `json:"email" db:"email" scim:"userName,emails" binding:"required,email"`
	SlackHandle string     `json:"slack_handle,omitempty" db:"slack_handle"`
	FullName    string     `json:"full_name,omitempty" db:"full_name" scim:"name.formatted,displayName"`
	Active      bool       `json:"active" db:"active" scim:"active"`
	CreatedAt   time.Time  `json:"created_at" db:"created_at" scim:"meta.created"`
	UpdatedAt   time.Time  `json:"updated_at" db:"updated_at" scim:"meta.lastModified"`
	LastLoginAt *time.Time `json:"last_login_at,omitempty" db:"last_login_at"`
}

// ServiceAccount represents a machine account.
type ServiceAccount struct {
	ID            uuid.UUID  `json:"id" db:"id"`
	Name          string     `json:"name" db:"name" binding:"required"`
	Description   string     `json:"description,omitempty" db:"description"`
	CreatedBy     *uuid.UUID `json:"created_by,omitempty" db:"created_by"`
	CreatedAt     time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at" db:"updated_at"`
	Enabled       bool       `json:"enabled" db:"enabled"`
	DelegatedFrom *uuid.UUID `json:"delegated_from,omitempty" db:"delegated_from"`
}

// TokenType represents the type of the token (user or service_account).
type TokenType string

const (
	TokenTypeUser           TokenType = "user"
	TokenTypeServiceAccount TokenType = "service-account"
)

// TokenStatus represents the status of the token.
type TokenStatus string

const (
	TokenStatusValid   TokenStatus = "valid"
	TokenStatusRevoked TokenStatus = "revoked"
)

// ApiToken represents an opaque token used for authentication.
type ApiToken struct {
	ID               uuid.UUID   `json:"id" db:"id"`
	TokenHash        string      `json:"-" db:"token_hash"`              // Never return hash in JSON
	TokenSuffix      string      `json:"token_suffix" db:"token_suffix"` // Last 8 chars for display in audit logs
	TokenType        TokenType   `json:"token_type" db:"token_type"`
	UserID           *uuid.UUID  `json:"user_id,omitempty" db:"user_id"`
	ServiceAccountID *uuid.UUID  `json:"service_account_id,omitempty" db:"service_account_id"`
	Status           TokenStatus `json:"status" db:"status"`
	CreatedAt        time.Time   `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time   `json:"updated_at" db:"updated_at"`
	ExpiresAt        time.Time   `json:"expires_at" db:"expires_at"`
	ActivatedAt      *time.Time  `json:"activated_at,omitempty" db:"activated_at"`
	LastUsedAt       *time.Time  `json:"last_used_at,omitempty" db:"last_used_at"`
}

// Group represents a group provisioned via SCIM.
// Groups contain members (users) and are used to derive permissions via GroupPermission mappings.
type Group struct {
	ID          uuid.UUID `json:"id" db:"id" scim:"id"`
	ExternalID  *string   `json:"external_id" db:"external_id" scim:"externalId"`
	DisplayName string    `json:"display_name" db:"display_name" scim:"displayName" binding:"required"`
	CreatedAt   time.Time `json:"created_at" db:"created_at" scim:"meta.created"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at" scim:"meta.lastModified"`
}

// GroupMember represents a user's membership in a group.
// Managed via SCIM PATCH /Groups/:id endpoint with atomic operations.
// SCIM tags map SCIM 2.0 member reference field names to database columns.
type GroupMember struct {
	ID        uuid.UUID `json:"id" db:"id"`
	GroupID   uuid.UUID `json:"group_id" db:"group_id" binding:"required"`
	UserID    uuid.UUID `json:"user_id" db:"user_id" scim:"value" binding:"required"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// GroupPermission represents a mapping from a group to a specific permission.
// Multiple mappings can exist for the same group, each granting a different permission.
// Permissions are resolved at authorization time by joining group_members → groups → group_permissions.
type GroupPermission struct {
	ID         uuid.UUID `json:"id" db:"id"`
	GroupName  string    `json:"group_name" db:"group_name" binding:"required"`
	Scope      string    `json:"scope" db:"scope" binding:"required"`
	Permission string    `json:"permission" db:"permission" binding:"required"`
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
	UpdatedAt  time.Time `json:"updated_at" db:"updated_at"`
}

// UserPermission represents a specific permission granted to a user.
type UserPermission struct {
	ID         uuid.UUID `json:"id" db:"id"`
	UserID     uuid.UUID `json:"user_id" db:"user_id" binding:"required"`
	Scope      string    `json:"scope" db:"scope" binding:"required"`
	Permission string    `json:"permission" db:"permission" binding:"required"`
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
	UpdatedAt  time.Time `json:"updated_at" db:"updated_at"`
}

// ServiceAccountOrigin represents an allowed IP range for a service account.
type ServiceAccountOrigin struct {
	ID               uuid.UUID  `json:"id" db:"id"`
	ServiceAccountID uuid.UUID  `json:"service_account_id" db:"service_account_id" binding:"required"`
	CIDR             string     `json:"cidr" db:"cidr" binding:"required,cidr"` // Stored as INET in DB, string here
	Description      string     `json:"description,omitempty" db:"description"`
	CreatedBy        *uuid.UUID `json:"created_by,omitempty" db:"created_by"`
	CreatedAt        time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at" db:"updated_at"`
}

// ServiceAccountPermission represents a specific permission granted to a service account.
type ServiceAccountPermission struct {
	ID               uuid.UUID `json:"id" db:"id"`
	ServiceAccountID uuid.UUID `json:"service_account_id" db:"service_account_id" binding:"required"`
	Scope            string    `json:"scope" db:"scope" binding:"required"`
	Permission       string    `json:"permission" db:"permission" binding:"required"`
	CreatedAt        time.Time `json:"created_at" db:"created_at"`
}

// Permission represents a permission grant with a scope.
// Both UserPermission and ServiceAccountPermission implement this interface,
// allowing unified permission checking logic in the auth package.
type Permission interface {
	GetScope() string
	GetPermission() string
}

// Compile-time interface verification.
var _ Permission = (*UserPermission)(nil)
var _ Permission = (*ServiceAccountPermission)(nil)

func (p *UserPermission) GetScope() string      { return p.Scope }
func (p *UserPermission) GetPermission() string { return p.Permission }

func (p *ServiceAccountPermission) GetScope() string      { return p.Scope }
func (p *ServiceAccountPermission) GetPermission() string { return p.Permission }
