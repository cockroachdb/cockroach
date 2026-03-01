// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"testing"

	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
)

// TestPrincipal_HasPermission tests basic permission checking (any scope)
func TestPrincipal_HasPermission(t *testing.T) {
	tests := []struct {
		name       string
		principal  *Principal
		permission string
		expected   bool
	}{
		{
			name: "user has permission with specific scope",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:create",
					},
				},
			},
			permission: "clusters:create",
			expected:   true,
		},
		{
			name: "user has permission with wildcard scope",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "*",
						Permission: "admin",
					},
				},
			},
			permission: "admin",
			expected:   true,
		},
		{
			name: "user does not have permission",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:create",
					},
				},
			},
			permission: "clusters:delete",
			expected:   false,
		},
		{
			name: "service account has permission",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.ServiceAccountPermission{
						Scope:      "aws-staging",
						Permission: "scim:manage-user",
					},
				},
			},
			permission: "scim:manage-user",
			expected:   true,
		},
		{
			name: "empty permissions",
			principal: &Principal{
				Permissions: []authmodels.Permission{},
			},
			permission: "clusters:create",
			expected:   false,
		},
		{
			name: "permission with colons in name",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "tokens:mine:view",
					},
				},
			},
			permission: "tokens:mine:view",
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.principal.HasPermission(tt.permission)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestPrincipal_HasPermissionScoped tests scoped permission checking
func TestPrincipal_HasPermissionScoped(t *testing.T) {
	tests := []struct {
		name       string
		principal  *Principal
		permission string
		scope      string
		expected   bool
	}{
		{
			name: "exact match",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:create",
					},
				},
			},
			permission: "clusters:create",
			scope:      "gcp-engineering",
			expected:   true,
		},
		{
			name: "wildcard scope in principal",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "*",
						Permission: "clusters:create",
					},
				},
			},
			permission: "clusters:create",
			scope:      "gcp-engineering",
			expected:   true,
		},
		{
			name: "wildcard scope in request",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:create",
					},
				},
			},
			permission: "clusters:create",
			scope:      "*",
			expected:   false,
		},
		{
			name: "wildcard scope in both",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "*",
						Permission: "admin",
					},
				},
			},
			permission: "admin",
			scope:      "*",
			expected:   true,
		},
		{
			name: "scope mismatch",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:create",
					},
				},
			},
			permission: "clusters:create",
			scope:      "aws-staging",
			expected:   false,
		},
		{
			name: "permission mismatch",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:create",
					},
				},
			},
			permission: "clusters:delete",
			scope:      "gcp-engineering",
			expected:   false,
		},
		{
			name: "service account with explicit scope",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.ServiceAccountPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:create",
					},
				},
			},
			permission: "clusters:create",
			scope:      "gcp-engineering",
			expected:   true,
		},
		{
			name: "multiple permissions - finds match",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "aws-staging",
						Permission: "clusters:read",
					},
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:create",
					},
					&authmodels.UserPermission{
						Scope:      "azure-prod",
						Permission: "clusters:delete",
					},
				},
			},
			permission: "clusters:create",
			scope:      "gcp-engineering",
			expected:   true,
		},
		{
			name: "permission with colons - scoped check",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "tokens:mine:view",
					},
				},
			},
			permission: "tokens:mine:view",
			scope:      "gcp-engineering",
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.principal.HasPermissionScoped(tt.permission, tt.scope)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestPrincipal_HasAnyPermission tests OR logic for permissions
func TestPrincipal_HasAnyPermission(t *testing.T) {
	tests := []struct {
		name        string
		principal   *Principal
		permissions []string
		expected    bool
	}{
		{
			name: "has one of multiple permissions",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:create",
					},
				},
			},
			permissions: []string{"clusters:delete", "clusters:create", "clusters:update"},
			expected:    true,
		},
		{
			name: "has multiple of the requested permissions",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:create",
					},
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:delete",
					},
				},
			},
			permissions: []string{"clusters:delete", "clusters:create"},
			expected:    true,
		},
		{
			name: "has none of the permissions",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:read",
					},
				},
			},
			permissions: []string{"clusters:create", "clusters:delete"},
			expected:    false,
		},
		{
			name: "empty permissions list",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "admin",
					},
				},
			},
			permissions: []string{},
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.principal.HasAnyPermission(tt.permissions)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestPrincipal_HasAllPermissions tests AND logic for permissions
func TestPrincipal_HasAllPermissions(t *testing.T) {
	tests := []struct {
		name        string
		principal   *Principal
		permissions []string
		expected    bool
	}{
		{
			name: "has all required permissions",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:create",
					},
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:delete",
					},
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:read",
					},
				},
			},
			permissions: []string{"clusters:create", "clusters:delete"},
			expected:    true,
		},
		{
			name: "missing one required permission",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:create",
					},
				},
			},
			permissions: []string{"clusters:create", "clusters:delete"},
			expected:    false,
		},
		{
			name: "missing all required permissions",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "clusters:read",
					},
				},
			},
			permissions: []string{"clusters:create", "clusters:delete"},
			expected:    false,
		},
		{
			name: "empty required permissions list",
			principal: &Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Scope:      "gcp-engineering",
						Permission: "admin",
					},
				},
			},
			permissions: []string{},
			expected:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.principal.HasAllPermissions(tt.permissions)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestPrincipal_HasPermission_Wildcard tests that wildcard permission "*" grants access to any permission
func TestPrincipal_HasPermission_Wildcard(t *testing.T) {
	// Principal with wildcard permission
	principal := &Principal{
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{
				Scope:      "*",
				Permission: "*", // Wildcard permission
			},
		},
	}

	t.Run("wildcard grants any HasPermission check", func(t *testing.T) {
		// Wildcard should grant access to any permission
		assert.True(t, principal.HasPermission("clusters:create"))
		assert.True(t, principal.HasPermission("clusters:delete"))
		assert.True(t, principal.HasPermission("admin"))
		assert.True(t, principal.HasPermission("any:permission:name"))
		assert.True(t, principal.HasPermission("scim:manage-user"))
		assert.True(t, principal.HasPermission("tokens:mine:view"))
	})

	t.Run("wildcard grants any HasPermissionScoped check", func(t *testing.T) {
		// Wildcard should grant access to any scoped permission check
		assert.True(t, principal.HasPermissionScoped("clusters:create", "gcp-engineering"))
		assert.True(t, principal.HasPermissionScoped("clusters:delete", "aws-staging"))
		assert.True(t, principal.HasPermissionScoped("admin", "azure-prod"))
		assert.True(t, principal.HasPermissionScoped("any:permission", "any-scope"))
	})

	t.Run("wildcard grants HasAnyPermission check", func(t *testing.T) {
		// Wildcard should grant access when checking for any of multiple permissions
		assert.True(t, principal.HasAnyPermission([]string{"clusters:create", "clusters:delete", "admin"}))
		assert.True(t, principal.HasAnyPermission([]string{"never:seen:before"}))
	})

	t.Run("wildcard grants HasAllPermissions check", func(t *testing.T) {
		// Wildcard should satisfy all required permissions
		assert.True(t, principal.HasAllPermissions([]string{"clusters:create", "clusters:delete", "admin"}))
		assert.True(t, principal.HasAllPermissions([]string{"any:permission:1", "any:permission:2", "any:permission:3"}))
	})
}

// TestPermission_Interface tests that both UserPermission and ServiceAccountPermission implement Permission
func TestPermission_Interface(t *testing.T) {
	t.Run("UserPermission implements Permission", func(t *testing.T) {
		userPerm := &authmodels.UserPermission{
			ID:         uuid.MakeV4(),
			UserID:     uuid.MakeV4(),
			Scope:      "gcp-engineering",
			Permission: "clusters:create",
		}

		var perm authmodels.Permission = userPerm

		assert.Equal(t, "gcp-engineering", perm.GetScope())
		assert.Equal(t, "clusters:create", perm.GetPermission())
	})

	t.Run("ServiceAccountPermission implements Permission", func(t *testing.T) {
		saPerm := &authmodels.ServiceAccountPermission{
			ID:               uuid.MakeV4(),
			ServiceAccountID: uuid.MakeV4(),
			Scope:            "aws-staging",
			Permission:       "scim:manage-user",
		}

		var perm authmodels.Permission = saPerm

		assert.Equal(t, "aws-staging", perm.GetScope())
		assert.Equal(t, "scim:manage-user", perm.GetPermission())
	})
}
